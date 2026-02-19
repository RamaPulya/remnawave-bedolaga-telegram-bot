from __future__ import annotations

import html
import math
from datetime import UTC, datetime
from urllib.parse import urlparse

from aiogram import Dispatcher, F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.localization.loader import DEFAULT_LANGUAGE
from app.localization.texts import get_texts
from app.spiderman.menu_media import SLOT_ADMIN_MAIN, edit_or_answer_media
from app.spiderman.proxy_storage import (
    PROXY_EVENT_GET_BATCH_CLICK,
    PROXY_EVENT_LINK_CLICK,
    PROXY_EVENT_NOT_WORKING_OPEN,
    PROXY_EVENT_NOT_WORKING_SUBMIT,
    PROXY_EVENT_SCREEN_OPEN,
    create_proxy_link,
    delete_proxy_link,
    get_proxy_link,
    get_proxy_link_counts,
    get_proxy_stats,
    list_proxy_links,
    parse_proxy_admin_input,
    set_proxy_link_active,
)
from app.spiderman.states import SpidermanProxyAdminStates
from app.utils.decorators import admin_required, error_handler


PROXY_ADMIN_MENU_CALLBACK = 'admin_spiderman_proxy'

_PROXY_STATS_PREFIX = 'admin_spider_proxy_stats:'
_PROXY_LINKS_ROOT_CALLBACK = 'admin_spider_proxy_links'
_PROXY_PAGE_PREFIX = 'admin_spider_proxy_page:'
_PROXY_ADD_CALLBACK = 'admin_spider_proxy_add'
_PROXY_ADD_CANCEL_CALLBACK = 'admin_spider_proxy_add_cancel'
_PROXY_ITEM_PREFIX = 'admin_spider_proxy_item:'
_PROXY_TOGGLE_PREFIX = 'admin_spider_proxy_toggle:'
_PROXY_DELETE_ASK_PREFIX = 'admin_spider_proxy_del_ask:'
_PROXY_DELETE_PREFIX = 'admin_spider_proxy_del:'

_PERIOD_KEYS = ('today', '7d', '30d', 'all')
_PAGE_SIZE = 8


def _period_label(texts, period_key: str) -> str:
    mapping = {
        'today': texts.t('PROXY_PERIOD_TODAY', 'Сегодня'),
        '7d': texts.t('PROXY_PERIOD_7D', '7 дней'),
        '30d': texts.t('PROXY_PERIOD_30D', '30 дней'),
        'all': texts.t('PROXY_PERIOD_ALL', 'Все время'),
    }
    return mapping.get(period_key, mapping['today'])


def _to_safe_period(period_key: str) -> str:
    key = (period_key or '').strip().lower()
    return key if key in _PERIOD_KEYS else 'today'


def _build_proxy_menu_keyboard(texts, *, period_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=texts.t('PROXY_PERIOD_TODAY', 'Сегодня'), callback_data=f'{_PROXY_STATS_PREFIX}today'),
                InlineKeyboardButton(text=texts.t('PROXY_PERIOD_7D', '7 дней'), callback_data=f'{_PROXY_STATS_PREFIX}7d'),
            ],
            [
                InlineKeyboardButton(text=texts.t('PROXY_PERIOD_30D', '30 дней'), callback_data=f'{_PROXY_STATS_PREFIX}30d'),
                InlineKeyboardButton(text=texts.t('PROXY_PERIOD_ALL', 'Все время'), callback_data=f'{_PROXY_STATS_PREFIX}all'),
            ],
            [InlineKeyboardButton(text=texts.t('PROXY_ADD_LINK', '➕ Добавить ссылку'), callback_data=_PROXY_ADD_CALLBACK)],
            [InlineKeyboardButton(text=texts.t('PROXY_LINKS_LIST', '📋 Список ссылок'), callback_data=f'{_PROXY_PAGE_PREFIX}1')],
            [InlineKeyboardButton(text=texts.BACK, callback_data='admin_spiderman_menu')],
        ]
    )


def _status_badge(is_active: bool) -> str:
    return 'ON' if is_active else 'OFF'


def _shorten_url(url: str, max_length: int = 52) -> str:
    parsed = urlparse(url or '')
    compact = url
    if parsed.scheme and parsed.netloc:
        compact = f'{parsed.scheme}://{parsed.netloc}{parsed.path or ""}'
    if len(compact) <= max_length:
        return compact
    return f'{compact[: max_length - 1]}…'


def _build_links_list_keyboard(texts, *, items, page: int, total_pages: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for item in items:
        label_base = item.name.strip() if item.name else _shorten_url(item.url, max_length=34)
        label = f'[{_status_badge(item.is_active)}] {label_base}'
        rows.append([InlineKeyboardButton(text=label, callback_data=f'{_PROXY_ITEM_PREFIX}{item.id}')])

    nav_row: list[InlineKeyboardButton] = []
    if page > 1:
        nav_row.append(
            InlineKeyboardButton(
                text='◀️',
                callback_data=f'{_PROXY_PAGE_PREFIX}{page - 1}',
            )
        )
    if page < total_pages:
        nav_row.append(
            InlineKeyboardButton(
                text='▶️',
                callback_data=f'{_PROXY_PAGE_PREFIX}{page + 1}',
            )
        )
    if nav_row:
        rows.append(nav_row)

    rows.append([InlineKeyboardButton(text=texts.t('PROXY_ADD_LINK', '➕ Добавить ссылку'), callback_data=_PROXY_ADD_CALLBACK)])
    rows.append([InlineKeyboardButton(text=texts.BACK, callback_data=PROXY_ADMIN_MENU_CALLBACK)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_link_details_keyboard(texts, *, link_id: str, is_active: bool) -> InlineKeyboardMarkup:
    toggle_text = texts.t('PROXY_DISABLE_LINK', '⛔ Выключить') if is_active else texts.t('PROXY_ENABLE_LINK', '✅ Включить')
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=toggle_text, callback_data=f'{_PROXY_TOGGLE_PREFIX}{link_id}')],
            [InlineKeyboardButton(text=texts.t('PROXY_DELETE_LINK', '🗑 Удалить'), callback_data=f'{_PROXY_DELETE_ASK_PREFIX}{link_id}')],
            [InlineKeyboardButton(text=texts.t('PROXY_LINKS_LIST', '📋 Список ссылок'), callback_data=f'{_PROXY_PAGE_PREFIX}1')],
            [InlineKeyboardButton(text=texts.BACK, callback_data=PROXY_ADMIN_MENU_CALLBACK)],
        ]
    )


def _build_delete_confirm_keyboard(texts, *, link_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=texts.t('PROXY_CONFIRM_DELETE', 'Да, удалить'), callback_data=f'{_PROXY_DELETE_PREFIX}{link_id}')],
            [InlineKeyboardButton(text=texts.t('PROXY_CANCEL_DELETE', 'Отмена'), callback_data=f'{_PROXY_ITEM_PREFIX}{link_id}')],
        ]
    )


def _build_add_cancel_keyboard(texts) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=texts.CANCEL, callback_data=_PROXY_ADD_CANCEL_CALLBACK)],
            [InlineKeyboardButton(text=texts.BACK, callback_data=PROXY_ADMIN_MENU_CALLBACK)],
        ]
    )


def _extract_suffix(callback_data: str, prefix: str) -> str:
    if not callback_data.startswith(prefix):
        return ''
    return callback_data[len(prefix) :].strip()


def _format_created_at(value: datetime | None) -> str:
    if value is None:
        return '—'
    safe_dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return safe_dt.strftime('%Y-%m-%d %H:%M UTC')


def _build_stats_lines(texts, stats: dict) -> list[str]:
    labels = {
        PROXY_EVENT_SCREEN_OPEN: texts.t('PROXY_STAT_SCREEN_OPEN', 'Открыли экран'),
        PROXY_EVENT_GET_BATCH_CLICK: texts.t('PROXY_STAT_GET_BATCH', 'Нажали "Получить 3"'),
        PROXY_EVENT_LINK_CLICK: texts.t('PROXY_STAT_LINK_CLICK', 'Нажали "Прокси 1/2/3"'),
        PROXY_EVENT_NOT_WORKING_OPEN: texts.t('PROXY_STAT_NOT_WORKING_OPEN', 'Открыли "Не работает"'),
        PROXY_EVENT_NOT_WORKING_SUBMIT: texts.t('PROXY_STAT_NOT_WORKING_SUBMIT', 'Подтвердили "Не работает"'),
    }
    lines: list[str] = []
    for event_id in (
        PROXY_EVENT_SCREEN_OPEN,
        PROXY_EVENT_GET_BATCH_CLICK,
        PROXY_EVENT_LINK_CLICK,
        PROXY_EVENT_NOT_WORKING_OPEN,
        PROXY_EVENT_NOT_WORKING_SUBMIT,
    ):
        metric = stats.get(event_id)
        total = int(metric.total if metric else 0)
        unique_users = int(metric.unique_users if metric else 0)
        lines.append(f'• {labels[event_id]}: {total} (уникальных: {unique_users})')
    return lines


async def _render_proxy_admin_menu(
    callback: types.CallbackQuery,
    *,
    db,
    language: str | None,
    period_key: str,
) -> None:
    texts = get_texts(language or DEFAULT_LANGUAGE)
    safe_period = _to_safe_period(period_key)
    total_links, active_links = await get_proxy_link_counts(db)
    stats = await get_proxy_stats(db, period_key=safe_period)
    lines = _build_stats_lines(texts, stats)
    stats_block = '\n'.join(lines)
    text = (
        '🕷️ <b>Spiderman menu</b>\n\n'
        '<b>Proxy</b>\n'
        f'Период: <b>{html.escape(_period_label(texts, safe_period))}</b>\n'
        f'Ссылок: <b>{active_links}/{total_links}</b> (активных/всего)\n\n'
        f'{stats_block}'
    )
    await edit_or_answer_media(
        callback,
        slot=SLOT_ADMIN_MAIN,
        caption=text,
        keyboard=_build_proxy_menu_keyboard(texts, period_key=safe_period),
        parse_mode='HTML',
    )


@admin_required
@error_handler
async def show_proxy_admin_menu(callback: types.CallbackQuery, state: FSMContext, db_user, db):
    await state.clear()
    await _render_proxy_admin_menu(
        callback,
        db=db,
        language=getattr(db_user, 'language', None),
        period_key='today',
    )
    await callback.answer()


@admin_required
@error_handler
async def switch_proxy_admin_period(callback: types.CallbackQuery, state: FSMContext, db_user, db):
    await state.clear()
    raw_period = _extract_suffix(callback.data or '', _PROXY_STATS_PREFIX)
    await _render_proxy_admin_menu(
        callback,
        db=db,
        language=getattr(db_user, 'language', None),
        period_key=_to_safe_period(raw_period),
    )
    await callback.answer()


@admin_required
@error_handler
async def open_proxy_links_page(callback: types.CallbackQuery, state: FSMContext, db_user, db):
    await state.clear()
    texts = get_texts(getattr(db_user, 'language', None) or DEFAULT_LANGUAGE)

    callback_data = callback.data or ''
    if callback_data == _PROXY_LINKS_ROOT_CALLBACK:
        page = 1
    else:
        page_text = _extract_suffix(callback_data, _PROXY_PAGE_PREFIX)
        try:
            page = int(page_text)
        except ValueError:
            page = 1
    page = max(1, page)

    total, items = await list_proxy_links(db, page=page, page_size=_PAGE_SIZE)
    total_pages = max(1, math.ceil(total / _PAGE_SIZE))
    if page > total_pages:
        page = total_pages
        total, items = await list_proxy_links(db, page=page, page_size=_PAGE_SIZE)

    if total == 0:
        caption = (
            '🕷️ <b>Spiderman menu</b>\n\n'
            '<b>Proxy</b>\n'
            'Список ссылок пуст.'
        )
    else:
        lines = [
            f'{idx}. [{_status_badge(item.is_active)}] {html.escape(item.name or _shorten_url(item.url, 42))}'
            for idx, item in enumerate(items, start=1 + (page - 1) * _PAGE_SIZE)
        ]
        list_block = '\n'.join(lines)
        caption = (
            '🕷️ <b>Spiderman menu</b>\n\n'
            '<b>Proxy: список ссылок</b>\n'
            f'Страница <b>{page}/{total_pages}</b>\n\n'
            f'{list_block}'
        )

    await edit_or_answer_media(
        callback,
        slot=SLOT_ADMIN_MAIN,
        caption=caption,
        keyboard=_build_links_list_keyboard(texts, items=items, page=page, total_pages=total_pages),
        parse_mode='HTML',
    )
    await callback.answer()


@admin_required
@error_handler
async def open_proxy_link_item(callback: types.CallbackQuery, state: FSMContext, db_user, db):
    await state.clear()
    texts = get_texts(getattr(db_user, 'language', None) or DEFAULT_LANGUAGE)
    link_id = _extract_suffix(callback.data or '', _PROXY_ITEM_PREFIX)
    if not link_id:
        await callback.answer()
        return

    link = await get_proxy_link(db, link_id)
    if link is None:
        await callback.answer('Ссылка не найдена.', show_alert=True)
        return

    caption = (
        '🕷️ <b>Spiderman menu</b>\n\n'
        '<b>Proxy: ссылка</b>\n'
        f'ID: <code>{html.escape(link.id)}</code>\n'
        f'Статус: <b>{_status_badge(link.is_active)}</b>\n'
        f'Название: <b>{html.escape(link.name or "—")}</b>\n'
        f'Создано: <b>{html.escape(_format_created_at(link.created_at))}</b>\n\n'
        f'<code>{html.escape(link.url)}</code>'
    )
    await edit_or_answer_media(
        callback,
        slot=SLOT_ADMIN_MAIN,
        caption=caption,
        keyboard=_build_link_details_keyboard(texts, link_id=link.id, is_active=link.is_active),
        parse_mode='HTML',
    )
    await callback.answer()


@admin_required
@error_handler
async def toggle_proxy_link_item(callback: types.CallbackQuery, state: FSMContext, db_user, db):
    await state.clear()
    link_id = _extract_suffix(callback.data or '', _PROXY_TOGGLE_PREFIX)
    if not link_id:
        await callback.answer()
        return

    link = await get_proxy_link(db, link_id)
    if link is None:
        await callback.answer('Ссылка не найдена.', show_alert=True)
        return

    updated = await set_proxy_link_active(db, link_id, is_active=not link.is_active)
    if not updated:
        await callback.answer('Не удалось обновить статус.', show_alert=True)
        return

    await open_proxy_link_item(
        callback=callback,
        state=state,
        db_user=db_user,
        db=db,
    )


@admin_required
@error_handler
async def ask_delete_proxy_link_item(callback: types.CallbackQuery, state: FSMContext, db_user, db):
    await state.clear()
    texts = get_texts(getattr(db_user, 'language', None) or DEFAULT_LANGUAGE)
    link_id = _extract_suffix(callback.data or '', _PROXY_DELETE_ASK_PREFIX)
    if not link_id:
        await callback.answer()
        return

    link = await get_proxy_link(db, link_id)
    if link is None:
        await callback.answer('Ссылка не найдена.', show_alert=True)
        return

    caption = (
        'Подтвердите удаление ссылки:\n\n'
        f'<code>{html.escape(link.url)}</code>'
    )
    await edit_or_answer_media(
        callback,
        slot=SLOT_ADMIN_MAIN,
        caption=caption,
        keyboard=_build_delete_confirm_keyboard(texts, link_id=link.id),
        parse_mode='HTML',
    )
    await callback.answer()


@admin_required
@error_handler
async def confirm_delete_proxy_link_item(callback: types.CallbackQuery, state: FSMContext, db_user, db):
    await state.clear()
    link_id = _extract_suffix(callback.data or '', _PROXY_DELETE_PREFIX)
    if not link_id:
        await callback.answer()
        return

    deleted = await delete_proxy_link(db, link_id)
    if not deleted:
        await callback.answer('Ссылка не найдена или уже удалена.', show_alert=True)
        return

    await open_proxy_links_page(
        callback=callback,
        state=state,
        db_user=db_user,
        db=db,
    )


@admin_required
@error_handler
async def start_add_proxy_link(callback: types.CallbackQuery, state: FSMContext, db_user, db):
    texts = get_texts(getattr(db_user, 'language', None) or DEFAULT_LANGUAGE)
    await state.set_state(SpidermanProxyAdminStates.waiting_for_proxy_link)
    caption = (
        'Отправьте ссылку прокси.\n\n'
        'Формат 1: <code>https://t.me/proxy?server=...&port=...&secret=...</code>\n'
        'Формат 2: <code>Название | tg://proxy?server=...&port=...&secret=...</code>'
    )
    await edit_or_answer_media(
        callback,
        slot=SLOT_ADMIN_MAIN,
        caption=caption,
        keyboard=_build_add_cancel_keyboard(texts),
        parse_mode='HTML',
    )
    await callback.answer()


@admin_required
@error_handler
async def cancel_add_proxy_link(callback: types.CallbackQuery, state: FSMContext, db_user, db):
    await state.clear()
    await show_proxy_admin_menu(callback, state, db_user, db)


@admin_required
@error_handler
async def handle_add_proxy_link_message(message: types.Message, state: FSMContext, db_user, db):
    texts = get_texts(getattr(db_user, 'language', None) or DEFAULT_LANGUAGE)
    name, normalized_url, parse_error = parse_proxy_admin_input(message.text or '')
    if parse_error:
        await message.answer(
            f'❌ {parse_error}',
            reply_markup=_build_add_cancel_keyboard(texts),
            parse_mode='HTML',
        )
        return

    created, code = await create_proxy_link(
        db,
        url=normalized_url,
        name=name,
        created_by=getattr(message.from_user, 'id', None),
    )
    if not created:
        if code == 'duplicate_url':
            await message.answer(
                '❌ Такая ссылка уже есть в базе.',
                reply_markup=_build_add_cancel_keyboard(texts),
            )
            return
        await message.answer(
            '❌ Не удалось сохранить ссылку. Повторите позже.',
            reply_markup=_build_add_cancel_keyboard(texts),
        )
        return

    await state.clear()
    await message.answer(
        (
            '✅ Ссылка добавлена.\n'
            f'ID: <code>{html.escape(code)}</code>\n'
            f'URL: <code>{html.escape(normalized_url)}</code>'
        ),
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text='↩️ Proxy', callback_data=PROXY_ADMIN_MENU_CALLBACK)],
                [InlineKeyboardButton(text='📋 Список ссылок', callback_data=f'{_PROXY_PAGE_PREFIX}1')],
            ]
        ),
    )


def register_proxy_admin_handlers(dp: Dispatcher) -> None:
    dp.callback_query.register(show_proxy_admin_menu, F.data == PROXY_ADMIN_MENU_CALLBACK)
    dp.callback_query.register(switch_proxy_admin_period, F.data.startswith(_PROXY_STATS_PREFIX))
    dp.callback_query.register(open_proxy_links_page, F.data == _PROXY_LINKS_ROOT_CALLBACK)
    dp.callback_query.register(open_proxy_links_page, F.data.startswith(_PROXY_PAGE_PREFIX))
    dp.callback_query.register(open_proxy_link_item, F.data.startswith(_PROXY_ITEM_PREFIX))
    dp.callback_query.register(toggle_proxy_link_item, F.data.startswith(_PROXY_TOGGLE_PREFIX))
    dp.callback_query.register(ask_delete_proxy_link_item, F.data.startswith(_PROXY_DELETE_ASK_PREFIX))
    dp.callback_query.register(confirm_delete_proxy_link_item, F.data.startswith(_PROXY_DELETE_PREFIX))
    dp.callback_query.register(start_add_proxy_link, F.data == _PROXY_ADD_CALLBACK)
    dp.callback_query.register(cancel_add_proxy_link, F.data == _PROXY_ADD_CANCEL_CALLBACK)
    dp.message.register(handle_add_proxy_link_message, SpidermanProxyAdminStates.waiting_for_proxy_link)
