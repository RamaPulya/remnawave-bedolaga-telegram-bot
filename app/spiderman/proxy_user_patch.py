from __future__ import annotations

import html
import importlib
from urllib.parse import quote_plus

from aiogram import Dispatcher, F, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.config import settings
from app.localization.loader import DEFAULT_LANGUAGE
from app.localization.texts import get_texts
from app.spiderman.proxy_storage import (
    PROXY_EVENT_GET_BATCH_CLICK,
    PROXY_EVENT_LINK_CLICK,
    PROXY_EVENT_NOT_WORKING_OPEN,
    PROXY_EVENT_NOT_WORKING_SUBMIT,
    PROXY_EVENT_SCREEN_OPEN,
    PROXY_BATCH_SIZE,
    check_proxy_batch_limits,
    get_batch_link,
    get_batch_links,
    issue_proxy_batch,
    log_proxy_event,
)


_PROXY_MENU_CALLBACK = 'menu_free_proxy'
_PROXY_GET_BATCH_CALLBACK = 'proxy_get_batch'
_PROXY_CLICK_PREFIX = 'proxy_click:'
_PROXY_NOT_WORKING_PREFIX = 'proxy_not_working:'
_PROXY_NOT_WORKING_SELECT_PREFIX = 'proxy_not_working_select:'
_PROXY_BACK_TO_BATCH_PREFIX = 'proxy_back_batch:'


def _loc(texts, key: str, default: str) -> str:
    values = getattr(texts, '_values', None)
    if isinstance(values, dict) and key in values:
        value = values.get(key)
        if isinstance(value, str) and value:
            return value

    fallback_values = getattr(texts, '_fallback_values', None)
    if isinstance(fallback_values, dict) and key in fallback_values:
        value = fallback_values.get(key)
        if isinstance(value, str) and value:
            return value

    return default


def _proxy_button_label(language: str | None) -> str:
    texts = get_texts(language or DEFAULT_LANGUAGE)
    return _loc(texts, 'MENU_FREE_PROXY', '⚡ Бесплатный Telegram Proxy')


def _is_proxy_menu_enabled() -> bool:
    return bool(getattr(settings, 'SPIDERMAN_FREE_PROXY_MENU_ENABLED', True))


def _build_share_url(texts) -> str | None:
    username = settings.get_bot_username()
    if not username:
        return None
    username = username.strip().lstrip('@')
    if not username:
        return None

    bot_link = f'https://t.me/{username}'
    share_template = _loc(
        texts,
        'PROXY_SHARE_TEXT',
        'Получи бесплатные прокси для Telegram и верни себе рабочий мессенджер.\nБот: {bot_username}',
    )
    try:
        share_text = share_template.format(bot_username=f'@{username}')
    except Exception:
        share_text = share_template
    return (
        'https://t.me/share/url'
        f'?url={quote_plus(bot_link)}'
        f'&text={quote_plus(share_text)}'
    )


def _build_share_row(texts) -> list[InlineKeyboardButton] | None:
    share_url = _build_share_url(texts)
    if not share_url:
        return None
    return [
        InlineKeyboardButton(
            text=_loc(texts, 'PROXY_SHARE_BUTTON', '📣 Рекомендовать друзьям'),
            url=share_url,
        )
    ]


def _ensure_proxy_row(
    keyboard: InlineKeyboardMarkup,
    *,
    language: str | None,
) -> InlineKeyboardMarkup:
    if not _is_proxy_menu_enabled():
        if keyboard is None:
            return InlineKeyboardMarkup(inline_keyboard=[])

        rows = list(getattr(keyboard, 'inline_keyboard', []) or [])
        filtered_rows: list[list[InlineKeyboardButton]] = []
        for row in rows:
            filtered_row = [button for button in row if getattr(button, 'callback_data', None) != _PROXY_MENU_CALLBACK]
            if filtered_row:
                filtered_rows.append(filtered_row)
        return InlineKeyboardMarkup(inline_keyboard=filtered_rows)

    if keyboard is None:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=_proxy_button_label(language),
                        callback_data=_PROXY_MENU_CALLBACK,
                    )
                ]
            ]
        )
    rows = list(getattr(keyboard, 'inline_keyboard', []) or [])
    for row in rows:
        for button in row:
            if getattr(button, 'callback_data', None) == _PROXY_MENU_CALLBACK:
                return keyboard

    proxy_row = [
        InlineKeyboardButton(
            text=_proxy_button_label(language),
            callback_data=_PROXY_MENU_CALLBACK,
        )
    ]
    rows.insert(0, proxy_row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _intro_text(texts) -> str:
    return _loc(
        texts,
        'PROXY_FREE_INTRO',
        (
            '⚡ <b>Бесплатные Telegram Proxy</b>\n\n'
            'Прокси общедоступные, стабильность не гарантируем.\n'
            'Мы поддерживаем их за свой счет.\n\n'
            'Нужен стабильный интернет для всех приложений и устройств — используйте наш VPN.'
        ),
    )


def _batch_text(texts) -> str:
    return _loc(
        texts,
        'PROXY_BATCH_TEXT',
        (
            'Выберите прокси ниже.\n'
            'Если не работает, нажмите «Не работает», и бот выдаст новую подборку.'
        ),
    )


def _build_home_keyboard(texts) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text=_loc(texts, 'PROXY_GET_BATCH_BUTTON', '📦 Получить 3 прокси'), callback_data=_PROXY_GET_BATCH_CALLBACK)],
    ]
    share_row = _build_share_row(texts)
    if share_row:
        rows.append(share_row)
    rows.append([InlineKeyboardButton(text=texts.BACK, callback_data='back_to_menu')])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_batch_keyboard(texts, *, batch_id: str, links) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for index, link in enumerate(links, start=1):
        rows.append(
            [
                InlineKeyboardButton(
                    text=_loc(texts, 'PROXY_ITEM_BUTTON', '🧩 Прокси {index}').format(index=index),
                    callback_data=f'{_PROXY_CLICK_PREFIX}{batch_id}:{link.id}',
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text=_loc(texts, 'PROXY_NOT_WORKING_BUTTON', '❌ Не работает'),
                callback_data=f'{_PROXY_NOT_WORKING_PREFIX}{batch_id}',
            )
        ]
    )
    share_row = _build_share_row(texts)
    if share_row:
        rows.append(share_row)
    rows.append([InlineKeyboardButton(text=texts.BACK, callback_data=_PROXY_MENU_CALLBACK)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_not_working_keyboard(texts, *, batch_id: str, links) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for index, link in enumerate(links, start=1):
        rows.append(
            [
                InlineKeyboardButton(
                    text=_loc(texts, 'PROXY_BAD_SELECT_ITEM', '❌ Прокси {index} не работает').format(index=index),
                    callback_data=f'{_PROXY_NOT_WORKING_SELECT_PREFIX}{batch_id}:{link.id}',
                )
            ]
        )
    share_row = _build_share_row(texts)
    if share_row:
        rows.append(share_row)
    rows.append([InlineKeyboardButton(text=texts.BACK, callback_data=f'{_PROXY_BACK_TO_BATCH_PREFIX}{batch_id}')])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _parse_batch_and_link(callback_data: str, prefix: str) -> tuple[str, str] | None:
    payload = (callback_data or '')[len(prefix) :]
    if ':' not in payload:
        return None
    batch_id, link_id = payload.split(':', 1)
    if not batch_id or not link_id:
        return None
    return batch_id, link_id


async def _render_proxy_home(callback: types.CallbackQuery, *, language: str | None) -> None:
    texts = get_texts(language or DEFAULT_LANGUAGE)
    menu_handlers = importlib.import_module('app.handlers.menu')
    await menu_handlers.edit_or_answer_photo(
        callback=callback,
        caption=_intro_text(texts),
        keyboard=_build_home_keyboard(texts),
        parse_mode='HTML',
    )


async def _render_proxy_batch(
    callback: types.CallbackQuery,
    *,
    language: str | None,
    batch_id: str,
    links,
) -> None:
    texts = get_texts(language or DEFAULT_LANGUAGE)
    menu_handlers = importlib.import_module('app.handlers.menu')
    await menu_handlers.edit_or_answer_photo(
        callback=callback,
        caption=_batch_text(texts),
        keyboard=_build_batch_keyboard(texts, batch_id=batch_id, links=links),
        parse_mode='HTML',
    )


async def _render_proxy_batch_by_id(
    callback: types.CallbackQuery,
    *,
    language: str | None,
    db,
    user_telegram_id: int,
    batch_id: str,
) -> None:
    links = await get_batch_links(
        db,
        user_telegram_id=user_telegram_id,
        batch_id=batch_id,
    )
    if not links:
        await _render_proxy_home(callback, language=language)
        return

    await _render_proxy_batch(
        callback,
        language=language,
        batch_id=batch_id,
        links=links,
    )


async def _render_selected_link(
    callback: types.CallbackQuery,
    *,
    language: str | None,
    link,
    batch_id: str,
    link_index: int | None,
) -> None:
    texts = get_texts(language or DEFAULT_LANGUAGE)
    menu_handlers = importlib.import_module('app.handlers.menu')
    safe_url = html.escape(link.url)
    title = _loc(texts, 'PROXY_LINK_MESSAGE_TITLE', '🧩 Выбран прокси')
    index_part = f' #{link_index}' if link_index else ''
    caption = (
        f'{title}{index_part}\n\n'
        f'<code>{safe_url}</code>\n\n'
        f'{_loc(texts, "PROXY_LINK_TIP", "Нажмите кнопку ниже для подключения.")}'
    )
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text=_loc(texts, 'PROXY_OPEN_URL_BUTTON', '🔌 Подключить прокси'), url=link.url)],
        [InlineKeyboardButton(text=_loc(texts, 'PROXY_GET_MORE', '📦 Получить еще 3 прокси'), callback_data=_PROXY_GET_BATCH_CALLBACK)],
    ]
    share_row = _build_share_row(texts)
    if share_row:
        rows.append(share_row)
    rows.append([InlineKeyboardButton(text=_loc(texts, 'PROXY_BACK_TO_LIST_BUTTON', '◀️ Назад к списку'), callback_data=f'{_PROXY_BACK_TO_BATCH_PREFIX}{batch_id}')])
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=rows
    )
    await menu_handlers.edit_or_answer_photo(
        callback=callback,
        caption=caption,
        keyboard=keyboard,
        parse_mode='HTML',
    )


async def show_free_proxy_menu(callback: types.CallbackQuery, db_user, db, state) -> None:
    if state is not None:
        await state.clear()

    language = getattr(db_user, 'language', None)
    await log_proxy_event(
        callback.from_user.id,
        PROXY_EVENT_SCREEN_OPEN,
        callback_data=_PROXY_MENU_CALLBACK,
        button_text='proxy_screen_open',
    )
    await _render_proxy_home(callback, language=language)
    await callback.answer()


async def send_proxy_batch(callback: types.CallbackQuery, db_user, db, state) -> None:
    if state is not None:
        await state.clear()

    language = getattr(db_user, 'language', None)
    texts = get_texts(language or DEFAULT_LANGUAGE)
    user_telegram_id = callback.from_user.id

    await log_proxy_event(
        user_telegram_id,
        PROXY_EVENT_GET_BATCH_CLICK,
        callback_data=_PROXY_GET_BATCH_CALLBACK,
        button_text='proxy_get_batch',
    )

    allowed, wait_seconds, daily_remaining = await check_proxy_batch_limits(db, user_telegram_id)
    if not allowed:
        if daily_remaining <= 0:
            await callback.answer(
                _loc(texts, 'PROXY_DAILY_LIMIT_REACHED', 'Достигнут дневной лимит: 10 выдач в сутки.'),
                show_alert=True,
            )
        else:
            await callback.answer(
                _loc(texts, 'PROXY_COOLDOWN', 'Слишком часто. Попробуйте через {seconds} сек.').format(seconds=wait_seconds),
                show_alert=True,
            )
        return

    batch_id, links = await issue_proxy_batch(
        db,
        user_telegram_id=user_telegram_id,
        batch_size=PROXY_BATCH_SIZE,
    )
    if not links:
        await callback.answer(
            _loc(texts, 'PROXY_LIST_EMPTY', 'Список прокси пока пуст. Попробуйте позже.'),
            show_alert=True,
        )
        await _render_proxy_home(callback, language=language)
        return

    await _render_proxy_batch(
        callback,
        language=language,
        batch_id=batch_id,
        links=links,
    )
    await callback.answer(
        _loc(texts, 'PROXY_BATCH_SENT', 'Готово. Вот 3 прокси.'),
    )


async def send_single_proxy_link(callback: types.CallbackQuery, db_user, db, state) -> None:
    if state is not None:
        await state.clear()

    data = callback.data or ''
    parsed = _parse_batch_and_link(data, _PROXY_CLICK_PREFIX)
    if parsed is None:
        await callback.answer()
        return
    batch_id, link_id = parsed

    link = await get_batch_link(
        db,
        user_telegram_id=callback.from_user.id,
        batch_id=batch_id,
        link_id=link_id,
    )
    if link is None:
        await callback.answer(
            'Эта ссылка уже недоступна. Запросите новый набор.',
            show_alert=True,
        )
        return

    language = getattr(db_user, 'language', None)
    texts = get_texts(language or DEFAULT_LANGUAGE)

    await log_proxy_event(
        callback.from_user.id,
        PROXY_EVENT_LINK_CLICK,
        callback_data=data,
        button_text='proxy_link_click',
    )

    batch_links = await get_batch_links(
        db,
        user_telegram_id=callback.from_user.id,
        batch_id=batch_id,
    )
    link_index = None
    for index, item in enumerate(batch_links, start=1):
        if item.id == link.id:
            link_index = index
            break

    await _render_selected_link(
        callback,
        language=language,
        link=link,
        batch_id=batch_id,
        link_index=link_index,
    )
    await callback.answer(_loc(texts, 'PROXY_LINK_SENT_ALERT', 'Ссылка готова.'))


async def open_not_working_selector(callback: types.CallbackQuery, db_user, db, state) -> None:
    if state is not None:
        await state.clear()

    data = callback.data or ''
    batch_id = data[len(_PROXY_NOT_WORKING_PREFIX) :]
    if not batch_id:
        await callback.answer()
        return

    links = await get_batch_links(
        db,
        user_telegram_id=callback.from_user.id,
        batch_id=batch_id,
    )
    if not links:
        await callback.answer('Набор устарел. Запросите новые прокси.', show_alert=True)
        return

    language = getattr(db_user, 'language', None)
    texts = get_texts(language or DEFAULT_LANGUAGE)

    await log_proxy_event(
        callback.from_user.id,
        PROXY_EVENT_NOT_WORKING_OPEN,
        callback_data=data,
        button_text='proxy_not_working_open',
    )

    menu_handlers = importlib.import_module('app.handlers.menu')
    await menu_handlers.edit_or_answer_photo(
        callback=callback,
        caption=_loc(texts, 'PROXY_NOT_WORKING_CHOOSE', 'Выберите, какой именно прокси не работает:'),
        keyboard=_build_not_working_keyboard(texts, batch_id=batch_id, links=links),
        parse_mode='HTML',
    )
    await callback.answer()


async def process_not_working_selection(callback: types.CallbackQuery, db_user, db, state) -> None:
    if state is not None:
        await state.clear()

    data = callback.data or ''
    parsed = _parse_batch_and_link(data, _PROXY_NOT_WORKING_SELECT_PREFIX)
    if parsed is None:
        await callback.answer()
        return
    batch_id, link_id = parsed

    language = getattr(db_user, 'language', None)
    texts = get_texts(language or DEFAULT_LANGUAGE)

    link = await get_batch_link(
        db,
        user_telegram_id=callback.from_user.id,
        batch_id=batch_id,
        link_id=link_id,
    )
    if link is None:
        await callback.answer('Набор устарел. Запросите новые прокси.', show_alert=True)
        return

    await log_proxy_event(
        callback.from_user.id,
        PROXY_EVENT_NOT_WORKING_SUBMIT,
        callback_data=data,
        button_text='proxy_not_working_submit',
    )

    allowed, wait_seconds, daily_remaining = await check_proxy_batch_limits(
        db,
        callback.from_user.id,
        apply_cooldown=False,
    )
    if not allowed:
        if daily_remaining <= 0:
            await callback.answer(
                _loc(texts, 'PROXY_DAILY_LIMIT_REACHED', 'Достигнут дневной лимит: 10 выдач в сутки.'),
                show_alert=True,
            )
        else:
            await callback.answer(
                _loc(texts, 'PROXY_COOLDOWN', 'Слишком часто. Попробуйте через {seconds} сек.').format(seconds=wait_seconds),
                show_alert=True,
            )
        return

    new_batch_id, links = await issue_proxy_batch(
        db,
        user_telegram_id=callback.from_user.id,
        batch_size=PROXY_BATCH_SIZE,
    )
    if not links:
        await callback.answer(
            _loc(texts, 'PROXY_LIST_EMPTY', 'Список прокси пока пуст. Попробуйте позже.'),
            show_alert=True,
        )
        await _render_proxy_home(callback, language=language)
        return

    await _render_proxy_batch(
        callback,
        language=language,
        batch_id=new_batch_id,
        links=links,
    )
    await callback.answer(
        _loc(texts, 'PROXY_REPLACED', 'Понял. Выдал новую подборку.'),
    )


async def back_to_proxy_batch(callback: types.CallbackQuery, db_user, db, state) -> None:
    if state is not None:
        await state.clear()

    data = callback.data or ''
    batch_id = data[len(_PROXY_BACK_TO_BATCH_PREFIX) :].strip()
    language = getattr(db_user, 'language', None)
    if not batch_id:
        await _render_proxy_home(callback, language=language)
        await callback.answer()
        return

    await _render_proxy_batch_by_id(
        callback,
        language=language,
        db=db,
        user_telegram_id=callback.from_user.id,
        batch_id=batch_id,
    )
    await callback.answer()


def register_proxy_user_handlers(dp: Dispatcher) -> None:
    dp.callback_query.register(show_free_proxy_menu, F.data == _PROXY_MENU_CALLBACK)
    dp.callback_query.register(send_proxy_batch, F.data == _PROXY_GET_BATCH_CALLBACK)
    dp.callback_query.register(send_single_proxy_link, F.data.startswith(_PROXY_CLICK_PREFIX))
    dp.callback_query.register(open_not_working_selector, F.data.startswith(_PROXY_NOT_WORKING_PREFIX))
    dp.callback_query.register(process_not_working_selection, F.data.startswith(_PROXY_NOT_WORKING_SELECT_PREFIX))
    dp.callback_query.register(back_to_proxy_batch, F.data.startswith(_PROXY_BACK_TO_BATCH_PREFIX))


def apply_proxy_feature_patches() -> None:
    inline_keyboards = importlib.import_module('app.keyboards.inline')
    menu_handlers = importlib.import_module('app.handlers.menu')
    start_handlers = importlib.import_module('app.handlers.start')

    if not getattr(inline_keyboards, '_spiderman_proxy_menu_keyboard_patched', False):
        original_sync = inline_keyboards.get_main_menu_keyboard
        original_async = inline_keyboards.get_main_menu_keyboard_async

        def patched_get_main_menu_keyboard(*args, **kwargs):
            keyboard = original_sync(*args, **kwargs)
            language = kwargs.get('language') or DEFAULT_LANGUAGE
            return _ensure_proxy_row(keyboard, language=language)

        async def patched_get_main_menu_keyboard_async(*args, **kwargs):
            keyboard = await original_async(*args, **kwargs)
            language = kwargs.get('language') or DEFAULT_LANGUAGE
            return _ensure_proxy_row(keyboard, language=language)

        inline_keyboards.get_main_menu_keyboard = patched_get_main_menu_keyboard
        inline_keyboards.get_main_menu_keyboard_async = patched_get_main_menu_keyboard_async
        inline_keyboards._spiderman_proxy_menu_keyboard_patched = True

        if hasattr(menu_handlers, 'get_main_menu_keyboard'):
            menu_handlers.get_main_menu_keyboard = patched_get_main_menu_keyboard
        menu_handlers.get_main_menu_keyboard_async = patched_get_main_menu_keyboard_async
        if hasattr(start_handlers, 'get_main_menu_keyboard'):
            start_handlers.get_main_menu_keyboard = patched_get_main_menu_keyboard
        start_handlers.get_main_menu_keyboard_async = patched_get_main_menu_keyboard_async

    if getattr(menu_handlers, '_spiderman_proxy_menu_handlers_patched', False):
        return

    original_register_handlers = menu_handlers.register_handlers

    def register_handlers_patched(dp: Dispatcher):
        original_register_handlers(dp)
        register_proxy_user_handlers(dp)

    menu_handlers.register_handlers = register_handlers_patched
    menu_handlers._spiderman_proxy_menu_handlers_patched = True
