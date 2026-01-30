from typing import Optional

from aiogram import Dispatcher, types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.localization.texts import get_texts
from app.utils.decorators import admin_required, error_handler
from app.spiderman.menu_media import (
    SLOT_ADMIN_MAIN,
    SLOT_EXTEND_DAYS,
    SLOT_EXTEND_TRAFFIC,
    SLOT_MAIN_MENU,
    SLOT_PURCHASE_SUCCESS,
    SLOT_REFERRAL,
    SLOT_SUBSCRIPTION,
    SLOT_SUPPORT,
    edit_or_answer_media,
    get_env_key_for_slot,
    normalize_slot,
)
from app.spiderman.states import SpidermanMediaStates

_MAIN_MENU_CALLBACK = "admin_spiderman_menu"
_CHANNEL_POST_CALLBACK = "admin_spiderman_channel_post"
_FILE_ID_MENU_CALLBACK = "admin_spiderman_menu_file_id"
_CLOSE_MESSAGE_CALLBACK = "admin_spiderman_close_message"

_SLOT_LABELS = {
    SLOT_MAIN_MENU: "🏠 Главное меню (fallback)",
    SLOT_SUBSCRIPTION: "📱 Моя подписка",
    SLOT_EXTEND_DAYS: "⏰ Продление подписки",
    SLOT_EXTEND_TRAFFIC: "📈 Докупить трафик",
    SLOT_SUPPORT: "🛟 Техподдержка",
    SLOT_REFERRAL: "👥 Партнёрка",
    SLOT_PURCHASE_SUCCESS: "✅ Успешная оплата",
    SLOT_ADMIN_MAIN: "🕷️ Админка (Spiderman menu)",
}


def _build_main_menu_keyboard(texts) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=texts.t(
                        "ADMIN_SPIDERMAN_MEDIA_FILE_ID_SECTION",
                        "📁 Настройка File_ID",
                    ),
                    callback_data=_FILE_ID_MENU_CALLBACK,
                )
            ],
            [
                InlineKeyboardButton(
                    text=texts.t(
                        "ADMIN_SPIDERMAN_CHANNEL_POST_BUTTON",
                        "📢 Пост в канал",
                    ),
                    callback_data=_CHANNEL_POST_CALLBACK,
                )
            ],
            [InlineKeyboardButton(text=texts.BACK, callback_data="admin_panel")],
        ]
    )


def _build_slots_keyboard(texts) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for slot_key, label in _SLOT_LABELS.items():
        rows.append(
            [
                InlineKeyboardButton(
                    text=label,
                    callback_data=f"spider_media_slot:{slot_key}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text=texts.BACK, callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_navigation_markup(callback_data: str, texts) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=texts.BACK, callback_data=callback_data)]]
    )


def _build_back_cancel_markup(back_callback_data: str, texts) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=texts.BACK, callback_data=back_callback_data),
                InlineKeyboardButton(text=texts.CANCEL, callback_data=_CLOSE_MESSAGE_CALLBACK),
            ]
        ]
    )


@admin_required
@error_handler
async def close_spiderman_message(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
    db,
):
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.answer()


@admin_required
@error_handler
async def show_spiderman_menu_root(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
    db,
):
    texts = get_texts(db_user.language)
    if state is not None:
        await state.clear()
    text = texts.t(
        "ADMIN_SPIDERMAN_MEDIA_TITLE",
        "🕷️ Spiderman menu\n\nВыберите раздел:",
    )
    await edit_or_answer_media(
        callback,
        slot=SLOT_ADMIN_MAIN,
        caption=text,
        keyboard=_build_main_menu_keyboard(texts),
        parse_mode="HTML",
    )
    await callback.answer()


@admin_required
@error_handler
async def show_spiderman_file_id_menu(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
    db,
):
    texts = get_texts(db_user.language)
    if state is not None:
        await state.clear()
    text = texts.t(
        "ADMIN_SPIDERMAN_MEDIA_FILE_ID_TITLE",
        "🕷️ Spiderman menu\n\nВыберите экран для медиа:",
    )
    await edit_or_answer_media(
        callback,
        slot=SLOT_ADMIN_MAIN,
        caption=text,
        keyboard=_build_slots_keyboard(texts),
        parse_mode="HTML",
    )
    await callback.answer()


@admin_required
@error_handler
async def select_spiderman_media_slot(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
    db,
):
    texts = get_texts(db_user.language)
    raw_slot = (callback.data or "").split(":", 1)[-1]
    slot = normalize_slot(raw_slot)
    label = _SLOT_LABELS.get(slot, slot)

    await state.set_state(SpidermanMediaStates.waiting_for_menu_media)
    await state.update_data(media_slot=slot)

    text = (
        "🕷️ Spiderman menu\n\n"
        f"Выбран экран: <b>{label}</b>\n\n"
        "Отправьте медиа (фото/видео/анимацию) и получите file_id для .env."
    )
    await edit_or_answer_media(
        callback,
        slot=SLOT_ADMIN_MAIN,
        caption=text,
        keyboard=_build_navigation_markup(_FILE_ID_MENU_CALLBACK, texts),
        parse_mode="HTML",
    )
    await callback.answer()


def _extract_media_file_id(message: types.Message) -> tuple[Optional[str], Optional[str]]:
    if message.animation:
        return message.animation.file_id, "animation"
    if message.video:
        return message.video.file_id, "video"
    if message.photo:
        return message.photo[-1].file_id, "photo"
    if message.document:
        return message.document.file_id, "document"
    return None, None


@admin_required
@error_handler
async def receive_spiderman_menu_media(
    message: types.Message,
    state: FSMContext,
    db_user,
    db,
):
    texts = get_texts(db_user.language)
    data = await state.get_data()
    slot = normalize_slot(data.get("media_slot"))
    file_id, media_type = _extract_media_file_id(message)

    if not file_id:
        await message.answer(
            "Нужно отправить медиа (фото, видео или гифку).",
            reply_markup=_build_back_cancel_markup(_FILE_ID_MENU_CALLBACK, texts),
        )
        return

    env_key = get_env_key_for_slot(slot) or "SPIDERMAN_MENU_MEDIA_MAIN_MENU"
    label = _SLOT_LABELS.get(slot, slot)

    await state.clear()

    response = (
        "✅ Сохранено\n\n"
        f"📍 Экран: <b>{label}</b>\n"
        f"📎 Тип: <b>{media_type or 'media'}</b>\n"
        f"🆔 file_id: <code>{file_id}</code>\n\n"
        f"Добавьте в .env:\n<code>{env_key}={file_id}</code>"
    )
    await message.answer(
        response,
        parse_mode="HTML",
        reply_markup=_build_back_cancel_markup(_FILE_ID_MENU_CALLBACK, texts),
    )


def register_handlers(dp: Dispatcher) -> None:
    dp.callback_query.register(
        show_spiderman_menu_root,
        F.data == _MAIN_MENU_CALLBACK,
    )
    dp.callback_query.register(
        show_spiderman_file_id_menu,
        F.data == _FILE_ID_MENU_CALLBACK,
    )
    dp.callback_query.register(
        select_spiderman_media_slot,
        F.data.startswith("spider_media_slot:"),
    )
    dp.message.register(
        receive_spiderman_menu_media,
        SpidermanMediaStates.waiting_for_menu_media,
    )
    dp.callback_query.register(
        close_spiderman_message,
        F.data == _CLOSE_MESSAGE_CALLBACK,
    )
