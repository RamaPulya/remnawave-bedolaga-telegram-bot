import importlib

from aiogram import F, types
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

from app.config import settings
from app.database.crud.user import get_user_by_telegram_id
from app.database.models import UserStatus
from app.localization.loader import DEFAULT_LANGUAGE
from app.localization.texts import get_texts


_FALLBACK_LABELS = {
    '🕷️ Главное меню',
    '🕷 Главное меню',
    '🕷️ Main menu',
    '🕷 Main menu',
}


def _get_reply_main_menu_label(language: str | None) -> str:
    texts = get_texts(language or DEFAULT_LANGUAGE)
    return texts.t('REPLY_MAIN_MENU_BUTTON', '🕷️ Главное меню')


def _get_reply_main_menu_keyboard(language: str | None = None) -> ReplyKeyboardMarkup:
    label = _get_reply_main_menu_label(language)
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=label)]],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


async def _ensure_reply_main_menu_button(
    message: types.Message,
    db,
    user,
) -> None:
    settings_dict = dict(getattr(user, 'notification_settings', None) or {})
    if settings_dict.get('reply_main_menu_keyboard_set'):
        return

    try:
        await message.bot.send_message(
            chat_id=message.chat.id,
            text='\u2063',
            reply_markup=_get_reply_main_menu_keyboard(getattr(user, 'language', None)),
            disable_notification=True,
        )
        settings_dict['reply_main_menu_keyboard_set'] = True
        user.notification_settings = settings_dict
        await db.commit()
    except Exception:
        return


async def _handle_reply_main_menu(
    message: types.Message,
    state,
    db,
    db_user=None,
):
    text = (message.text or '').strip()
    language = getattr(db_user, 'language', None)

    if not language:
        user = db_user or await get_user_by_telegram_id(db, message.from_user.id)
        language = getattr(user, 'language', None) if user else None

    expected_label = _get_reply_main_menu_label(language)
    if text not in _FALLBACK_LABELS and text != expected_label:
        return

    start_handlers = importlib.import_module('app.handlers.start')

    data = await state.get_data() or {}
    if 'pending_start_payload' in data or 'campaign_notification_sent' in data:
        data.pop('pending_start_payload', None)
        data.pop('campaign_notification_sent', None)
        await state.set_data(data)

    safe_message = message
    text_overridden = False
    original_text = message.text

    try:
        if hasattr(message, 'model_copy'):
            safe_message = message.model_copy(update={'text': '/start'})
        else:
            message.text = '/start'
            text_overridden = True
    except Exception:
        safe_message = message

    try:
        await start_handlers.cmd_start(safe_message, state, db, db_user=db_user)
    finally:
        if text_overridden:
            message.text = original_text


def apply_reply_main_menu_patches() -> None:
    start_handlers = importlib.import_module('app.handlers.start')

    if getattr(start_handlers, '_spiderman_reply_main_menu_patched', False):
        return

    original_cmd_start = start_handlers.cmd_start
    original_register_handlers = start_handlers.register_handlers

    async def patched_cmd_start(message, state, db, db_user=None):
        await original_cmd_start(message, state, db, db_user=db_user)

        if not settings.SPIDERMAN_MODE:
            return

        user = db_user or await get_user_by_telegram_id(db, message.from_user.id)
        if not user or getattr(user, 'status', None) != UserStatus.ACTIVE.value:
            return

        await _ensure_reply_main_menu_button(message, db, user)

    def patched_register_handlers(dp):
        dp.message.register(_handle_reply_main_menu, F.text.in_(sorted(_FALLBACK_LABELS)))
        original_register_handlers(dp)

    start_handlers.cmd_start = patched_cmd_start
    start_handlers.register_handlers = patched_register_handlers
    start_handlers._spiderman_reply_main_menu_patched = True
