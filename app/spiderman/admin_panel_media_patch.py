import logging
from typing import Any, Optional

from aiogram import types
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardMarkup

from app.config import settings
from app.spiderman.menu_media import (
    SLOT_ADMIN_MAIN,
    SLOT_MAIN_MENU,
    answer_media,
    edit_message_media,
    resolve_media,
)

logger = logging.getLogger(__name__)

_ORIGINAL_MESSAGE_EDIT_TEXT = None
_ADMIN_MEDIA_MESSAGES: set[tuple[int, int]] = set()


def _filter_edit_caption_kwargs(*, reply_markup: Optional[InlineKeyboardMarkup], parse_mode: str) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"parse_mode": parse_mode}
    if reply_markup is not None:
        kwargs["reply_markup"] = reply_markup
    return kwargs


def _keyboard_has_admin_callbacks(keyboard: Optional[InlineKeyboardMarkup]) -> bool:
    if not keyboard or not getattr(keyboard, "inline_keyboard", None):
        return False
    for row in keyboard.inline_keyboard:
        for button in row:
            callback_data = getattr(button, "callback_data", None)
            if not isinstance(callback_data, str):
                continue
            if callback_data == "admin_panel":
                return True
            if callback_data.startswith("admin_"):
                return True
    return False


def _keyboard_has_user_callbacks(keyboard: Optional[InlineKeyboardMarkup]) -> bool:
    if not keyboard or not getattr(keyboard, "inline_keyboard", None):
        return False
    for row in keyboard.inline_keyboard:
        for button in row:
            callback_data = getattr(button, "callback_data", None)
            if not isinstance(callback_data, str):
                continue
            if callback_data == "back_to_menu" or callback_data.startswith("menu_"):
                return True
    return False


def _message_key(message: types.Message) -> Optional[tuple[int, int]]:
    chat = getattr(message, "chat", None)
    chat_id = getattr(chat, "id", None)
    message_id = getattr(message, "message_id", None)
    if isinstance(chat_id, int) and isinstance(message_id, int):
        return (chat_id, message_id)
    return None


async def _edit_text_patched(self: types.Message, text: str, *args: Any, **kwargs: Any):
    if not settings.SPIDERMAN_MODE:
        return await _ORIGINAL_MESSAGE_EDIT_TEXT(self, text, *args, **kwargs)

    reply_markup = kwargs.get("reply_markup")
    key = _message_key(self)
    is_admin_media_message = bool(key and key in _ADMIN_MEDIA_MESSAGES)
    has_admin_callbacks = _keyboard_has_admin_callbacks(reply_markup)
    has_user_callbacks = _keyboard_has_user_callbacks(reply_markup)

    if is_admin_media_message and has_user_callbacks and not has_admin_callbacks:
        if key:
            _ADMIN_MEDIA_MESSAGES.discard(key)
        parse_mode = kwargs.get("parse_mode") or "HTML"
        try:
            await self.delete()
        except Exception:
            pass
        logger.warning("🕷️ Админ-панель: переход в пользовательское меню, отправляю новое сообщение")
        return await answer_media(
            self,
            slot=SLOT_MAIN_MENU,
            caption=text,
            keyboard=reply_markup,
            parse_mode=parse_mode,
        )

    if not is_admin_media_message and not has_admin_callbacks:
        # В пользовательских сценариях сообщение часто является медиа (GIF/видео/фото) с подписью.
        # Telegram запрещает edit_text для таких сообщений - нужно редактировать caption.
        # Дополнительно: иногда в callback.message нет полного набора полей медиа, поэтому
        # пробуем edit_caption "в лоб" и только потом падаем обратно на edit_text.
        parse_mode = kwargs.get("parse_mode") or "HTML"
        try:
            return await self.edit_caption(
                caption=text,
                **_filter_edit_caption_kwargs(reply_markup=reply_markup, parse_mode=parse_mode),
            )
        except TelegramBadRequest as error:
            if "message is not modified" in str(error).lower():
                return self
        except Exception:
            pass
        return await _ORIGINAL_MESSAGE_EDIT_TEXT(self, text, *args, **kwargs)

    media_source = resolve_media(SLOT_ADMIN_MAIN)
    if not media_source:
        return await _ORIGINAL_MESSAGE_EDIT_TEXT(self, text, *args, **kwargs)

    parse_mode = kwargs.get("parse_mode") or "HTML"

    if self.photo or self.video or self.animation:
        updated = await edit_message_media(
            self,
            slot=SLOT_ADMIN_MAIN,
            caption=text,
            keyboard=reply_markup,
            parse_mode=parse_mode,
        )
        if updated:
            if key:
                _ADMIN_MEDIA_MESSAGES.add(key)
            return self
        logger.warning(
            "🕷️ Админ-панель: не удалось обновить медиа через edit_media (caption_len=%d), пересылаю заново",
            len(text or ""),
        )

    try:
        await self.delete()
    except Exception:
        pass

    if media_source.local_path and not media_source.file_id:
        logger.warning("🕷️ Админ-панель: используется локальный fallback (%s)", media_source.local_path)

    sent = await answer_media(
        self,
        slot=SLOT_ADMIN_MAIN,
        caption=text,
        keyboard=reply_markup,
        parse_mode=parse_mode,
    )
    sent_key = _message_key(sent)
    if sent_key:
        _ADMIN_MEDIA_MESSAGES.add(sent_key)
    return sent


def apply_admin_panel_media_patches() -> None:
    global _ORIGINAL_MESSAGE_EDIT_TEXT

    if getattr(types.Message, "_spiderman_admin_panel_media_patched", False):
        return

    _ORIGINAL_MESSAGE_EDIT_TEXT = types.Message.edit_text
    types.Message.edit_text = _edit_text_patched  # type: ignore[assignment]
    types.Message._spiderman_admin_panel_media_patched = True  # type: ignore[attr-defined]

    logger.info("🕷️ Патч медиа для админки применён")
