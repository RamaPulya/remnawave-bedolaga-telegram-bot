import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from aiogram import Bot, types
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError
from aiogram.types import (
    FSInputFile,
    InputMediaAnimation,
    InputMediaPhoto,
    InputMediaVideo,
)

from app.config import settings
from app.utils.message_patch import (
    append_privacy_hint,
    is_privacy_restricted_error,
    prepare_privacy_safe_kwargs,
)

logger = logging.getLogger(__name__)

SLOT_MAIN_MENU = "main_menu"
SLOT_SUBSCRIPTION = "subscription"
SLOT_EXTEND_DAYS = "extend_days"
SLOT_EXTEND_TRAFFIC = "extend_traffic"
SLOT_SUPPORT = "support"
SLOT_REFERRAL = "referral"
SLOT_PURCHASE_SUCCESS = "purchase_success"
SLOT_ADMIN_MAIN = "admin_main"

_SLOT_ALIASES = {
    "partner": SLOT_REFERRAL,
    "partners": SLOT_REFERRAL,
    "referrals": SLOT_REFERRAL,
}

_SLOT_SETTING_MAP = {
    SLOT_MAIN_MENU: "SPIDERMAN_MENU_MEDIA_MAIN_MENU",
    SLOT_SUBSCRIPTION: "SPIDERMAN_MENU_MEDIA_SUBSCRIPTION",
    SLOT_EXTEND_DAYS: "SPIDERMAN_MENU_MEDIA_EXTEND_DAYS",
    SLOT_EXTEND_TRAFFIC: "SPIDERMAN_MENU_MEDIA_EXTEND_TRAFFIC",
    SLOT_SUPPORT: "SPIDERMAN_MENU_MEDIA_SUPPORT",
    SLOT_REFERRAL: "SPIDERMAN_MENU_MEDIA_REFERRAL",
    SLOT_PURCHASE_SUCCESS: "SPIDERMAN_MENU_MEDIA_PURCHASE_SUCCESS",
    SLOT_ADMIN_MAIN: "SPIDERMAN_MENU_ADMIN_MAIN",
}

_MEDIA_TYPE_ORDER = ("animation", "video", "photo")
_MEDIA_CAPTION_LIMIT = 1000
_MAX_RETRIES = 3
_RETRY_DELAY = 0.5

_FILE_ID_TYPE_CACHE: dict[str, str] = {}


@dataclass(frozen=True)
class MediaSource:
    file_id: Optional[str] = None
    local_path: Optional[Path] = None


def normalize_slot(slot: Optional[str]) -> str:
    normalized = (slot or "").strip().lower()
    if normalized in _SLOT_ALIASES:
        return _SLOT_ALIASES[normalized]
    if normalized in _SLOT_SETTING_MAP:
        return normalized
    return SLOT_MAIN_MENU


def get_env_key_for_slot(slot: str) -> Optional[str]:
    return _SLOT_SETTING_MAP.get(normalize_slot(slot))


def _normalize_file_id(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _get_slot_file_id(slot: str) -> Optional[str]:
    setting_key = _SLOT_SETTING_MAP.get(normalize_slot(slot))
    if not setting_key:
        return None
    return _normalize_file_id(getattr(settings, setting_key, None))


def _get_fallback_path() -> Optional[Path]:
    raw_path = (getattr(settings, "SPIDERMAN_MENU_MEDIA_FALLBACK_PATH", "") or "").strip()
    if not raw_path:
        raw_path = settings.LOGO_FILE
    if not raw_path:
        return None
    path = Path(raw_path)
    if not path.is_absolute():
        path = Path(raw_path)
    if path.exists():
        return path
    return None


def resolve_media(slot: str) -> Optional[MediaSource]:
    if not settings.SPIDERMAN_MODE:
        return None
    normalized = normalize_slot(slot)
    file_id = _get_slot_file_id(normalized)
    if not file_id and normalized != SLOT_MAIN_MENU:
        file_id = _get_slot_file_id(SLOT_MAIN_MENU)
    if file_id:
        return MediaSource(file_id=file_id)
    fallback_path = _get_fallback_path()
    if fallback_path:
        return MediaSource(local_path=fallback_path)
    return None


def _build_base_kwargs(
    keyboard: Optional[types.InlineKeyboardMarkup],
    parse_mode: Optional[str],
) -> dict[str, object]:
    kwargs: dict[str, object] = {}
    if parse_mode is not None:
        kwargs["parse_mode"] = parse_mode
    if keyboard is not None:
        kwargs["reply_markup"] = keyboard
    return kwargs


def _get_caption_language(callback: types.CallbackQuery) -> Optional[str]:
    user = getattr(callback, "from_user", None)
    language_code = getattr(user, "language_code", None)
    if language_code:
        return language_code
    return None


def _is_message_not_modified(error: TelegramBadRequest) -> bool:
    return "message is not modified" in str(error).lower()


async def _try_update_reply_markup(
    message: types.Message,
    keyboard: Optional[types.InlineKeyboardMarkup],
) -> None:
    if not keyboard:
        return

    try:
        await message.edit_reply_markup(reply_markup=keyboard)
    except TelegramBadRequest as error:
        if _is_message_not_modified(error):
            return
        return
    except TelegramNetworkError:
        return


async def _answer_text(
    callback: types.CallbackQuery,
    caption: str,
    keyboard: Optional[types.InlineKeyboardMarkup],
    parse_mode: Optional[str],
    error: Optional[TelegramBadRequest] = None,
) -> None:
    language = _get_caption_language(callback)
    kwargs = _build_base_kwargs(keyboard, parse_mode)

    if error and is_privacy_restricted_error(error):
        caption = append_privacy_hint(caption, language)
        kwargs = prepare_privacy_safe_kwargs(kwargs)

    kwargs.setdefault("parse_mode", parse_mode or "HTML")
    await callback.message.answer(caption, **kwargs)


async def _edit_or_answer_text(
    callback: types.CallbackQuery,
    caption: str,
    keyboard: Optional[types.InlineKeyboardMarkup],
    parse_mode: Optional[str],
) -> None:
    resolved_parse_mode = parse_mode or "HTML"
    try:
        if callback.message.photo or callback.message.video or callback.message.animation:
            await callback.message.edit_caption(
                caption=caption,
                reply_markup=keyboard,
                parse_mode=resolved_parse_mode,
            )
        else:
            await callback.message.edit_text(
                caption,
                reply_markup=keyboard,
                parse_mode=resolved_parse_mode,
            )
    except TelegramBadRequest as error:
        if _is_message_not_modified(error):
            return
        await _answer_text(callback, caption, keyboard, resolved_parse_mode, error)


def _get_local_media_type(path: Path) -> Optional[str]:
    suffix = path.suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
        return "photo"
    if suffix in {".gif"}:
        return "animation"
    if suffix in {".mp4", ".mov", ".mkv", ".webm"}:
        return "video"
    return None


def _get_media_type_candidates(source: MediaSource) -> Iterable[str]:
    if source.file_id:
        cached = _FILE_ID_TYPE_CACHE.get(source.file_id)
        if cached:
            return [cached]
        return list(_MEDIA_TYPE_ORDER)
    if source.local_path:
        detected = _get_local_media_type(source.local_path)
        if detected:
            return [detected]
        return list(_MEDIA_TYPE_ORDER)
    return list(_MEDIA_TYPE_ORDER)


def _build_media_payload(source: MediaSource) -> str | FSInputFile:
    if source.file_id:
        return source.file_id
    return FSInputFile(source.local_path)


async def _send_media_with_type(
    message: types.Message,
    media_type: str,
    payload: str | FSInputFile,
    caption: str,
    keyboard: Optional[types.InlineKeyboardMarkup],
    parse_mode: Optional[str],
) -> types.Message:
    for attempt in range(_MAX_RETRIES):
        try:
            if media_type == "animation":
                return await message.answer_animation(
                    animation=payload,
                    caption=caption,
                    reply_markup=keyboard,
                    parse_mode=parse_mode,
                )
            if media_type == "video":
                return await message.answer_video(
                    video=payload,
                    caption=caption,
                    reply_markup=keyboard,
                    parse_mode=parse_mode,
                )
            return await message.answer_photo(
                photo=payload,
                caption=caption,
                reply_markup=keyboard,
                parse_mode=parse_mode,
            )
        except TelegramNetworkError as error:
            if attempt < _MAX_RETRIES - 1:
                logger.warning(
                    "🌐 Сетевая ошибка отправки медиа (%s), попытка %d/%d: %s",
                    media_type,
                    attempt + 1,
                    _MAX_RETRIES,
                    error,
                )
                await asyncio.sleep(_RETRY_DELAY * (attempt + 1))
                continue
            raise


async def _edit_media_with_type(
    message: types.Message,
    media_type: str,
    payload: str | FSInputFile,
    caption: str,
    keyboard: Optional[types.InlineKeyboardMarkup],
    parse_mode: Optional[str],
) -> None:
    for attempt in range(_MAX_RETRIES):
        try:
            if media_type == "animation":
                media = InputMediaAnimation(
                    media=payload,
                    caption=caption,
                    parse_mode=parse_mode,
                )
            elif media_type == "video":
                media = InputMediaVideo(
                    media=payload,
                    caption=caption,
                    parse_mode=parse_mode,
                )
            else:
                media = InputMediaPhoto(
                    media=payload,
                    caption=caption,
                    parse_mode=parse_mode,
                )
            await message.edit_media(media, reply_markup=keyboard)
            return
        except TelegramNetworkError as error:
            if attempt < _MAX_RETRIES - 1:
                logger.warning(
                    "🌐 Сетевая ошибка редактирования медиа (%s), попытка %d/%d: %s",
                    media_type,
                    attempt + 1,
                    _MAX_RETRIES,
                    error,
                )
                await asyncio.sleep(_RETRY_DELAY * (attempt + 1))
                continue
            raise


async def _send_or_edit_media(
    callback: types.CallbackQuery,
    source: MediaSource,
    caption: str,
    keyboard: Optional[types.InlineKeyboardMarkup],
    parse_mode: Optional[str],
) -> bool:
    payload = _build_media_payload(source)
    candidates = list(_get_media_type_candidates(source))
    last_error: Optional[Exception] = None
    message = callback.message

    for media_type in candidates:
        try:
            if message.photo or message.video or message.animation:
                await _edit_media_with_type(message, media_type, payload, caption, keyboard, parse_mode)
            else:
                try:
                    await message.delete()
                except Exception:
                    pass
                await _send_media_with_type(message, media_type, payload, caption, keyboard, parse_mode)

            if source.file_id:
                _FILE_ID_TYPE_CACHE[source.file_id] = media_type
            return True
        except TelegramBadRequest as error:
            last_error = error
            if _is_message_not_modified(error):
                if message:
                    await _try_update_reply_markup(message, keyboard)
                return True
            if source.file_id and media_type in _FILE_ID_TYPE_CACHE:
                _FILE_ID_TYPE_CACHE.pop(source.file_id, None)
            continue
        except TelegramNetworkError as error:
            last_error = error
            break

    if last_error:
        logger.warning("⚠️ Не удалось отправить медиа (%s): %s", source.file_id or source.local_path, last_error)
    return False


async def edit_or_answer_media(
    callback: types.CallbackQuery,
    *,
    slot: str,
    caption: str,
    keyboard: Optional[types.InlineKeyboardMarkup],
    parse_mode: Optional[str] = "HTML",
    force_text: bool = False,
) -> None:
    if not callback.message:
        return

    if force_text or not settings.SPIDERMAN_MODE:
        await _edit_or_answer_text(callback, caption, keyboard, parse_mode)
        return

    if caption and len(caption) > _MEDIA_CAPTION_LIMIT:
        await _edit_or_answer_text(callback, caption, keyboard, parse_mode)
        return

    source = resolve_media(slot)
    if not source:
        await _edit_or_answer_text(callback, caption, keyboard, parse_mode)
        return

    try:
        success = await _send_or_edit_media(callback, source, caption, keyboard, parse_mode)
        if not success:
            await _edit_or_answer_text(callback, caption, keyboard, parse_mode)
    except TelegramBadRequest as error:
        if is_privacy_restricted_error(error):
            await _edit_or_answer_text(callback, caption, keyboard, parse_mode)
            return
        await _edit_or_answer_text(callback, caption, keyboard, parse_mode)
    except TelegramNetworkError as error:
        logger.warning("🌐 Сетевая ошибка отправки медиа: %s", error)
        await _edit_or_answer_text(callback, caption, keyboard, parse_mode)


async def answer_media(
    message: types.Message,
    *,
    slot: str,
    caption: str,
    keyboard: Optional[types.InlineKeyboardMarkup],
    parse_mode: Optional[str] = "HTML",
) -> types.Message:
    if not settings.SPIDERMAN_MODE:
        return await message.answer(caption, reply_markup=keyboard, parse_mode=parse_mode)

    if caption and len(caption) > _MEDIA_CAPTION_LIMIT:
        return await message.answer(caption, reply_markup=keyboard, parse_mode=parse_mode)

    source = resolve_media(slot)
    if not source:
        return await message.answer(caption, reply_markup=keyboard, parse_mode=parse_mode)

    payload = _build_media_payload(source)
    candidates = list(_get_media_type_candidates(source))
    last_error: Optional[Exception] = None

    for media_type in candidates:
        try:
            sent_message = await _send_media_with_type(message, media_type, payload, caption, keyboard, parse_mode)
            if source.file_id:
                _FILE_ID_TYPE_CACHE[source.file_id] = media_type
            return sent_message
        except TelegramBadRequest as error:
            last_error = error
            if source.file_id and media_type in _FILE_ID_TYPE_CACHE:
                _FILE_ID_TYPE_CACHE.pop(source.file_id, None)
            continue
        except TelegramNetworkError as error:
            last_error = error
            break

    if last_error:
        logger.warning("⚠️ Не удалось отправить медиа (%s): %s", source.file_id or source.local_path, last_error)
    return await message.answer(caption, reply_markup=keyboard, parse_mode=parse_mode)


async def send_media_to_chat(
    bot: Bot,
    *,
    chat_id: str | int,
    slot: str,
    caption: str,
    keyboard: Optional[types.InlineKeyboardMarkup],
    parse_mode: Optional[str] = "HTML",
) -> types.Message:
    if not settings.SPIDERMAN_MODE:
        return await bot.send_message(
            chat_id=chat_id,
            text=caption,
            reply_markup=keyboard,
            parse_mode=parse_mode,
        )

    if caption and len(caption) > _MEDIA_CAPTION_LIMIT:
        return await bot.send_message(
            chat_id=chat_id,
            text=caption,
            reply_markup=keyboard,
            parse_mode=parse_mode,
        )

    source = resolve_media(slot)
    if not source:
        return await bot.send_message(
            chat_id=chat_id,
            text=caption,
            reply_markup=keyboard,
            parse_mode=parse_mode,
        )

    payload = _build_media_payload(source)
    candidates = list(_get_media_type_candidates(source))
    last_error: Optional[Exception] = None

    for media_type in candidates:
        try:
            if media_type == "animation":
                sent = await bot.send_animation(
                    chat_id=chat_id,
                    animation=payload,
                    caption=caption,
                    reply_markup=keyboard,
                    parse_mode=parse_mode,
                )
            elif media_type == "video":
                sent = await bot.send_video(
                    chat_id=chat_id,
                    video=payload,
                    caption=caption,
                    reply_markup=keyboard,
                    parse_mode=parse_mode,
                )
            else:
                sent = await bot.send_photo(
                    chat_id=chat_id,
                    photo=payload,
                    caption=caption,
                    reply_markup=keyboard,
                    parse_mode=parse_mode,
                )
            if source.file_id:
                _FILE_ID_TYPE_CACHE[source.file_id] = media_type
            return sent
        except TelegramBadRequest as error:
            last_error = error
            if source.file_id and media_type in _FILE_ID_TYPE_CACHE:
                _FILE_ID_TYPE_CACHE.pop(source.file_id, None)
            continue
        except TelegramNetworkError as error:
            last_error = error
            break

    if last_error:
        logger.warning(
            "🕷️ Не удалось отправить медиа (%s): %s",
            source.file_id or source.local_path,
            last_error,
        )
    return await bot.send_message(
        chat_id=chat_id,
        text=caption,
        reply_markup=keyboard,
        parse_mode=parse_mode,
    )


async def edit_message_media(
    message: types.Message,
    *,
    slot: str,
    caption: str,
    keyboard: Optional[types.InlineKeyboardMarkup],
    parse_mode: Optional[str] = "HTML",
) -> bool:
    if not settings.SPIDERMAN_MODE:
        return False

    if caption and len(caption) > _MEDIA_CAPTION_LIMIT:
        return False

    if not (message.photo or message.video or message.animation):
        return False

    source = resolve_media(slot)
    if not source:
        return False

    payload = _build_media_payload(source)
    candidates = list(_get_media_type_candidates(source))
    last_error: Optional[Exception] = None

    for media_type in candidates:
        try:
            await _edit_media_with_type(message, media_type, payload, caption, keyboard, parse_mode)
            if source.file_id:
                _FILE_ID_TYPE_CACHE[source.file_id] = media_type
            return True
        except TelegramBadRequest as error:
            last_error = error
            if _is_message_not_modified(error):
                await _try_update_reply_markup(message, keyboard)
                return True
            if source.file_id and media_type in _FILE_ID_TYPE_CACHE:
                _FILE_ID_TYPE_CACHE.pop(source.file_id, None)
            continue
        except TelegramNetworkError as error:
            last_error = error
            break

    if last_error:
        logger.warning("⚠️ Не удалось обновить медиа в сообщении (%s): %s", source.file_id or source.local_path, last_error)
    return False
