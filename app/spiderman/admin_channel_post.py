import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, MessageEntity

from app.config import settings
from app.localization.texts import get_texts
from app.spiderman.menu_media import SLOT_ADMIN_MAIN, answer_media, edit_or_answer_media
from app.spiderman.states import SpidermanChannelPostStates
from app.utils.decorators import admin_required, error_handler


logger = logging.getLogger(__name__)

_MAIN_MENU_CALLBACK = 'admin_spiderman_menu'
_CHANNEL_POST_ROOT = 'admin_spiderman_channel_post'
_CHANNEL_POST_ACTION_PREFIX = 'spiderman_channel_post:'
_CHANNEL_POST_SEND = 'spiderman_channel_post:send'
_CHANNEL_POST_CANCEL = 'spiderman_channel_post:cancel'
_CHANNEL_POST_STATE_KEY = 'channel_post_content'


@dataclass(frozen=True)
class ChannelPostContent:
    content_type: str
    file_id: Optional[str]
    text: Optional[str]
    entities: list[dict[str, Any]]
    caption_entities: list[dict[str, Any]]

    def to_state(self) -> dict[str, Any]:
        return {
            'content_type': self.content_type,
            'file_id': self.file_id,
            'text': self.text,
            'entities': self.entities,
            'caption_entities': self.caption_entities,
        }

    @classmethod
    def from_state(cls, data: dict[str, Any]) -> 'ChannelPostContent':
        return cls(
            content_type=data['content_type'],
            file_id=data.get('file_id'),
            text=data.get('text'),
            entities=data.get('entities', []),
            caption_entities=data.get('caption_entities', []),
        )

    @classmethod
    def from_message(cls, message: types.Message) -> Optional['ChannelPostContent']:
        media_type = _detect_media_type(message)
        text = message.caption if message.caption else message.text
        file_id = _extract_file_id(message)

        if not media_type and not text:
            return None

        return cls(
            content_type=media_type or 'text',
            file_id=file_id,
            text=text,
            entities=_serialize_entities(message.entities or []),
            caption_entities=_serialize_entities(message.caption_entities or []),
        )


def _serialize_entities(entities: Iterable[MessageEntity]) -> list[dict[str, Any]]:
    return [entity.model_dump() for entity in entities]


def _deserialize_entities(source: list[dict[str, Any]]) -> list[MessageEntity]:
    return [MessageEntity(**data) for data in source]


def _detect_media_type(message: types.Message) -> Optional[str]:
    if message.animation:
        return 'animation'
    if message.video:
        return 'video'
    if message.photo:
        return 'photo'
    if message.document:
        return 'document'
    return None


def _extract_file_id(message: types.Message) -> Optional[str]:
    media = message.animation or message.video or (message.photo[-1] if message.photo else None) or message.document
    return getattr(media, 'file_id', None)


def _normalize_channel_target(raw: Optional[str]) -> str | int | None:
    if not raw:
        return None

    value = str(raw).strip()
    if not value:
        return None

    if '#' in value:
        value = value.split('#', 1)[0].strip()
        if not value:
            return None

    if value.startswith(('http://', 'https://')):
        parsed = urlparse(value)
        if parsed.netloc in {'t.me', 'telegram.me', 'telegram.dog'}:
            path = (parsed.path or '').strip('/').split('/', 1)[0]
            if path:
                value = f'@{path}'

    if value.startswith(('t.me/', 'telegram.me/', 'telegram.dog/')):
        path = value.split('/', 1)[-1].strip('/').split('/', 1)[0]
        if path:
            value = f'@{path}'

    if value.startswith('@'):
        return value

    if re.fullmatch(r'-?\d+', value):
        try:
            return int(value)
        except ValueError:
            return None

    return value


def _get_channel_target() -> str | int | None:
    for key in ('CHANNEL_POST_ID', 'CHANNEL_SUB_ID'):
        raw = getattr(settings, key, None)
        target = _normalize_channel_target(raw)
        if target is not None:
            return target
    return None


def _build_back_markup(texts) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=texts.BACK, callback_data=_MAIN_MENU_CALLBACK)]]
    )


def _build_confirm_markup(texts) -> InlineKeyboardMarkup:
    builder = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=texts.t('ADMIN_SPIDERMAN_CHANNEL_POST_SEND', '🚀 Опубликовать'),
                    callback_data=_CHANNEL_POST_SEND,
                ),
                InlineKeyboardButton(
                    text=texts.t('ADMIN_SPIDERMAN_CHANNEL_POST_CANCEL', '❌ Отмена'),
                    callback_data=_CHANNEL_POST_CANCEL,
                ),
            ]
        ]
    )
    return builder


async def _send_content(
    bot: Bot,
    chat_id: str | int,
    content: ChannelPostContent,
    reply_markup: InlineKeyboardMarkup | None,
    parse_mode: str = 'HTML',
) -> types.Message:
    caption = content.text
    if content.content_type == 'text':
        return await bot.send_message(
            chat_id=chat_id,
            text=caption or '',
            parse_mode=parse_mode,
            entities=_deserialize_entities(content.entities) or None,
            disable_web_page_preview=True,
            disable_notification=True,
            reply_markup=reply_markup,
        )

    kwargs = dict(
        chat_id=chat_id,
        parse_mode=parse_mode,
        reply_markup=reply_markup,
        disable_notification=True,
    )

    if content.content_type == 'photo':
        kwargs.update(
            photo=content.file_id,
            caption=caption,
            caption_entities=_deserialize_entities(content.caption_entities) or None,
        )
        return await bot.send_photo(**kwargs)

    if content.content_type == 'video':
        kwargs.update(
            video=content.file_id,
            caption=caption,
            caption_entities=_deserialize_entities(content.caption_entities) or None,
        )
        return await bot.send_video(**kwargs)

    if content.content_type == 'animation':
        kwargs.update(
            animation=content.file_id,
            caption=caption,
            caption_entities=_deserialize_entities(content.caption_entities) or None,
        )
        return await bot.send_animation(**kwargs)

    if content.content_type == 'document':
        kwargs.update(
            document=content.file_id,
            caption=caption,
            caption_entities=_deserialize_entities(content.caption_entities) or None,
        )
        return await bot.send_document(**kwargs)

    raise ValueError(f'Unsupported content type: {content.content_type}')


@admin_required
@error_handler
async def show_spiderman_channel_post_root(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
    db,
):
    texts = get_texts(db_user.language)
    if state is not None:
        await state.clear()
    title = texts.t(
        'ADMIN_SPIDERMAN_CHANNEL_POST_TITLE',
        '🕷️ Spiderman menu\n\n📢 Пост в канал',
    )
    prompt = texts.t(
        'ADMIN_SPIDERMAN_CHANNEL_POST_PROMPT',
        'Отправьте текст или медиа, которое нужно разместить в канале (фото, видео, анимация, документ).',
    )
    await edit_or_answer_media(
        callback,
        slot=SLOT_ADMIN_MAIN,
        caption=f'{title}\n\n{prompt}',
        keyboard=_build_back_markup(texts),
        parse_mode='HTML',
    )
    await callback.answer()
    await state.set_state(SpidermanChannelPostStates.waiting_for_post)


@admin_required
@error_handler
async def receive_channel_post_message(
    message: types.Message,
    state: FSMContext,
    db_user,
    db,
):
    texts = get_texts(db_user.language)
    content = ChannelPostContent.from_message(message)
    if not content:
        await message.answer(
            texts.t(
                'ADMIN_SPIDERMAN_CHANNEL_POST_EMPTY',
                'Нужно отправить текст или медиа для публикации.',
            ),
            reply_markup=_build_back_markup(texts),
        )
        return

    await state.update_data(
        {
            _CHANNEL_POST_STATE_KEY: content.to_state(),
        }
    )
    await state.set_state(SpidermanChannelPostStates.confirming_post)

    try:
        await message.delete()
    except Exception:
        pass

    await _send_content(
        message.bot,
        message.chat.id,
        content,
        reply_markup=_build_confirm_markup(texts),
    )


@admin_required
@error_handler
async def handle_channel_post_confirm(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
    db,
    bot: Bot,
):
    texts = get_texts(db_user.language)
    action = (callback.data or '').split(':', 1)[-1]
    stored = await state.get_data()
    content_data = stored.get(_CHANNEL_POST_STATE_KEY)

    target = _get_channel_target()
    if not target:
        await callback.message.edit_text(
            texts.t(
                'ADMIN_SPIDERMAN_CHANNEL_POST_NO_CHANNEL',
                'Нужно указать CHANNEL_POST_ID или CHANNEL_SUB_ID в .env, чтобы публиковать сообщения.',
            ),
            reply_markup=_build_back_markup(texts),
            parse_mode='HTML',
        )
        await state.clear()
        await callback.answer()
        return

    if not content_data:
        await callback.answer()
        return

    content = ChannelPostContent.from_state(content_data)

    if action == 'send':
        try:
            await _send_content(bot, target, content, reply_markup=None)
            success_text = texts.t(
                'ADMIN_SPIDERMAN_CHANNEL_POST_SUCCESS',
                '✅ Пост опубликован.',
            )
        except TelegramBadRequest as error:
            logger.exception('❌ Не удалось опубликовать пост в канал: %s', error)
            error_text = str(error)
            if 'chat not found' in error_text.lower():
                success_text = (
                    '❌ Канал/чат не найден.\n\n'
                    'Проверьте `CHANNEL_POST_ID`/`CHANNEL_SUB_ID` и добавьте бота в канал (лучше админом с правом постить).'
                )
            else:
                success_text = texts.t(
                    'ADMIN_SPIDERMAN_CHANNEL_POST_ERROR',
                    '❌ Ошибка при попытке отправить сообщение: {error}',
                ).format(error=error)
        except Exception as error:
            logger.exception('❌ Не удалось опубликовать пост в канал: %s', error)
            success_text = texts.t(
                'ADMIN_SPIDERMAN_CHANNEL_POST_ERROR',
                '❌ Ошибка при попытке отправить сообщение: {error}',
            ).format(error=error)
    else:
        success_text = texts.t(
            'ADMIN_SPIDERMAN_CHANNEL_POST_CANCELED',
            '❌ Публикация отменена.',
        )

    if callback.message:
        try:
            await callback.message.delete()
        except Exception:
            pass
        await answer_media(
            callback.message,
            slot=SLOT_ADMIN_MAIN,
            caption=success_text,
            keyboard=_build_back_markup(texts),
            parse_mode='HTML',
        )

    await state.clear()
    await callback.answer()


def register_handlers(dp: Dispatcher) -> None:
    dp.callback_query.register(
        show_spiderman_channel_post_root,
        F.data == _CHANNEL_POST_ROOT,
    )
    dp.message.register(
        receive_channel_post_message,
        SpidermanChannelPostStates.waiting_for_post,
    )
    dp.callback_query.register(
        handle_channel_post_confirm,
        F.data.startswith(_CHANNEL_POST_ACTION_PREFIX),
        SpidermanChannelPostStates.confirming_post,
    )
