import logging
from typing import Optional

from aiogram import Bot, types
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database.crud.user import add_user_balance, get_user_by_id, subtract_user_balance
from app.database.models import PaymentMethod, User
from app.localization.texts import get_texts
from app.services.user_service import UserService

logger = logging.getLogger(__name__)

_ORIGINAL_SEND_BALANCE_NOTIFICATION = None
_ORIGINAL_UPDATE_USER_BALANCE = None


async def _send_balance_notification(
    self,
    bot: Bot,
    user: User,
    amount_kopeks: int,
    admin_name: str,
) -> bool:
    """Отправляет уведомление пользователю о пополнении/списании баланса."""
    try:
        if amount_kopeks > 0:
            emoji = "💰"
            amount_text = f"+{settings.format_price(amount_kopeks)}"
            message = (
                f"{emoji} <b>Баланс пополнен!</b>\n\n"
                f"💵 <b>Сумма:</b> {amount_text}\n"
                f"👤 <b>Администратор:</b> {admin_name}\n"
                f"💳 <b>Текущий баланс:</b> {settings.format_price(user.balance_kopeks)}\n\n"
                f"Спасибо за использование нашего сервиса! 🎉"
            )
        else:
            emoji = "💸"
            amount_text = f"-{settings.format_price(abs(amount_kopeks))}"
            message = (
                f"{emoji} <b>Средства списаны с баланса</b>\n\n"
                f"💵 <b>Сумма:</b> {amount_text}\n"
                f"👤 <b>Администратор:</b> {admin_name}\n"
                f"💳 <b>Текущий баланс:</b> {settings.format_price(user.balance_kopeks)}\n\n"
                f"Если у вас есть вопросы, обратитесь в поддержку."
            )

        keyboard_rows = []
        if getattr(user, "subscription", None) and user.subscription.status in {
            "active",
            "expired",
            "trial",
        }:
            keyboard_rows.append([
                types.InlineKeyboardButton(
                    text=get_texts(user.language).t("SUBSCRIPTION_EXTEND", "💎 Продлить подписку"),
                    callback_data="subscription_extend",
                )
            ])

        reply_markup = (
            types.InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
            if keyboard_rows
            else None
        )

        await bot.send_message(
            chat_id=user.telegram_id,
            text=message,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )

        logger.info(
            "✅ Уведомление об изменении баланса отправлено пользователю %s",
            user.telegram_id,
        )
        return True

    except TelegramForbiddenError:
        logger.warning("⚠️ Пользователь %s заблокировал бота", user.telegram_id)
        return False
    except TelegramBadRequest as exc:
        logger.error(
            "❌ Ошибка Telegram API при отправке уведомления пользователю %s: %s",
            user.telegram_id,
            exc,
        )
        return False
    except Exception as exc:
        logger.error(
            "❌ Неожиданная ошибка при отправке уведомления пользователю %s: %s",
            user.telegram_id,
            exc,
        )
        return False


async def update_user_balance(
    self,
    db: AsyncSession,
    user_id: int,
    amount_kopeks: int,
    description: str,
    admin_id: int,
    bot: Optional[Bot] = None,
    admin_name: Optional[str] = None,
) -> bool:
    try:
        user = await get_user_by_id(db, user_id)
        if not user:
            return False

        if amount_kopeks > 0:
            await add_user_balance(
                db,
                user,
                amount_kopeks,
                description=description,
                payment_method=PaymentMethod.MANUAL,
            )
            logger.info(
                "Админ %s пополнил баланс пользователя %s на %s ₽",
                admin_id,
                user_id,
                amount_kopeks / 100,
            )
            success = True
        else:
            success = await subtract_user_balance(
                db,
                user,
                abs(amount_kopeks),
                description,
                create_transaction=True,
                payment_method=PaymentMethod.MANUAL,
            )
            if success:
                logger.info(
                    "Админ %s списал с баланса пользователя %s %s ₽",
                    admin_id,
                    user_id,
                    abs(amount_kopeks) / 100,
                )

        if success and bot:
            await db.refresh(user)

            if not admin_name:
                admin_user = await get_user_by_id(db, admin_id)
                admin_name = admin_user.full_name if admin_user else f"Админ #{admin_id}"

            await _send_balance_notification(self, bot, user, amount_kopeks, admin_name)

        return success

    except Exception as exc:
        logger.error("Ошибка изменения баланса пользователя: %s", exc)
        return False


def apply_user_service_patches() -> None:
    if getattr(UserService, "_spiderman_user_service_patched", False):
        return

    global _ORIGINAL_SEND_BALANCE_NOTIFICATION
    global _ORIGINAL_UPDATE_USER_BALANCE
    _ORIGINAL_SEND_BALANCE_NOTIFICATION = UserService._send_balance_notification
    _ORIGINAL_UPDATE_USER_BALANCE = UserService.update_user_balance

    UserService._send_balance_notification = _send_balance_notification
    UserService.update_user_balance = update_user_balance

    UserService._spiderman_user_service_patched = True
