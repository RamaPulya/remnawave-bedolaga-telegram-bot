import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database.models import PaymentMethod, SubscriptionStatus, TransactionType, User
from app.services.admin_notification_service import AdminNotificationService
from app.services.subscription_checkout_service import clear_subscription_checkout_draft
from app.services.subscription_service import SubscriptionService
from app.services.user_cart_service import user_cart_service
from app.spiderman.tariff_context import TariffCode, normalize_tariff_code
from app.localization.texts import get_texts

logger = logging.getLogger(__name__)

_ORIGINAL_AUTO_PURCHASE_SAVED_CART_AFTER_TOPUP = None
_ORIGINAL_AUTO_ACTIVATE_SUBSCRIPTION_AFTER_TOPUP = None


def _parse_uuid_list(raw_value: Optional[object]) -> List[str]:
    if not raw_value:
        return []
    if isinstance(raw_value, list):
        return [str(v).strip() for v in raw_value if str(v).strip()]
    items: List[str] = []
    for chunk in str(raw_value).split(","):
        value = chunk.strip()
        if value:
            items.append(value)
    return items


def _get_white_unlimited_end_date() -> datetime:
    return datetime(2099, 1, 1)


def _safe_int(value: Optional[object], default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _extract_white_cart_countries(cart_data: Dict[str, Any]) -> List[str]:
    for key in ("countries", "connected_squads", "selected_countries"):
        values = _parse_uuid_list(cart_data.get(key))
        if values:
            return values
    return _parse_uuid_list(getattr(settings, "WHITE_TARIFF_SQUADS", ""))


async def _auto_purchase_white_cart_after_topup(
    db: AsyncSession,
    user: User,
    cart_data: Dict[str, Any],
    *,
    bot: Optional[Bot] = None,
) -> bool:
    if not settings.is_auto_purchase_after_topup_enabled():
        return False

    from app.database.crud.server_squad import add_user_to_servers, get_server_ids_by_uuids
    from app.database.crud.transaction import create_transaction
    from app.database.crud.user import get_user_by_id, subtract_user_balance
    from app.database.crud import subscription as subscription_crud

    fresh_user = await get_user_by_id(db, user.id)
    if not fresh_user:
        logger.warning("❌ Автопокупка White: не удалось перезагрузить пользователя %s", user.telegram_id)
        return False
    user = fresh_user

    final_traffic_gb = cart_data.get("final_traffic_gb", cart_data.get("traffic_gb"))
    if final_traffic_gb is None:
        logger.warning("❌ Автопокупка White: в корзине нет выбранного трафика (user=%s)", user.telegram_id)
        return False
    traffic_gb = _safe_int(final_traffic_gb, -1)
    if traffic_gb < 0:
        logger.warning(
            "❌ Автопокупка White: некорректный трафик в корзине: %s (user=%s)",
            final_traffic_gb,
            user.telegram_id,
        )
        return False

    countries = _extract_white_cart_countries(cart_data)
    if not countries:
        logger.warning("❌ Автопокупка White: в корзине нет стран/серверов (user=%s)", user.telegram_id)
        return False

    final_price = _safe_int(
        cart_data.get("total_price")
        or cart_data.get("final_price")
        or cart_data.get("price")
        or 0
    )
    if final_price <= 0:
        logger.warning(
            "❌ Автопокупка White: некорректная сумма корзины: %s (user=%s)",
            cart_data.get("total_price") or cart_data.get("final_price") or cart_data.get("price"),
            user.telegram_id,
        )
        return False

    logger.info(
        "🧾 Автопокупка White: корзина user=%s traffic=%sGB countries=%s price=%s",
        user.telegram_id,
        traffic_gb,
        len(countries),
        final_price,
    )

    if user.balance_kopeks < final_price:
        logger.info(
            "💡 Автопокупка White: недостаточно баланса user=%s (%s < %s)",
            user.telegram_id,
            user.balance_kopeks,
            final_price,
        )
        return False

    consume_promo_offer = bool(cart_data.get("promo_offer_discount_value"))
    description = f"Автопокупка трафика {traffic_gb} ГБ (White)"
    if traffic_gb == 0:
        description = "Автопокупка трафика ♾️ (White)"

    success = await subtract_user_balance(
        db=db,
        user=user,
        amount_kopeks=final_price,
        description=description,
        consume_promo_offer=consume_promo_offer,
    )
    if not success:
        return False

    try:
        from app.utils.user_utils import mark_user_as_had_paid_subscription

        await mark_user_as_had_paid_subscription(db, user)
    except Exception as error:
        logger.warning("⚠️ Автопокупка White: не удалось отметить paid subscription (user=%s): %s", user.telegram_id, error)

    subscription = await subscription_crud.get_subscription_by_user_id(
        db,
        user.id,
        tariff_code=TariffCode.WHITE.value,
    )

    now = datetime.utcnow()
    if subscription:
        subscription.is_trial = False
        subscription.status = SubscriptionStatus.ACTIVE.value
        subscription.updated_at = now
        subscription.end_date = _get_white_unlimited_end_date()
        subscription.connected_squads = list(countries)

        if traffic_gb == 0:
            subscription.traffic_limit_gb = 0
            subscription.purchased_traffic_gb = 0
        else:
            current_limit = _safe_int(getattr(subscription, "traffic_limit_gb", 0) or 0)
            if current_limit != 0:
                subscription.traffic_limit_gb = current_limit + traffic_gb
            current_purchased = _safe_int(getattr(subscription, "purchased_traffic_gb", 0) or 0)
            subscription.purchased_traffic_gb = current_purchased + traffic_gb

        await db.commit()
        await db.refresh(subscription)
    else:
        subscription = await subscription_crud.create_paid_subscription(
            db=db,
            user_id=user.id,
            duration_days=_safe_int(cart_data.get("period_days"), 30) or 30,
            traffic_limit_gb=traffic_gb,
            device_limit=0,
            connected_squads=list(countries),
            update_server_counters=False,
            tariff_code=TariffCode.WHITE.value,
        )
        subscription.end_date = _get_white_unlimited_end_date()
        subscription.purchased_traffic_gb = 0 if traffic_gb == 0 else traffic_gb
        await db.commit()
        await db.refresh(subscription)

    server_prices = cart_data.get("server_prices_for_period") or []
    server_ids = await get_server_ids_by_uuids(db, list(countries))
    if server_ids:
        from app.database.crud.subscription import add_subscription_servers

        if not isinstance(server_prices, list) or len(server_prices) != len(server_ids):
            server_prices = [0] * len(server_ids)
        await add_subscription_servers(db, subscription, server_ids, server_prices)
        await add_user_to_servers(db, server_ids)

    subscription_service = SubscriptionService()
    remnawave_user = None
    if getattr(subscription, "remnawave_uuid", None):
        remnawave_user = await subscription_service.update_remnawave_user(
            db,
            subscription,
            reset_traffic=settings.RESET_TRAFFIC_ON_PAYMENT,
            reset_reason="автопокупка White",
        )
    else:
        remnawave_user = await subscription_service.create_remnawave_user(
            db,
            subscription,
            reset_traffic=settings.RESET_TRAFFIC_ON_PAYMENT,
            reset_reason="автопокупка White",
        )
    if not remnawave_user:
        logger.error("❌ Автопокупка White: не удалось создать/обновить RemnaWave пользователя (user=%s)", user.telegram_id)
        try:
            await subscription_service.create_remnawave_user(
                db,
                subscription,
                reset_traffic=False,
                reset_reason="автопокупка White (повтор)",
            )
        except Exception as error:
            logger.error("❌ Автопокупка White: повторная попытка RemnaWave тоже упала: %s", error)

    transaction = await create_transaction(
        db=db,
        user_id=user.id,
        type=TransactionType.SUBSCRIPTION_PAYMENT,
        amount_kopeks=final_price,
        description="Подписка White",
        payment_method=PaymentMethod.BALANCE,
    )

    await user_cart_service.delete_user_cart(user.id)
    await clear_subscription_checkout_draft(user.id)

    if bot:
        try:
            texts = get_texts(getattr(user, "language", "ru"))

            message = (
                "✅ Трафик White автоматически приобретён после пополнения баланса.\n\n"
                "🎉 Подписка успешно обновлена!\n\n"
                "Перейдите в раздел «Моя подписка», чтобы получить ссылку и инструкции."
            )

            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text=texts.t("MY_SUBSCRIPTION_BUTTON", "📱 Моя подписка"),
                            callback_data="menu_subscription",
                        )
                    ]
                ]
            )

            await bot.send_message(
                chat_id=user.telegram_id,
                text=message,
                reply_markup=keyboard,
            )

            notification_service = AdminNotificationService(bot)
            await notification_service.send_subscription_purchase_notification(
                db,
                user,
                subscription,
                transaction,
                0,
                False,
            )
        except Exception as error:
            logger.warning("⚠️ Автопокупка White: не удалось отправить уведомление админам: %s", error)

    logger.info(
        "✅ Автопокупка White: выполнено user=%s subscription_id=%s traffic=%sGB price=%s",
        user.telegram_id,
        getattr(subscription, "id", None),
        traffic_gb,
        final_price,
    )
    return True


async def auto_purchase_saved_cart_after_topup(
    db: AsyncSession,
    user: User,
    *,
    bot: Optional[Bot] = None,
) -> bool:
    if _ORIGINAL_AUTO_PURCHASE_SAVED_CART_AFTER_TOPUP is None:
        raise RuntimeError("Spiderman auto purchase patch was not initialized")

    if not settings.SPIDERMAN_MODE or not settings.MULTI_TARIFF_ENABLED:
        return await _ORIGINAL_AUTO_PURCHASE_SAVED_CART_AFTER_TOPUP(db, user, bot=bot)

    if not settings.is_auto_purchase_after_topup_enabled():
        return await _ORIGINAL_AUTO_PURCHASE_SAVED_CART_AFTER_TOPUP(db, user, bot=bot)

    cart_data = await user_cart_service.get_user_cart(user.id)
    if not cart_data:
        return await _ORIGINAL_AUTO_PURCHASE_SAVED_CART_AFTER_TOPUP(db, user, bot=bot)

    tariff_code = normalize_tariff_code(cart_data.get("tariff_code"))
    if tariff_code != TariffCode.WHITE.value:
        return await _ORIGINAL_AUTO_PURCHASE_SAVED_CART_AFTER_TOPUP(db, user, bot=bot)

    return await _auto_purchase_white_cart_after_topup(db, user, cart_data, bot=bot)


async def auto_activate_subscription_after_topup(
    db: AsyncSession,
    user: User,
    *,
    bot: Optional[Bot] = None,
) -> bool:
    if _ORIGINAL_AUTO_ACTIVATE_SUBSCRIPTION_AFTER_TOPUP is None:
        raise RuntimeError("Spiderman auto activate patch was not initialized")

    if not settings.SPIDERMAN_MODE or not settings.MULTI_TARIFF_ENABLED:
        return await _ORIGINAL_AUTO_ACTIVATE_SUBSCRIPTION_AFTER_TOPUP(db, user, bot=bot)

    cart_data = await user_cart_service.get_user_cart(user.id)
    if cart_data:
        tariff_code = normalize_tariff_code(cart_data.get("tariff_code"))
        logger.info(
            "🛑 Автоактивация: пропускаем fallback, потому что сохранена корзина (tariff=%s, user=%s)",
            tariff_code,
            user.telegram_id,
        )
        return False

    try:
        from datetime import datetime as _dt
        from app.database.crud import subscription as subscription_crud

        now = _dt.utcnow()
        for tariff_code in (TariffCode.STANDARD.value, TariffCode.WHITE.value):
            subscription = await subscription_crud.get_subscription_by_user_id(
                db,
                user.id,
                tariff_code=tariff_code,
            )
            if not subscription or not getattr(subscription, "end_date", None):
                continue
            status = str(getattr(subscription, "status", "") or "").strip().lower()
            if status == SubscriptionStatus.ACTIVE.value and subscription.end_date > now:
                logger.info(
                    "🛑 Автоактивация: у пользователя %s уже есть активная подписка (tariff=%s, id=%s), пропускаем",
                    user.telegram_id,
                    tariff_code,
                    getattr(subscription, "id", None),
                )
                return False
    except Exception as error:  # pragma: no cover - defensive logging
        logger.warning("⚠️ Автоактивация: не удалось проверить активные подписки перед fallback: %s", error)

    return await _ORIGINAL_AUTO_ACTIVATE_SUBSCRIPTION_AFTER_TOPUP(db, user, bot=bot)


def apply_subscription_auto_purchase_patches() -> None:
    import app.services.subscription_auto_purchase_service as auto_purchase_service

    if getattr(auto_purchase_service, "_spiderman_auto_purchase_patched", False):
        return

    global _ORIGINAL_AUTO_PURCHASE_SAVED_CART_AFTER_TOPUP
    global _ORIGINAL_AUTO_ACTIVATE_SUBSCRIPTION_AFTER_TOPUP

    _ORIGINAL_AUTO_PURCHASE_SAVED_CART_AFTER_TOPUP = auto_purchase_service.auto_purchase_saved_cart_after_topup
    _ORIGINAL_AUTO_ACTIVATE_SUBSCRIPTION_AFTER_TOPUP = auto_purchase_service.auto_activate_subscription_after_topup

    auto_purchase_service.auto_purchase_saved_cart_after_topup = auto_purchase_saved_cart_after_topup
    auto_purchase_service.auto_activate_subscription_after_topup = auto_activate_subscription_after_topup

    auto_purchase_service._spiderman_auto_purchase_patched = True

    try:
        import sys

        for module_name in (
            "app.services.payment.cloudpayments",
            "app.services.payment.cryptobot",
            "app.services.payment.freekassa",
            "app.services.payment.mulenpay",
            "app.services.payment.pal24",
            "app.services.payment.platega",
            "app.services.payment.stars",
            "app.services.payment.wata",
            "app.services.payment.yookassa",
            "app.services.tribute_service",
        ):
            module = sys.modules.get(module_name)
            if not module:
                continue

            if hasattr(module, "auto_purchase_saved_cart_after_topup"):
                setattr(module, "auto_purchase_saved_cart_after_topup", auto_purchase_saved_cart_after_topup)
            if hasattr(module, "auto_activate_subscription_after_topup"):
                setattr(module, "auto_activate_subscription_after_topup", auto_activate_subscription_after_topup)

        logger.info("🕷️ SpiderMan: патчи автопокупки (White) подключены")
    except Exception as error:  # pragma: no cover - defensive logging
        logger.warning("⚠️ SpiderMan: не удалось пропатчить импорты payment-модулей: %s", error)
