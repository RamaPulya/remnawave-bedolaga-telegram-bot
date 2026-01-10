import logging
from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.database.crud.subscription import check_and_update_subscription_status
from app.database.models import Subscription, SubscriptionStatus
from app.spiderman.tariff_context import normalize_tariff_code

logger = logging.getLogger(__name__)


async def get_subscription_by_user_id(
    db: AsyncSession,
    user_id: int,
    *,
    tariff_code: Optional[str] = None,
) -> Optional[Subscription]:
    tariff_code = normalize_tariff_code(tariff_code)
    result = await db.execute(
        select(Subscription)
        .options(selectinload(Subscription.user))
        .where(
            and_(
                Subscription.user_id == user_id,
                Subscription.tariff_code == tariff_code,
            )
        )
        .order_by(Subscription.created_at.desc())
        .limit(1)
    )
    subscription = result.scalar_one_or_none()

    if subscription:
        logger.info(
            "🔍 Загружена подписка %s для пользователя %s (тариф=%s, статус=%s)",
            subscription.id,
            user_id,
            tariff_code,
            subscription.status,
        )
        subscription = await check_and_update_subscription_status(db, subscription)

    return subscription


async def get_subscriptions_for_user(
    db: AsyncSession,
    user_id: int,
    *,
    tariff_code: Optional[str] = None,
) -> List[Subscription]:
    query = (
        select(Subscription)
        .options(selectinload(Subscription.user))
        .where(Subscription.user_id == user_id)
        .order_by(Subscription.created_at.desc())
    )
    if tariff_code:
        query = query.where(Subscription.tariff_code == normalize_tariff_code(tariff_code))

    result = await db.execute(query)
    return result.scalars().all()


async def get_subscription_by_remnawave_uuid(
    db: AsyncSession,
    remnawave_uuid: str,
) -> Optional[Subscription]:
    if not remnawave_uuid:
        return None
    result = await db.execute(
        select(Subscription).where(Subscription.remnawave_uuid == remnawave_uuid)
    )
    return result.scalar_one_or_none()


async def create_trial_subscription(
    db: AsyncSession,
    user_id: int,
    duration_days: int = None,
    traffic_limit_gb: int = None,
    device_limit: Optional[int] = None,
    squad_uuid: str = None,
    *,
    tariff_code: Optional[str] = None,
) -> Subscription:
    tariff_code = normalize_tariff_code(tariff_code)
    duration_days = duration_days or settings.TRIAL_DURATION_DAYS
    traffic_limit_gb = traffic_limit_gb or settings.TRIAL_TRAFFIC_LIMIT_GB
    if device_limit is None:
        device_limit = settings.TRIAL_DEVICE_LIMIT

    if not squad_uuid:
        try:
            from app.database.crud.server_squad import get_random_trial_squad_uuid

            squad_uuid = await get_random_trial_squad_uuid(db)

            if squad_uuid:
                logger.debug(
                    "🔍 Выбран сквад %s для триала пользователя %s",
                    squad_uuid,
                    user_id,
                )
        except Exception as error:
            logger.error(
                "❌ Не удалось получить сквад для триала пользователя %s: %s",
                user_id,
                error,
            )

    end_date = datetime.utcnow() + timedelta(days=duration_days)

    subscription = Subscription(
        user_id=user_id,
        tariff_code=tariff_code,
        status=SubscriptionStatus.ACTIVE.value,
        is_trial=True,
        start_date=datetime.utcnow(),
        end_date=end_date,
        traffic_limit_gb=traffic_limit_gb,
        device_limit=device_limit,
        connected_squads=[squad_uuid] if squad_uuid else [],
        autopay_enabled=settings.is_autopay_enabled_by_default(),
        autopay_days_before=settings.DEFAULT_AUTOPAY_DAYS_BEFORE,
    )

    db.add(subscription)
    await db.commit()
    await db.refresh(subscription)

    logger.info(
        "🎁 Создана триальная подписка для пользователя %s (тариф=%s, id=%s)",
        user_id,
        tariff_code,
        subscription.id,
    )

    if squad_uuid:
        try:
            from app.database.crud.server_squad import (
                get_server_ids_by_uuids,
                add_user_to_servers,
            )

            server_ids = await get_server_ids_by_uuids(db, [squad_uuid])
            if server_ids:
                await add_user_to_servers(db, server_ids)
                logger.info(
                    "✅ Обновлен счетчик пользователей для trial сквада %s",
                    squad_uuid,
                )
            else:
                logger.warning(
                    "⚠️ Не найдены серверы для обновления счетчика (сквад %s)",
                    squad_uuid,
                )
        except Exception as error:
            logger.error(
                "❌ Не удалось обновить счетчик trial сквада %s: %s",
                squad_uuid,
                error,
            )

    return subscription


async def create_paid_subscription(
    db: AsyncSession,
    user_id: int,
    duration_days: int,
    traffic_limit_gb: int = 0,
    device_limit: Optional[int] = None,
    connected_squads: List[str] = None,
    update_server_counters: bool = False,
    *,
    tariff_code: Optional[str] = None,
) -> Subscription:
    tariff_code = normalize_tariff_code(tariff_code)
    end_date = datetime.utcnow() + timedelta(days=duration_days)

    if device_limit is None:
        device_limit = settings.DEFAULT_DEVICE_LIMIT

    subscription = Subscription(
        user_id=user_id,
        tariff_code=tariff_code,
        status=SubscriptionStatus.ACTIVE.value,
        is_trial=False,
        start_date=datetime.utcnow(),
        end_date=end_date,
        traffic_limit_gb=traffic_limit_gb,
        device_limit=device_limit,
        connected_squads=connected_squads or [],
        autopay_enabled=settings.is_autopay_enabled_by_default(),
        autopay_days_before=settings.DEFAULT_AUTOPAY_DAYS_BEFORE,
    )

    db.add(subscription)
    await db.commit()
    await db.refresh(subscription)

    logger.info(
        "💎 Создана платная подписка для пользователя %s (тариф=%s, id=%s, статус=%s)",
        user_id,
        tariff_code,
        subscription.id,
        subscription.status,
    )

    squad_uuids = list(connected_squads or [])
    if update_server_counters and squad_uuids:
        try:
            from app.database.crud.server_squad import (
                get_server_ids_by_uuids,
                add_user_to_servers,
            )

            server_ids = await get_server_ids_by_uuids(db, squad_uuids)
            if server_ids:
                await add_user_to_servers(db, server_ids)
                logger.info(
                    "✅ Обновлен счетчик пользователей для платной подписки пользователя %s (сквады: %s)",
                    user_id,
                    squad_uuids,
                )
            else:
                logger.warning(
                    "⚠️ Не найдены серверы для платной подписки пользователя %s (сквады: %s)",
                    user_id,
                    squad_uuids,
                )
        except Exception as error:
            logger.error(
                "❌ Не удалось обновить счетчики платной подписки пользователя %s: %s",
                user_id,
                error,
            )

    return subscription


async def create_subscription_no_commit(
    db: AsyncSession,
    user_id: int,
    status: str = "trial",
    is_trial: bool = True,
    end_date: datetime = None,
    traffic_limit_gb: int = 10,
    traffic_used_gb: float = 0.0,
    device_limit: int = 1,
    connected_squads: list = None,
    remnawave_uuid: Optional[str] = None,
    remnawave_short_uuid: str = None,
    subscription_url: str = "",
    subscription_crypto_link: str = "",
    autopay_enabled: Optional[bool] = None,
    autopay_days_before: Optional[int] = None,
    *,
    tariff_code: Optional[str] = None,
) -> Subscription:
    if end_date is None:
        end_date = datetime.utcnow() + timedelta(days=3)

    if connected_squads is None:
        connected_squads = []

    subscription = Subscription(
        user_id=user_id,
        tariff_code=normalize_tariff_code(tariff_code),
        status=status,
        is_trial=is_trial,
        end_date=end_date,
        traffic_limit_gb=traffic_limit_gb,
        traffic_used_gb=traffic_used_gb,
        device_limit=device_limit,
        connected_squads=connected_squads,
        remnawave_uuid=remnawave_uuid,
        remnawave_short_uuid=remnawave_short_uuid,
        subscription_url=subscription_url,
        subscription_crypto_link=subscription_crypto_link,
        autopay_enabled=(
            settings.is_autopay_enabled_by_default()
            if autopay_enabled is None
            else autopay_enabled
        ),
        autopay_days_before=(
            settings.DEFAULT_AUTOPAY_DAYS_BEFORE
            if autopay_days_before is None
            else autopay_days_before
        ),
    )

    db.add(subscription)

    logger.info(
        "Prepared subscription for user %s (tariff=%s, pending commit)",
        user_id,
        subscription.tariff_code,
    )
    return subscription


async def create_subscription(
    db: AsyncSession,
    user_id: int,
    status: str = "trial",
    is_trial: bool = True,
    end_date: datetime = None,
    traffic_limit_gb: int = 10,
    traffic_used_gb: float = 0.0,
    device_limit: int = 1,
    connected_squads: list = None,
    remnawave_uuid: Optional[str] = None,
    remnawave_short_uuid: str = None,
    subscription_url: str = "",
    subscription_crypto_link: str = "",
    autopay_enabled: Optional[bool] = None,
    autopay_days_before: Optional[int] = None,
    *,
    tariff_code: Optional[str] = None,
) -> Subscription:
    if end_date is None:
        end_date = datetime.utcnow() + timedelta(days=3)

    if connected_squads is None:
        connected_squads = []

    subscription = Subscription(
        user_id=user_id,
        tariff_code=normalize_tariff_code(tariff_code),
        status=status,
        is_trial=is_trial,
        end_date=end_date,
        traffic_limit_gb=traffic_limit_gb,
        traffic_used_gb=traffic_used_gb,
        device_limit=device_limit,
        connected_squads=connected_squads,
        remnawave_uuid=remnawave_uuid,
        remnawave_short_uuid=remnawave_short_uuid,
        subscription_url=subscription_url,
        subscription_crypto_link=subscription_crypto_link,
        autopay_enabled=(
            settings.is_autopay_enabled_by_default()
            if autopay_enabled is None
            else autopay_enabled
        ),
        autopay_days_before=(
            settings.DEFAULT_AUTOPAY_DAYS_BEFORE
            if autopay_days_before is None
            else autopay_days_before
        ),
    )

    db.add(subscription)
    await db.commit()
    await db.refresh(subscription)

    logger.info(
        "Created subscription for user %s (tariff=%s, id=%s)",
        user_id,
        subscription.tariff_code,
        subscription.id,
    )
    return subscription


async def create_pending_subscription(
    db: AsyncSession,
    user_id: int,
    duration_days: int,
    traffic_limit_gb: int = 0,
    device_limit: int = 1,
    connected_squads: List[str] = None,
    payment_method: str = "pending",
    total_price_kopeks: int = 0,
    *,
    tariff_code: Optional[str] = None,
) -> Subscription:
    current_time = datetime.utcnow()
    end_date = current_time + timedelta(days=duration_days)
    tariff_code = normalize_tariff_code(tariff_code)

    existing_subscription = await get_subscription_by_user_id(
        db,
        user_id,
        tariff_code=tariff_code,
    )

    if existing_subscription:
        if (
            existing_subscription.status == SubscriptionStatus.ACTIVE.value
            and existing_subscription.end_date > current_time
        ):
            logger.warning(
                "Pending subscription for active user %s ignored (tariff=%s).",
                user_id,
                tariff_code,
            )
            return existing_subscription

        existing_subscription.status = SubscriptionStatus.PENDING.value
        existing_subscription.is_trial = False
        existing_subscription.start_date = current_time
        existing_subscription.end_date = end_date
        existing_subscription.traffic_limit_gb = traffic_limit_gb
        existing_subscription.device_limit = device_limit
        existing_subscription.connected_squads = connected_squads or []
        existing_subscription.traffic_used_gb = 0.0
        existing_subscription.updated_at = current_time

        await db.commit()
        await db.refresh(existing_subscription)

        logger.info(
            "Updated pending subscription user %s (tariff=%s, id=%s, method=%s)",
            user_id,
            tariff_code,
            existing_subscription.id,
            payment_method,
        )
        return existing_subscription

    subscription = Subscription(
        user_id=user_id,
        tariff_code=tariff_code,
        status=SubscriptionStatus.PENDING.value,
        is_trial=False,
        start_date=current_time,
        end_date=end_date,
        traffic_limit_gb=traffic_limit_gb,
        device_limit=device_limit,
        connected_squads=connected_squads or [],
        autopay_enabled=settings.is_autopay_enabled_by_default(),
        autopay_days_before=settings.DEFAULT_AUTOPAY_DAYS_BEFORE,
    )

    db.add(subscription)
    await db.commit()
    await db.refresh(subscription)

    logger.info(
        "Created pending subscription user %s (tariff=%s, id=%s, method=%s)",
        user_id,
        tariff_code,
        subscription.id,
        payment_method,
    )

    return subscription


async def activate_pending_subscription(
    db: AsyncSession,
    user_id: int,
    period_days: int = None,
    *,
    tariff_code: Optional[str] = None,
) -> Optional[Subscription]:
    tariff_code = normalize_tariff_code(tariff_code)
    logger.info(
        "Activate pending subscription user %s (tariff=%s, period=%s days)",
        user_id,
        tariff_code,
        period_days,
    )

    result = await db.execute(
        select(Subscription).where(
            and_(
                Subscription.user_id == user_id,
                Subscription.tariff_code == tariff_code,
                Subscription.status == SubscriptionStatus.PENDING.value,
            )
        )
    )
    pending_subscription = result.scalar_one_or_none()

    if not pending_subscription:
        logger.warning(
            "No pending subscription for user %s (tariff=%s)",
            user_id,
            tariff_code,
        )
        return None

    current_time = datetime.utcnow()
    pending_subscription.status = SubscriptionStatus.ACTIVE.value

    if period_days is not None:
        effective_start = pending_subscription.start_date or current_time
        if effective_start < current_time:
            effective_start = current_time
        pending_subscription.end_date = effective_start + timedelta(days=period_days)

    if not pending_subscription.start_date or pending_subscription.start_date < current_time:
        pending_subscription.start_date = current_time

    await db.commit()
    await db.refresh(pending_subscription)

    logger.info(
        "Activated pending subscription user %s (tariff=%s, id=%s)",
        user_id,
        tariff_code,
        pending_subscription.id,
    )
    return pending_subscription


def apply_subscription_crud_patches() -> None:
    import app.database.crud.subscription as subscription_crud

    if getattr(subscription_crud, "_spiderman_patched", False):
        return

    subscription_crud.get_subscription_by_user_id = get_subscription_by_user_id
    subscription_crud.get_subscriptions_for_user = get_subscriptions_for_user
    subscription_crud.get_subscription_by_remnawave_uuid = get_subscription_by_remnawave_uuid
    subscription_crud.create_trial_subscription = create_trial_subscription
    subscription_crud.create_paid_subscription = create_paid_subscription
    subscription_crud.create_subscription_no_commit = create_subscription_no_commit
    subscription_crud.create_subscription = create_subscription
    subscription_crud.create_pending_subscription = create_pending_subscription
    subscription_crud.activate_pending_subscription = activate_pending_subscription
    subscription_crud._spiderman_patched = True
