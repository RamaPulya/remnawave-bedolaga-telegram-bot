import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database.models import Subscription
from app.spiderman.tariff_context import normalize_tariff_code

logger = logging.getLogger(__name__)


async def ensure_single_subscription(
    db: AsyncSession,
    user_id: int,
    *,
    tariff_code: Optional[str] = None,
) -> Optional[Subscription]:
    tariff_code = normalize_tariff_code(tariff_code)
    result = await db.execute(
        select(Subscription)
        .where(
            Subscription.user_id == user_id,
            Subscription.tariff_code == tariff_code,
        )
        .order_by(Subscription.created_at.desc())
    )
    subscriptions = result.scalars().all()

    if len(subscriptions) <= 1:
        return subscriptions[0] if subscriptions else None

    latest_subscription = subscriptions[0]
    old_subscriptions = subscriptions[1:]

    logger.warning(
        "Detected %s subscriptions for user %s (tariff=%s). Deleting %s old.",
        len(subscriptions),
        user_id,
        tariff_code,
        len(old_subscriptions),
    )

    for old_sub in old_subscriptions:
        await db.delete(old_sub)
        logger.info(
            "Deleted subscription id=%s (tariff=%s, created_at=%s)",
            old_sub.id,
            tariff_code,
            old_sub.created_at,
        )

    await db.commit()
    await db.refresh(latest_subscription)

    logger.info(
        "Kept subscription id=%s (tariff=%s, created_at=%s)",
        latest_subscription.id,
        tariff_code,
        latest_subscription.created_at,
    )
    return latest_subscription


async def update_or_create_subscription(
    db: AsyncSession,
    user_id: int,
    **subscription_data,
) -> Subscription:
    tariff_code = normalize_tariff_code(
        subscription_data.pop("tariff_code", None)
    )
    existing_subscription = await ensure_single_subscription(
        db,
        user_id,
        tariff_code=tariff_code,
    )

    if existing_subscription:
        for key, value in subscription_data.items():
            if key == "tariff_code":
                continue
            if hasattr(existing_subscription, key):
                setattr(existing_subscription, key, value)

        existing_subscription.updated_at = datetime.utcnow()
        await db.commit()
        await db.refresh(existing_subscription)

        logger.info(
            "Updated subscription id=%s (tariff=%s)",
            existing_subscription.id,
            tariff_code,
        )
        return existing_subscription

    subscription_defaults = dict(subscription_data)
    autopay_enabled = subscription_defaults.pop("autopay_enabled", None)
    autopay_days_before = subscription_defaults.pop("autopay_days_before", None)

    new_subscription = Subscription(
        user_id=user_id,
        tariff_code=tariff_code,
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
        **subscription_defaults,
    )

    db.add(new_subscription)
    await db.commit()
    await db.refresh(new_subscription)

    logger.info(
        "Created subscription id=%s (tariff=%s)",
        new_subscription.id,
        tariff_code,
    )
    return new_subscription


async def cleanup_duplicate_subscriptions(db: AsyncSession) -> int:
    result = await db.execute(
        select(Subscription.user_id, Subscription.tariff_code)
        .group_by(Subscription.user_id, Subscription.tariff_code)
        .having(func.count(Subscription.id) > 1)
    )
    user_tariffs = result.all()

    total_deleted = 0

    for user_id, tariff_code in user_tariffs:
        subscriptions_result = await db.execute(
            select(Subscription)
            .where(
                Subscription.user_id == user_id,
                Subscription.tariff_code == tariff_code,
            )
            .order_by(Subscription.created_at.desc())
        )
        subscriptions = subscriptions_result.scalars().all()

        for old_subscription in subscriptions[1:]:
            await db.delete(old_subscription)
            total_deleted += 1
            logger.info(
                "Deleted duplicate subscription id=%s user=%s tariff=%s",
                old_subscription.id,
                user_id,
                tariff_code,
            )

    await db.commit()
    logger.info("Cleaned %s duplicate subscriptions", total_deleted)

    return total_deleted


def apply_subscription_utils_patches() -> None:
    import app.utils.subscription_utils as subscription_utils

    if getattr(subscription_utils, "_spiderman_patched", False):
        return

    subscription_utils.ensure_single_subscription = ensure_single_subscription
    subscription_utils.update_or_create_subscription = update_or_create_subscription
    subscription_utils.cleanup_duplicate_subscriptions = cleanup_duplicate_subscriptions
    subscription_utils._spiderman_patched = True
