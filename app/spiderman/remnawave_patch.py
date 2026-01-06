import asyncio
import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database.crud.user import get_user_by_id
from app.database.models import Subscription, SubscriptionStatus, User
from app.external.remnawave_api import (
    RemnaWaveAPIError,
    RemnaWaveUser,
    UserStatus,
    TrafficLimitStrategy,
)
from app.services.subscription_service import get_traffic_reset_strategy
from app.spiderman.tariff_context import TariffCode, normalize_tariff_code
from app.utils.subscription_utils import resolve_hwid_device_limit_for_payload

logger = logging.getLogger(__name__)


def _normalize_tag(value: Optional[str], setting_name: str) -> Optional[str]:
    if value is None:
        return None
    try:
        return settings._normalize_user_tag(value, setting_name)
    except Exception:
        cleaned = str(value).strip().upper()
        return cleaned or None


def _sanitize_username(value: str, *, preserve_leading_underscore: bool = False) -> str:
    sanitized = re.sub(r"[^0-9A-Za-z._-]+", "_", value)
    sanitized = re.sub(r"_+", "_", sanitized)
    if preserve_leading_underscore:
        return sanitized.strip(".-")
    return sanitized.strip("._-")


def _get_white_suffix() -> str:
    raw_suffix = (settings.WHITE_TARIFF_SUFFIX or "_w").strip()
    if not raw_suffix:
        raw_suffix = "_w"

    sanitized = _sanitize_username(raw_suffix, preserve_leading_underscore=True)
    if not sanitized:
        return "_w"

    if not sanitized.startswith("_"):
        sanitized = "_" + sanitized.lstrip("._-")

    if sanitized == "_":
        return "_w"

    return sanitized


def _build_remnawave_username(user: User, tariff_code: str) -> str:
    base_username = settings.format_remnawave_username(
        full_name=user.full_name,
        username=user.username,
        telegram_id=user.telegram_id,
    )

    if tariff_code != TariffCode.WHITE.value:
        return base_username

    suffix = _get_white_suffix()

    if base_username.lower().endswith(suffix.lower()):
        candidate = base_username
    else:
        candidate = f"{base_username}{suffix}"

    candidate = _sanitize_username(candidate)
    if not candidate:
        candidate = base_username

    return candidate[:64]


def _resolve_user_tag(subscription: Subscription, tariff_code: str) -> Optional[str]:
    if getattr(subscription, "is_trial", False):
        return settings.get_trial_user_tag()

    if tariff_code == TariffCode.WHITE.value:
        tag = _normalize_tag(settings.WHITE_TARIFF_TAG, "WHITE_TARIFF_TAG")
        if tag:
            return tag
    else:
        tag = _normalize_tag(settings.STANDARD_TARIFF_TAG, "STANDARD_TARIFF_TAG")
        if tag:
            return tag

    return settings.get_paid_subscription_user_tag()


def _resolve_subscription_uuid(
    subscription: Subscription,
    user: Optional[User],
    tariff_code: str,
) -> Optional[str]:
    uuid_value = getattr(subscription, "remnawave_uuid", None)
    if uuid_value:
        return uuid_value

    if tariff_code == TariffCode.STANDARD.value and user is not None:
        return getattr(user, "remnawave_uuid", None)

    return None


def _apply_remnawave_identity(
    subscription: Subscription,
    user: Optional[User],
    remnawave_user: RemnaWaveUser,
    tariff_code: str,
) -> None:
    subscription.remnawave_uuid = remnawave_user.uuid
    subscription.remnawave_short_uuid = remnawave_user.short_uuid
    subscription.subscription_url = remnawave_user.subscription_url
    subscription.subscription_crypto_link = remnawave_user.happ_crypto_link

    if tariff_code == TariffCode.STANDARD.value and user is not None:
        user.remnawave_uuid = remnawave_user.uuid


def _resolve_hwid_limit(subscription: Subscription, tariff_code: str) -> Optional[int]:
    if tariff_code == TariffCode.WHITE.value:
        return 0
    return resolve_hwid_device_limit_for_payload(subscription)


def _resolve_traffic_limit_strategy(tariff_code: str) -> TrafficLimitStrategy:
    if tariff_code == TariffCode.WHITE.value:
        return TrafficLimitStrategy.NO_RESET
    return get_traffic_reset_strategy()


def _pick_panel_user_by_username(
    candidates: List[RemnaWaveUser],
    username: str,
    tariff_code: str,
) -> Optional[RemnaWaveUser]:
    if not candidates:
        return None

    for candidate in candidates:
        if candidate.username == username:
            return candidate

    if tariff_code == TariffCode.WHITE.value:
        white_tag = _normalize_tag(settings.WHITE_TARIFF_TAG, "WHITE_TARIFF_TAG")
        if white_tag:
            for candidate in candidates:
                if (candidate.tag or "").strip().upper() == white_tag:
                    return candidate

        suffix = _get_white_suffix().lower()
        for candidate in candidates:
            if candidate.username and candidate.username.lower().endswith(suffix):
                return candidate
        return None

    standard_tag = _normalize_tag(settings.STANDARD_TARIFF_TAG, "STANDARD_TARIFF_TAG")
    if standard_tag:
        for candidate in candidates:
            if (candidate.tag or "").strip().upper() == standard_tag:
                return candidate

    if len(candidates) == 1:
        candidate = candidates[0]
        candidate_tag = (candidate.tag or "").strip().upper()
        white_tag = _normalize_tag(settings.WHITE_TARIFF_TAG, "WHITE_TARIFF_TAG")
        suffix = _get_white_suffix().lower()
        if white_tag and candidate_tag == white_tag:
            return None
        if candidate.username and suffix and candidate.username.lower().endswith(suffix):
            return None
        return candidate

    return None


def _detect_tariff_from_panel_user(panel_user: Dict[str, Any]) -> str:
    tag = panel_user.get("tag")
    if tag:
        tag_value = str(tag).strip().upper()
        white_tag = _normalize_tag(settings.WHITE_TARIFF_TAG, "WHITE_TARIFF_TAG")
        standard_tag = _normalize_tag(settings.STANDARD_TARIFF_TAG, "STANDARD_TARIFF_TAG")
        if white_tag and tag_value == white_tag:
            return TariffCode.WHITE.value
        if standard_tag and tag_value == standard_tag:
            return TariffCode.STANDARD.value

    username = (panel_user.get("username") or "").lower()
    suffix = _get_white_suffix().lower()
    if suffix and username.endswith(suffix):
        return TariffCode.WHITE.value

    return TariffCode.STANDARD.value


async def create_remnawave_user(
    self,
    db: AsyncSession,
    subscription: Subscription,
    *,
    reset_traffic: bool = False,
    reset_reason: Optional[str] = None,
) -> Optional[RemnaWaveUser]:
    try:
        user = await get_user_by_id(db, subscription.user_id)
        if not user:
            logger.error("❌ Пользователь %s не найден", subscription.user_id)
            return None

        validation_success = await self.validate_and_clean_subscription(db, subscription, user)
        if not validation_success:
            logger.error("❌ Валидация подписки не прошла для пользователя %s", user.telegram_id)
            return None

        tariff_code = normalize_tariff_code(getattr(subscription, "tariff_code", None))
        user_tag = _resolve_user_tag(subscription, tariff_code)
        username = _build_remnawave_username(user, tariff_code)

        async with self.get_api_client() as api:
            hwid_limit = _resolve_hwid_limit(subscription, tariff_code)

            remnawave_user = None
            remnawave_uuid = _resolve_subscription_uuid(subscription, user, tariff_code)
            if remnawave_uuid:
                remnawave_user = await api.get_user_by_uuid(remnawave_uuid)
                if not remnawave_user:
                    logger.warning(
                        "⚠️ RemnaWave пользователь %s не найден для подписки %s",
                        remnawave_uuid,
                        subscription.id,
                    )

            if not remnawave_user:
                remnawave_user = await api.get_user_by_username(username)

            if not remnawave_user:
                existing_users = await api.get_user_by_telegram_id(user.telegram_id)
                remnawave_user = _pick_panel_user_by_username(
                    existing_users,
                    username,
                    tariff_code,
                )

            if remnawave_user:
                update_kwargs = dict(
                    uuid=remnawave_user.uuid,
                    status=UserStatus.ACTIVE,
                    expire_at=subscription.end_date,
                    traffic_limit_bytes=self._gb_to_bytes(subscription.traffic_limit_gb),
                    traffic_limit_strategy=_resolve_traffic_limit_strategy(tariff_code),
                    description=settings.format_remnawave_user_description(
                        full_name=user.full_name,
                        username=user.username,
                        telegram_id=user.telegram_id,
                    ),
                    active_internal_squads=subscription.connected_squads,
                )

                if user_tag is not None:
                    update_kwargs["tag"] = user_tag

                if hwid_limit is not None:
                    update_kwargs["hwid_device_limit"] = hwid_limit

                updated_user = await api.update_user(**update_kwargs)

                if reset_traffic:
                    await self._reset_user_traffic(
                        api,
                        updated_user.uuid,
                        user.telegram_id,
                        reset_reason,
                    )

            else:
                create_kwargs = dict(
                    username=username,
                    expire_at=subscription.end_date,
                    status=UserStatus.ACTIVE,
                    traffic_limit_bytes=self._gb_to_bytes(subscription.traffic_limit_gb),
                    traffic_limit_strategy=_resolve_traffic_limit_strategy(tariff_code),
                    telegram_id=user.telegram_id,
                    description=settings.format_remnawave_user_description(
                        full_name=user.full_name,
                        username=user.username,
                        telegram_id=user.telegram_id,
                    ),
                    active_internal_squads=subscription.connected_squads,
                )

                if user_tag is not None:
                    create_kwargs["tag"] = user_tag

                if hwid_limit is not None:
                    create_kwargs["hwid_device_limit"] = hwid_limit

                updated_user = await api.create_user(**create_kwargs)

                if reset_traffic:
                    await self._reset_user_traffic(
                        api,
                        updated_user.uuid,
                        user.telegram_id,
                        reset_reason,
                    )

            _apply_remnawave_identity(subscription, user, updated_user, tariff_code)
            await db.commit()

            logger.info(
                "✅ RemnaWave пользователь синхронизирован для подписки %s (тариф=%s, uuid=%s)",
                subscription.id,
                tariff_code,
                updated_user.uuid,
            )
            return updated_user

    except RemnaWaveAPIError as error:
        logger.error("❌ Ошибка RemnaWave API: %s", error)
        return None
    except Exception as error:
        logger.error("❌ Ошибка синхронизации RemnaWave пользователя: %s", error)
        return None


async def update_remnawave_user(
    self,
    db: AsyncSession,
    subscription: Subscription,
    *,
    reset_traffic: bool = False,
    reset_reason: Optional[str] = None,
) -> Optional[RemnaWaveUser]:
    try:
        user = await get_user_by_id(db, subscription.user_id)
        if not user:
            logger.error("❌ Пользователь %s не найден", subscription.user_id)
            return None

        tariff_code = normalize_tariff_code(getattr(subscription, "tariff_code", None))
        remnawave_uuid = _resolve_subscription_uuid(subscription, user, tariff_code)

        async with self.get_api_client() as api:
            if not remnawave_uuid:
                username = _build_remnawave_username(user, tariff_code)
                remnawave_user = await api.get_user_by_username(username)
                if not remnawave_user:
                    existing_users = await api.get_user_by_telegram_id(user.telegram_id)
                    remnawave_user = _pick_panel_user_by_username(
                        existing_users,
                        username,
                        tariff_code,
                    )
                if remnawave_user:
                    remnawave_uuid = remnawave_user.uuid
                    subscription.remnawave_uuid = remnawave_uuid
                    if tariff_code == TariffCode.STANDARD.value:
                        user.remnawave_uuid = remnawave_uuid
                else:
                    logger.error(
                        "❌ RemnaWave UUID не найден для подписки %s (тариф=%s)",
                        subscription.id,
                        tariff_code,
                    )
                    return None

            current_time = datetime.utcnow()
            is_actually_active = (
                subscription.status == SubscriptionStatus.ACTIVE.value
                and subscription.end_date > current_time
            )

            if (
                subscription.status == SubscriptionStatus.ACTIVE.value
                and subscription.end_date <= current_time
            ):
                subscription.status = SubscriptionStatus.EXPIRED.value
                subscription.updated_at = current_time
                await db.commit()
                is_actually_active = False
                logger.info(
                    "🔄 Подписка %s автоматически переведена в истекшую",
                    subscription.id,
                )

            user_tag = _resolve_user_tag(subscription, tariff_code)
            hwid_limit = _resolve_hwid_limit(subscription, tariff_code)

            update_kwargs = dict(
                uuid=remnawave_uuid,
                status=UserStatus.ACTIVE if is_actually_active else UserStatus.EXPIRED,
                expire_at=subscription.end_date,
                traffic_limit_bytes=self._gb_to_bytes(subscription.traffic_limit_gb),
                traffic_limit_strategy=_resolve_traffic_limit_strategy(tariff_code),
                description=settings.format_remnawave_user_description(
                    full_name=user.full_name,
                    username=user.username,
                    telegram_id=user.telegram_id,
                ),
                active_internal_squads=subscription.connected_squads,
            )

            if user_tag is not None:
                update_kwargs["tag"] = user_tag

            if hwid_limit is not None:
                update_kwargs["hwid_device_limit"] = hwid_limit

            updated_user = await api.update_user(**update_kwargs)

            if reset_traffic:
                await self._reset_user_traffic(
                    api,
                    remnawave_uuid,
                    user.telegram_id,
                    reset_reason,
                )

            subscription.subscription_url = updated_user.subscription_url
            subscription.subscription_crypto_link = updated_user.happ_crypto_link
            await db.commit()

            logger.info(
                "✅ RemnaWave пользователь обновлен для подписки %s (тариф=%s, uuid=%s)",
                subscription.id,
                tariff_code,
                remnawave_uuid,
            )
            return updated_user

    except RemnaWaveAPIError as error:
        logger.error("❌ Ошибка RemnaWave API: %s", error)
        return None
    except Exception as error:
        logger.error("❌ Ошибка синхронизации RemnaWave пользователя: %s", error)
        return None


async def revoke_subscription(
    self,
    db: AsyncSession,
    subscription: Subscription,
) -> Optional[str]:
    try:
        user = await get_user_by_id(db, subscription.user_id)
        tariff_code = normalize_tariff_code(getattr(subscription, "tariff_code", None))
        remnawave_uuid = _resolve_subscription_uuid(subscription, user, tariff_code)

        if not remnawave_uuid:
            return None

        async with self.get_api_client() as api:
            updated_user = await api.revoke_user_subscription(remnawave_uuid)

            subscription.remnawave_short_uuid = updated_user.short_uuid
            subscription.subscription_url = updated_user.subscription_url
            subscription.subscription_crypto_link = updated_user.happ_crypto_link
            await db.commit()

            logger.info(
                "🔗 Ссылка подписки обновлена для пользователя %s (тариф=%s)",
                user.telegram_id if user else subscription.user_id,
                tariff_code,
            )
            return updated_user.subscription_url

    except Exception as error:
        logger.error("❌ Ошибка обновления ссылки подписки: %s", error)
        return None


async def sync_subscription_usage(
    self,
    db: AsyncSession,
    subscription: Subscription,
) -> bool:
    try:
        user = await get_user_by_id(db, subscription.user_id)
        tariff_code = normalize_tariff_code(getattr(subscription, "tariff_code", None))
        remnawave_uuid = _resolve_subscription_uuid(subscription, user, tariff_code)
        if not remnawave_uuid:
            return False

        async with self.get_api_client() as api:
            remnawave_user = await api.get_user_by_uuid(remnawave_uuid)
            if not remnawave_user:
                return False

            used_gb = self._bytes_to_gb(remnawave_user.used_traffic_bytes)
            subscription.traffic_used_gb = used_gb
            await db.commit()

            logger.debug(
                "📊 Синхронизирован трафик для подписки %s (тариф=%s): %.2f ГБ",
                subscription.id,
                tariff_code,
                used_gb,
            )
            return True

    except Exception as error:
        logger.error("❌ Ошибка синхронизации трафика: %s", error)
        return False


async def validate_and_clean_subscription(
    self,
    db: AsyncSession,
    subscription: Subscription,
    user: User,
) -> bool:
    try:
        tariff_code = normalize_tariff_code(getattr(subscription, "tariff_code", None))
        remnawave_uuid = _resolve_subscription_uuid(subscription, user, tariff_code)
        needs_cleanup = False
        updated_identity = False

        if tariff_code == TariffCode.STANDARD.value:
            if user.remnawave_uuid and subscription.remnawave_uuid != user.remnawave_uuid:
                subscription.remnawave_uuid = user.remnawave_uuid
                remnawave_uuid = user.remnawave_uuid
                updated_identity = True

        if remnawave_uuid:
            try:
                async with self.get_api_client() as api:
                    remnawave_user = await api.get_user_by_uuid(remnawave_uuid)
                    if not remnawave_user:
                        logger.warning(
                            "⚠️ RemnaWave пользователь %s не найден для подписки %s",
                            remnawave_uuid,
                            subscription.id,
                        )
                        needs_cleanup = True
                    elif remnawave_user.telegram_id != user.telegram_id:
                        logger.warning(
                            "⚠️ Несовпадение Telegram ID: ожидается %s (подписка %s)",
                            user.telegram_id,
                            subscription.id,
                        )
                        needs_cleanup = True
            except Exception as api_error:
                logger.error("❌ Ошибка проверки RemnaWave: %s", api_error)
                needs_cleanup = True

        if subscription.remnawave_short_uuid and not remnawave_uuid:
            logger.warning(
                "⚠️ У подписки %s есть short UUID без remnawave UUID",
                subscription.id,
            )
            needs_cleanup = True

        if needs_cleanup:
            logger.info("🧹 Очищаем подписку %s (тариф=%s)", subscription.id, tariff_code)
            subscription.remnawave_uuid = None
            subscription.remnawave_short_uuid = None
            subscription.subscription_url = ""
            subscription.subscription_crypto_link = ""
            subscription.connected_squads = []

            if tariff_code == TariffCode.STANDARD.value:
                user.remnawave_uuid = None

            await db.commit()
            logger.info("✅ Подписка %s очищена", subscription.id)
            return True

        if updated_identity:
            await db.commit()

        return True

    except Exception as error:
        logger.error("❌ Ошибка валидации подписки: %s", error)
        await db.rollback()
        return False


async def _create_subscription_from_panel_data(
    self,
    db: AsyncSession,
    user: User,
    panel_user: Dict[str, Any],
    *,
    tariff_code: Optional[str] = None,
) -> None:
    try:
        from app.database.crud.subscription import create_subscription_no_commit

        tariff_code = normalize_tariff_code(
            tariff_code or _detect_tariff_from_panel_user(panel_user)
        )
        expire_at_str = panel_user.get("expireAt", "")
        expire_at = self._parse_remnawave_date(expire_at_str)

        panel_status = panel_user.get("status", "ACTIVE")
        current_time = self._now_utc()

        if panel_status == "ACTIVE" and expire_at > current_time:
            status = SubscriptionStatus.ACTIVE
        elif expire_at <= current_time:
            status = SubscriptionStatus.EXPIRED
        else:
            status = SubscriptionStatus.DISABLED

        traffic_limit_bytes = panel_user.get("trafficLimitBytes", 0)
        traffic_limit_gb = traffic_limit_bytes // (1024**3) if traffic_limit_bytes > 0 else 0

        used_traffic_bytes = panel_user.get("usedTrafficBytes", 0)
        traffic_used_gb = used_traffic_bytes / (1024**3)

        active_squads = panel_user.get("activeInternalSquads", [])
        squad_uuids = []
        if isinstance(active_squads, list):
            for squad in active_squads:
                if isinstance(squad, dict) and "uuid" in squad:
                    squad_uuids.append(squad["uuid"])
                elif isinstance(squad, str):
                    squad_uuids.append(squad)

        device_limit = panel_user.get("hwidDeviceLimit")
        if device_limit is None:
            device_limit = settings.DEFAULT_DEVICE_LIMIT

        subscription_data = {
            "user_id": user.id,
            "tariff_code": tariff_code,
            "status": status.value,
            "is_trial": False,
            "end_date": expire_at,
            "traffic_limit_gb": traffic_limit_gb,
            "traffic_used_gb": traffic_used_gb,
            "device_limit": device_limit,
            "connected_squads": squad_uuids,
            "remnawave_uuid": panel_user.get("uuid"),
            "remnawave_short_uuid": panel_user.get("shortUuid"),
            "subscription_url": panel_user.get("subscriptionUrl", ""),
            "subscription_crypto_link": (
                panel_user.get("subscriptionCryptoLink")
                or (panel_user.get("happ") or {}).get("cryptoLink", "")
            ),
        }

        await create_subscription_no_commit(db, **subscription_data)
        logger.info(
            "🧾 Подготовлена подписка для пользователя %s (тариф=%s, срок=%s)",
            user.telegram_id,
            tariff_code,
            expire_at,
        )

    except Exception as error:
        logger.error(
            "❌ Ошибка создания подписки для пользователя %s: %s",
            user.telegram_id,
            error,
        )


async def _update_subscription_from_panel_data(
    self,
    db: AsyncSession,
    user: User,
    panel_user: Dict[str, Any],
    *,
    tariff_code: Optional[str] = None,
) -> None:
    try:
        from app.database.crud.subscription import get_subscription_by_user_id

        tariff_code = normalize_tariff_code(
            tariff_code or _detect_tariff_from_panel_user(panel_user)
        )
        subscription = await get_subscription_by_user_id(
            db,
            user.id,
            tariff_code=tariff_code,
        )
        if not subscription:
            await _create_subscription_from_panel_data(
                self,
                db,
                user,
                panel_user,
                tariff_code=tariff_code,
            )
            return

        panel_status = panel_user.get("status", "ACTIVE")
        expire_at_str = panel_user.get("expireAt", "")
        if expire_at_str:
            expire_at = self._parse_remnawave_date(expire_at_str)
            if abs((subscription.end_date - expire_at).total_seconds()) > 60:
                subscription.end_date = expire_at

        current_time = self._now_utc()
        if panel_status == "ACTIVE" and subscription.end_date > current_time:
            new_status = SubscriptionStatus.ACTIVE.value
        elif subscription.end_date <= current_time:
            new_status = SubscriptionStatus.EXPIRED.value
        elif panel_status == "DISABLED":
            new_status = SubscriptionStatus.DISABLED.value
        else:
            new_status = subscription.status

        if subscription.status != new_status:
            subscription.status = new_status

        used_traffic_bytes = panel_user.get("usedTrafficBytes", 0)
        traffic_used_gb = used_traffic_bytes / (1024**3)
        if abs(subscription.traffic_used_gb - traffic_used_gb) > 0.01:
            subscription.traffic_used_gb = traffic_used_gb

        traffic_limit_bytes = panel_user.get("trafficLimitBytes", 0)
        traffic_limit_gb = traffic_limit_bytes // (1024**3) if traffic_limit_bytes > 0 else 0
        if subscription.traffic_limit_gb != traffic_limit_gb:
            subscription.traffic_limit_gb = traffic_limit_gb

        device_limit = panel_user.get("hwidDeviceLimit")
        if device_limit is None:
            device_limit = settings.DEFAULT_DEVICE_LIMIT
        if subscription.device_limit != device_limit:
            subscription.device_limit = device_limit

        panel_uuid = panel_user.get("uuid")
        if panel_uuid and subscription.remnawave_uuid != panel_uuid:
            subscription.remnawave_uuid = panel_uuid

        new_short_uuid = panel_user.get("shortUuid")
        if new_short_uuid and subscription.remnawave_short_uuid != new_short_uuid:
            subscription.remnawave_short_uuid = new_short_uuid

        panel_url = panel_user.get("subscriptionUrl", "")
        if panel_url and subscription.subscription_url != panel_url:
            subscription.subscription_url = panel_url

        panel_crypto = (
            panel_user.get("subscriptionCryptoLink")
            or (panel_user.get("happ") or {}).get("cryptoLink", "")
        )
        if panel_crypto and subscription.subscription_crypto_link != panel_crypto:
            subscription.subscription_crypto_link = panel_crypto

        active_squads = panel_user.get("activeInternalSquads", [])
        squad_uuids = []
        if isinstance(active_squads, list):
            for squad in active_squads:
                if isinstance(squad, dict) and "uuid" in squad:
                    squad_uuids.append(squad["uuid"])
                elif isinstance(squad, str):
                    squad_uuids.append(squad)

        if set(subscription.connected_squads or []) != set(squad_uuids):
            subscription.connected_squads = squad_uuids

        logger.debug(
            "🔄 Подписка обновлена для пользователя %s (тариф=%s)",
            user.telegram_id,
            tariff_code,
        )

    except Exception as error:
        logger.error(
            "❌ Ошибка обновления подписки для пользователя %s: %s",
            user.telegram_id,
            error,
        )
        raise


async def sync_users_from_panel(self, db: AsyncSession, sync_type: str = "all") -> Dict[str, int]:
    try:
        stats = {"created": 0, "updated": 0, "errors": 0, "deleted": 0}

        logger.info("🔄 Синхронизация RemnaWave из панели: %s", sync_type)

        async with self.get_api_client() as api:
            panel_users: List[Dict[str, Any]] = []
            start = 0
            size = 500

            while True:
                response = await api.get_all_users(start=start, size=size, enrich_happ_links=False)
                users_batch = response["users"]

                for user_obj in users_batch:
                    panel_users.append(
                        {
                            "uuid": user_obj.uuid,
                            "shortUuid": user_obj.short_uuid,
                            "username": user_obj.username,
                            "status": user_obj.status.value,
                            "telegramId": user_obj.telegram_id,
                            "expireAt": user_obj.expire_at.isoformat() + "Z",
                            "trafficLimitBytes": user_obj.traffic_limit_bytes,
                            "usedTrafficBytes": user_obj.used_traffic_bytes,
                            "hwidDeviceLimit": user_obj.hwid_device_limit,
                            "subscriptionUrl": user_obj.subscription_url,
                            "subscriptionCryptoLink": user_obj.happ_crypto_link,
                            "activeInternalSquads": user_obj.active_internal_squads,
                            "tag": user_obj.tag,
                        }
                    )

                if len(users_batch) < size:
                    break
                start += size

        panel_users_with_tg = [
            user for user in panel_users if user.get("telegramId") is not None
        ]
        panel_keys = {
            (user.get("telegramId"), _detect_tariff_from_panel_user(user))
            for user in panel_users_with_tg
        }

        bot_users_result = await db.execute(select(User))
        bot_users = bot_users_result.scalars().all()
        bot_users_by_telegram_id = {user.telegram_id: user for user in bot_users}
        bot_users_by_uuid = {
            user.remnawave_uuid: user
            for user in bot_users
            if getattr(user, "remnawave_uuid", None)
        }

        batch_size = 50
        pending_uuid_mutations: List[Any] = []

        for i, panel_user in enumerate(panel_users_with_tg):
            uuid_mutation = None
            try:
                telegram_id = panel_user.get("telegramId")
                if not telegram_id:
                    continue

                tariff_code = _detect_tariff_from_panel_user(panel_user)
                db_user = bot_users_by_telegram_id.get(telegram_id)

                if not db_user:
                    if sync_type in ["new_only", "all"]:
                        db_user, is_created = await self._get_or_create_bot_user_from_panel(db, panel_user)
                        if not db_user:
                            stats["errors"] += 1
                            continue
                        bot_users_by_telegram_id[telegram_id] = db_user

                        if tariff_code == TariffCode.STANDARD.value:
                            _, uuid_mutation = self._ensure_user_remnawave_uuid(
                                db_user,
                                panel_user.get("uuid"),
                                bot_users_by_uuid,
                            )

                        if is_created:
                            await _create_subscription_from_panel_data(
                                self,
                                db,
                                db_user,
                                panel_user,
                                tariff_code=tariff_code,
                            )
                            stats["created"] += 1
                        else:
                            await _update_subscription_from_panel_data(
                                self,
                                db,
                                db_user,
                                panel_user,
                                tariff_code=tariff_code,
                            )
                            stats["updated"] += 1
                else:
                    if sync_type in ["update_only", "all"]:
                        await _update_subscription_from_panel_data(
                            self,
                            db,
                            db_user,
                            panel_user,
                            tariff_code=tariff_code,
                        )

                        if tariff_code == TariffCode.STANDARD.value:
                            _, uuid_mutation = self._ensure_user_remnawave_uuid(
                                db_user,
                                panel_user.get("uuid"),
                                bot_users_by_uuid,
                            )

                        stats["updated"] += 1

            except Exception as user_error:
                logger.error("❌ Ошибка обработки пользователя панели: %s", user_error)
                stats["errors"] += 1
                if uuid_mutation:
                    uuid_mutation.rollback()
                if pending_uuid_mutations:
                    for mutation in reversed(pending_uuid_mutations):
                        mutation.rollback()
                    pending_uuid_mutations.clear()
                try:
                    await db.rollback()
                except Exception:
                    pass
                continue

            else:
                if uuid_mutation and uuid_mutation.has_changes():
                    pending_uuid_mutations.append(uuid_mutation)

            if (i + 1) % batch_size == 0:
                try:
                    await db.commit()
                    pending_uuid_mutations.clear()
                except Exception as commit_error:
                    logger.error("❌ Ошибка коммита синхронизации: %s", commit_error)
                    await db.rollback()
                    for mutation in reversed(pending_uuid_mutations):
                        mutation.rollback()
                    pending_uuid_mutations.clear()
                    stats["errors"] += batch_size

        try:
            await db.commit()
            pending_uuid_mutations.clear()
        except Exception as final_commit_error:
            logger.error("❌ Ошибка финального коммита синхронизации: %s", final_commit_error)
            await db.rollback()
            for mutation in reversed(pending_uuid_mutations):
                mutation.rollback()
            pending_uuid_mutations.clear()

        if sync_type == "all":
            try:
                from app.database.crud.subscription import get_subscriptions_batch

                offset = 0
                limit = 500
                current_time = datetime.utcnow()
                panel_uuid_set = {user.get("uuid") for user in panel_users_with_tg if user.get("uuid")}

                while True:
                    subscriptions = await get_subscriptions_batch(db, offset=offset, limit=limit)
                    if not subscriptions:
                        break

                    for subscription in subscriptions:
                        if not subscription.user:
                            continue

                        key = (
                            subscription.user.telegram_id,
                            normalize_tariff_code(subscription.tariff_code),
                        )
                        if key in panel_keys:
                            continue

                        sub_uuid = subscription.remnawave_uuid
                        if sub_uuid and sub_uuid in panel_uuid_set:
                            continue

                        subscription.status = SubscriptionStatus.DISABLED.value
                        subscription.is_trial = True
                        subscription.end_date = current_time
                        subscription.traffic_limit_gb = 0
                        subscription.traffic_used_gb = 0.0
                        subscription.device_limit = settings.DEFAULT_DEVICE_LIMIT
                        subscription.connected_squads = []
                        subscription.autopay_enabled = False
                        subscription.remnawave_uuid = None
                        subscription.remnawave_short_uuid = None
                        subscription.subscription_url = ""
                        subscription.subscription_crypto_link = ""
                        subscription.updated_at = current_time

                        if (
                            normalize_tariff_code(subscription.tariff_code)
                            == TariffCode.STANDARD.value
                            and subscription.user.remnawave_uuid == sub_uuid
                        ):
                            subscription.user.remnawave_uuid = None

                        stats["deleted"] += 1

                    await db.commit()

                    if len(subscriptions) < limit:
                        break

                    offset += limit

            except Exception as cleanup_error:
                logger.error("❌ Ошибка очистки после синхронизации: %s", cleanup_error)

        return stats

    except Exception as error:
        logger.error("❌ Ошибка синхронизации RemnaWave: %s", error)
        return {"created": 0, "updated": 0, "errors": 1, "deleted": 0}


async def sync_users_to_panel(self, db: AsyncSession) -> Dict[str, int]:
    from app.database.crud.subscription import get_subscriptions_batch

    try:
        stats = {"created": 0, "updated": 0, "errors": 0}

        batch_size = 500
        offset = 0
        concurrent_limit = 5

        async with self.get_api_client() as api:
            semaphore = asyncio.Semaphore(concurrent_limit)

            while True:
                subscriptions = await get_subscriptions_batch(
                    db,
                    offset=offset,
                    limit=batch_size,
                )

                if not subscriptions:
                    break

                valid_subscriptions = [s for s in subscriptions if s.user]
                if not valid_subscriptions:
                    if len(subscriptions) < batch_size:
                        break
                    offset += batch_size
                    continue

                async def process_subscription(sub: Subscription):
                    async with semaphore:
                        try:
                            user = sub.user
                            tariff_code = normalize_tariff_code(sub.tariff_code)
                            hwid_limit = _resolve_hwid_limit(sub, tariff_code)
                            expire_at = self._safe_expire_at_for_panel(sub.end_date)

                            is_subscription_active = (
                                sub.status in (
                                    SubscriptionStatus.ACTIVE.value,
                                    SubscriptionStatus.TRIAL.value,
                                )
                                and sub.end_date > datetime.utcnow()
                            )
                            status = UserStatus.ACTIVE if is_subscription_active else UserStatus.DISABLED

                            username = _build_remnawave_username(user, tariff_code)
                            user_tag = _resolve_user_tag(sub, tariff_code)

                            create_kwargs = dict(
                                username=username,
                                expire_at=expire_at,
                                status=status,
                                traffic_limit_bytes=sub.traffic_limit_gb * (1024**3)
                                if sub.traffic_limit_gb > 0
                                else 0,
                                traffic_limit_strategy=_resolve_traffic_limit_strategy(tariff_code),
                                telegram_id=user.telegram_id,
                                description=settings.format_remnawave_user_description(
                                    full_name=user.full_name,
                                    username=user.username,
                                    telegram_id=user.telegram_id,
                                ),
                                active_internal_squads=sub.connected_squads,
                            )

                            if user_tag is not None:
                                create_kwargs["tag"] = user_tag

                            if hwid_limit is not None:
                                create_kwargs["hwid_device_limit"] = hwid_limit

                            panel_uuid = _resolve_subscription_uuid(sub, user, tariff_code)
                            if panel_uuid and sub.remnawave_uuid != panel_uuid:
                                sub.remnawave_uuid = panel_uuid
                            if not panel_uuid:
                                existing_user = await api.get_user_by_username(username)
                                if not existing_user:
                                    candidates = await api.get_user_by_telegram_id(user.telegram_id)
                                    existing_user = _pick_panel_user_by_username(
                                        candidates,
                                        username,
                                        tariff_code,
                                    )
                                if existing_user:
                                    panel_uuid = existing_user.uuid
                                    sub.remnawave_uuid = panel_uuid
                                    if tariff_code == TariffCode.STANDARD.value:
                                        user.remnawave_uuid = panel_uuid

                            if panel_uuid:
                                update_kwargs = dict(
                                    uuid=panel_uuid,
                                    status=status,
                                    expire_at=expire_at,
                                    traffic_limit_bytes=create_kwargs["traffic_limit_bytes"],
                                    traffic_limit_strategy=_resolve_traffic_limit_strategy(tariff_code),
                                    description=create_kwargs["description"],
                                    active_internal_squads=sub.connected_squads,
                                )

                                if user_tag is not None:
                                    update_kwargs["tag"] = user_tag

                                if hwid_limit is not None:
                                    update_kwargs["hwid_device_limit"] = hwid_limit

                                try:
                                    updated_user = await api.update_user(**update_kwargs)
                                    sub.subscription_url = updated_user.subscription_url
                                    sub.subscription_crypto_link = updated_user.happ_crypto_link
                                    return ("updated", sub, None)
                                except RemnaWaveAPIError as api_error:
                                    if api_error.status_code == 404:
                                        new_user = await api.create_user(**create_kwargs)
                                        return ("created", sub, new_user)
                                    raise
                            else:
                                new_user = await api.create_user(**create_kwargs)
                                return ("created", sub, new_user)

                        except Exception as error:
                            logger.error(
                                "❌ Синхронизация с панелью не удалась для пользователя %s: %s",
                                sub.user.telegram_id if sub.user else "N/A",
                                error,
                            )
                            return ("error", sub, None)

                tasks = [process_subscription(s) for s in valid_subscriptions]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        stats["errors"] += 1
                        continue

                    action, sub, new_user = result
                    if action == "created":
                        if new_user:
                            sub.remnawave_uuid = new_user.uuid
                            sub.remnawave_short_uuid = new_user.short_uuid
                            sub.subscription_url = new_user.subscription_url
                            sub.subscription_crypto_link = new_user.happ_crypto_link
                            if normalize_tariff_code(sub.tariff_code) == TariffCode.STANDARD.value:
                                sub.user.remnawave_uuid = new_user.uuid
                        stats["created"] += 1
                    elif action == "updated":
                        stats["updated"] += 1
                    else:
                        stats["errors"] += 1

                try:
                    await db.commit()
                except Exception as commit_error:
                    logger.error("❌ Ошибка коммита синхронизации панели: %s", commit_error)
                    await db.rollback()
                    stats["errors"] += len(valid_subscriptions)

                if len(subscriptions) < batch_size:
                    break

                offset += batch_size

        return stats

    except Exception as error:
        logger.error("❌ Ошибка синхронизации RemnaWave в панель: %s", error)
        return {"created": 0, "updated": 0, "errors": 1}


def apply_remnawave_patches() -> None:
    from app.services.subscription_service import SubscriptionService
    from app.services.remnawave_service import RemnaWaveService

    if getattr(SubscriptionService, "_spiderman_remnawave_patched", False):
        return

    SubscriptionService.create_remnawave_user = create_remnawave_user
    SubscriptionService.update_remnawave_user = update_remnawave_user
    SubscriptionService.revoke_subscription = revoke_subscription
    SubscriptionService.sync_subscription_usage = sync_subscription_usage
    SubscriptionService.validate_and_clean_subscription = validate_and_clean_subscription
    SubscriptionService._spiderman_remnawave_patched = True

    if not getattr(RemnaWaveService, "_spiderman_remnawave_patched", False):
        RemnaWaveService.sync_users_from_panel = sync_users_from_panel
        RemnaWaveService._create_subscription_from_panel_data = _create_subscription_from_panel_data
        RemnaWaveService._update_subscription_from_panel_data = _update_subscription_from_panel_data
        RemnaWaveService.sync_users_to_panel = sync_users_to_panel
        RemnaWaveService._spiderman_remnawave_patched = True
