import html
import logging
from datetime import datetime
from typing import Dict, Optional

from aiogram import types
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.keyboards.inline import get_back_keyboard
from app.localization.texts import get_texts
from app.services.subscription_service import SubscriptionService
from app.spiderman.tariff_context import TariffCode, normalize_tariff_code
from app.utils.subscription_utils import get_display_subscription_link

logger = logging.getLogger(__name__)

_ORIGINAL_SHOW_SUBSCRIPTION_INFO = None


async def _get_devices_count_by_uuid(remnawave_uuid: Optional[str]) -> Optional[int]:
    if not remnawave_uuid:
        return None

    try:
        from app.services.remnawave_service import RemnaWaveService

        service = RemnaWaveService()
        async with service.get_api_client() as api:
            response = await api._make_request("GET", f"/api/hwid/devices/{remnawave_uuid}")
            if response and "response" in response:
                return int(response["response"].get("total", 0))
    except Exception as exc:
        logger.error("Failed to fetch devices for RemnaWave UUID %s: %s", remnawave_uuid, exc)

    return None


def _format_profile_block(db_user) -> str:
    name = html.escape(getattr(db_user, "full_name", "") or "")
    telegram_id = getattr(db_user, "telegram_id", "")
    balance = settings.format_price(getattr(db_user, "balance_kopeks", 0) or 0)

    return (
        "👤 <b>Профиль:</b>\n"
        "<blockquote>"
        f"📝 <b>Имя:</b> {name}\n"
        f"🆔 <code>{telegram_id}</code>\n"
        f"💳 <b>Баланс:</b> {balance}"
        "</blockquote>"
    )


def _is_subscription_active(subscription) -> bool:
    actual_status = (getattr(subscription, "actual_status", "") or "").lower()
    if actual_status not in {"active", "trial"}:
        return False
    end_date = getattr(subscription, "end_date", None)
    if not end_date:
        return False
    return end_date > datetime.utcnow()


def _format_inactive_block(label: str) -> str:
    return (
        f"🔑 <b>Ваша подписка {label}:</b>\n"
        "<blockquote>Подписка неактивна — оплатите ее в меню «Купить подписку»</blockquote>"
    )


def _format_device_limit_display(subscription) -> str:
    device_limit = getattr(subscription, "device_limit", 0) or 0
    if device_limit == 0:
        return "♾️"

    modem_enabled = getattr(subscription, "modem_enabled", False) or False
    if modem_enabled and settings.is_modem_enabled():
        visible_device_limit = max(0, device_limit - 1)
        return f"{visible_device_limit} + модем"

    return str(device_limit)



def _format_standard_info(
    subscription,
    devices_used: Optional[int],
    *,
    show_devices: bool,
) -> tuple[str, str]:
    now = datetime.utcnow()
    end_date = getattr(subscription, "end_date", None)
    days_left = 0
    if end_date and end_date > now:
        days_left = max(0, (end_date - now).days)

    devices_used_display = "—" if devices_used is None else str(devices_used)
    device_limit_display = _format_device_limit_display(subscription)

    heading = "📦 <b>Информация о тарифе:</b> 🕷️ Питер Паркер"
    lines = [
        f"🗓️ <b>Осталось:</b> {days_left} дней",
        "📊 <b>Трафик:</b> ♾️",
    ]
    if show_devices:
        lines.append(
            f"📱 <b>Кол-во устройств:</b> {devices_used_display}/{device_limit_display}"
        )
    return heading, "\n".join(lines)



def _format_white_info(subscription, *, show_devices: bool) -> tuple[str, str]:
    traffic_limit = getattr(subscription, "traffic_limit_gb", 0) or 0
    traffic_used = getattr(subscription, "traffic_used_gb", 0.0) or 0.0

    if traffic_limit <= 0:
        traffic_display = "♾️"
    else:
        remaining = max(0.0, traffic_limit - traffic_used)
        traffic_display = f"{remaining:.2f} GB"

    heading = "📦 <b>Информация о тарифе:</b> ⚪️ Саша Белый"
    lines = [f"📊 <b>Трафик:</b> {traffic_display}"]
    if show_devices:
        lines.append("📱 <b>Кол-во устройств:</b> ♾️")
    return heading, "\n".join(lines)



def _build_subscription_block(
    label: str,
    link: Optional[str],
    info_heading: str,
    info_text: str,
) -> str:
    lines = [f"🔑 <b>Ваша подписка {label}:</b>"]
    if link:
        lines.append(f"<code>{html.escape(link)}</code>")
        lines.append("")
    lines.append(info_heading)
    lines.append(f"<blockquote>{info_text}</blockquote>")
    return "\n".join(lines)


def _pick_primary_subscription(subscriptions_by_tariff: Dict[str, object]):
    standard = subscriptions_by_tariff.get(TariffCode.STANDARD.value)
    white = subscriptions_by_tariff.get(TariffCode.WHITE.value)

    if standard and _is_subscription_active(standard):
        return standard
    if white and _is_subscription_active(white):
        return white
    return standard or white


def _build_subscription_keyboard(
    language: str,
    *,
    standard_subscription,
    white_subscription,
    primary_subscription,
    hide_subscription_link: bool,
) -> types.InlineKeyboardMarkup:
    texts = get_texts(language)
    rows = []

    if standard_subscription and _is_subscription_active(standard_subscription):
        link = None if hide_subscription_link else get_display_subscription_link(standard_subscription)
        if link:
            rows.append([
                types.InlineKeyboardButton(
                    text="🔗 Подключиться к 🕷️ Standard",
                    url=link,
                )
            ])

    if white_subscription and _is_subscription_active(white_subscription):
        link = None if hide_subscription_link else get_display_subscription_link(white_subscription)
        if link:
            rows.append([
                types.InlineKeyboardButton(
                    text="🔗 Подключиться к ⚪️ White",
                    url=link,
                )
            ])

    if primary_subscription:
        is_trial = bool(getattr(primary_subscription, "is_trial", False))
        if not is_trial:
            rows.append([
                types.InlineKeyboardButton(
                    text=texts.MENU_EXTEND_SUBSCRIPTION,
                    callback_data="subscription_extend",
                )
            ])
            rows.append([
                types.InlineKeyboardButton(
                    text=texts.t("AUTOPAY_BUTTON", "💳 Автоплатеж"),
                    callback_data="subscription_autopay",
                )
            ])

        if is_trial:
            rows.append([
                types.InlineKeyboardButton(
                    text=texts.MENU_BUY_SUBSCRIPTION,
                    callback_data="subscription_upgrade",
                )
            ])
        else:
            if standard_subscription:
                rows.append([
                    types.InlineKeyboardButton(
                        text="📱 Изменить устройства",
                        callback_data="subscription_change_devices",
                    )
                ])
                rows.append([
                    types.InlineKeyboardButton(
                        text="⚙️ Управление устройствами",
                        callback_data="subscription_manage_devices",
                    )
                ])

            if (
                settings.is_traffic_topup_enabled()
                and not settings.is_traffic_topup_blocked()
                and primary_subscription
                and (getattr(primary_subscription, "traffic_limit_gb", 0) or 0) > 0
            ):
                rows.append([
                    types.InlineKeyboardButton(
                        text=texts.t("BUY_TRAFFIC_BUTTON", "📈 Докупить трафик"),
                        callback_data="buy_traffic",
                    )
                ])

    rows.append([types.InlineKeyboardButton(text=texts.BACK, callback_data="back_to_menu")])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


async def show_subscription_info(
    callback: types.CallbackQuery,
    db_user,
    db: AsyncSession,
):
    await db.refresh(db_user)
    texts = get_texts(db_user.language)

    from app.database.crud.subscription import (
        get_subscriptions_for_user,
        check_and_update_subscription_status,
    )

    subscriptions = await get_subscriptions_for_user(db, db_user.id)
    if not subscriptions:
        await callback.message.edit_text(
            texts.SUBSCRIPTION_NONE,
            reply_markup=get_back_keyboard(db_user.language),
        )
        await callback.answer()
        return

    subscriptions_by_tariff: Dict[str, object] = {}
    for subscription in subscriptions:
        tariff_code = normalize_tariff_code(getattr(subscription, "tariff_code", None))
        current = subscriptions_by_tariff.get(tariff_code)
        if current is None:
            subscriptions_by_tariff[tariff_code] = subscription
            continue
        current_created_at = getattr(current, "created_at", None)
        candidate_created_at = getattr(subscription, "created_at", None)
        if candidate_created_at and (not current_created_at or candidate_created_at > current_created_at):
            subscriptions_by_tariff[tariff_code] = subscription

    subscription_service = SubscriptionService()
    for subscription in subscriptions_by_tariff.values():
        subscription = await check_and_update_subscription_status(db, subscription)
        await subscription_service.sync_subscription_usage(db, subscription)

        if not getattr(subscription, "remnawave_uuid", None) or not getattr(subscription, "subscription_url", None):
            try:
                await subscription_service.create_remnawave_user(db, subscription, reset_traffic=False)
            except Exception as sync_error:
                logger.warning(
                    "RemnaWave sync failed for subscription %s: %s",
                    getattr(subscription, "id", None),
                    sync_error,
                )

    standard_subscription = subscriptions_by_tariff.get(TariffCode.STANDARD.value)
    white_subscription = subscriptions_by_tariff.get(TariffCode.WHITE.value)

    blocks = [_format_profile_block(db_user)]
    hide_subscription_link = settings.should_hide_subscription_link()
    show_devices = settings.is_devices_selection_enabled()

    if standard_subscription:
        if _is_subscription_active(standard_subscription):
            link = None
            if not hide_subscription_link:
                link = get_display_subscription_link(standard_subscription)
            devices_used = None
            if show_devices:
                devices_used = await _get_devices_count_by_uuid(
                    getattr(standard_subscription, "remnawave_uuid", None)
                    or getattr(db_user, "remnawave_uuid", None)
                )
            info_heading, info_text = _format_standard_info(
                standard_subscription,
                devices_used,
                show_devices=show_devices,
            )
            blocks.append(
                _build_subscription_block("Standard", link, info_heading, info_text)
            )
        else:
            blocks.append(_format_inactive_block("Standard"))

    if white_subscription:
        if _is_subscription_active(white_subscription):
            link = None
            if not hide_subscription_link:
                link = get_display_subscription_link(white_subscription)
            info_heading, info_text = _format_white_info(
                white_subscription,
                show_devices=show_devices,
            )
            blocks.append(_build_subscription_block("White", link, info_heading, info_text))
        else:
            blocks.append(_format_inactive_block("White"))

    primary_subscription = _pick_primary_subscription(subscriptions_by_tariff)
    message = "\n\n".join(blocks)

    await callback.message.edit_text(
        message,
        reply_markup=_build_subscription_keyboard(
            db_user.language,
            standard_subscription=standard_subscription,
            white_subscription=white_subscription,
            primary_subscription=primary_subscription,
            hide_subscription_link=hide_subscription_link,
        ),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
    await callback.answer()


def apply_subscription_menu_patches() -> None:
    import app.handlers.subscription.purchase as purchase
    import app.handlers.subscription as subscription_pkg

    if getattr(purchase, "_spiderman_subscription_menu_patched", False):
        return

    global _ORIGINAL_SHOW_SUBSCRIPTION_INFO
    _ORIGINAL_SHOW_SUBSCRIPTION_INFO = purchase.show_subscription_info

    purchase.show_subscription_info = show_subscription_info
    subscription_pkg.show_subscription_info = show_subscription_info
    purchase._spiderman_subscription_menu_patched = True
