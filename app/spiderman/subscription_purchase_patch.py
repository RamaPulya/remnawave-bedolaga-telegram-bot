import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from aiogram import F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import PERIOD_PRICES, settings
from app.database.crud import subscription as subscription_crud
from app.database.crud.transaction import create_transaction
from app.database.crud.user import subtract_user_balance
from app.database.models import SubscriptionStatus, TransactionType
from app.handlers.subscription.common import (
    _apply_discount_to_monthly_component,
    _apply_promo_offer_discount,
    _get_promo_offer_discount_percent,
)
from app.handlers.subscription.pricing import _build_subscription_period_prompt
from app.handlers.subscription.promo import _build_promo_group_discount_text, _get_promo_offer_hint
from app.handlers.subscription.summary import present_subscription_summary
from app.keyboards.inline import (
    get_back_keyboard,
    get_devices_keyboard,
    get_happ_download_button_row,
    get_insufficient_balance_keyboard,
    get_subscription_period_keyboard,
)
from app.localization.texts import get_texts
from app.services.blacklist_service import blacklist_service
from app.services.subscription_checkout_service import (
    clear_subscription_checkout_draft,
    save_subscription_checkout_draft,
    should_offer_checkout_resume,
)
from app.services.subscription_service import SubscriptionService
from app.services.user_cart_service import user_cart_service
from app.spiderman.tariff_context import TariffCode, normalize_tariff_code
from app.states import SubscriptionStates
from app.utils.pricing_utils import (
    apply_percentage_discount,
    calculate_months_from_days,
    format_period_description,
    validate_pricing_calculation,
)
from app.utils.subscription_utils import get_display_subscription_link

from app.handlers.subscription.purchase import _edit_message_text_or_caption

logger = logging.getLogger(__name__)

_ORIGINAL_START_PURCHASE = None
_ORIGINAL_SELECT_PERIOD = None
_ORIGINAL_SELECT_TRAFFIC = None
_ORIGINAL_CONFIRM_PURCHASE = None
_ORIGINAL_PREPARE_SUMMARY = None
_ORIGINAL_HANDLE_CONFIG_BACK = None
_ORIGINAL_REGISTER_HANDLERS = None


async def _get_last_paid_period_days(
    db: AsyncSession,
    subscription_id: Optional[int],
    fallback_days: int,
    available_periods: Optional[List[int]] = None,
) -> int:
    if not subscription_id:
        return fallback_days

    try:
        from sqlalchemy import select
        from app.database.models import SubscriptionEvent

        result = await db.execute(
            select(SubscriptionEvent)
            .where(
                SubscriptionEvent.subscription_id == subscription_id,
                SubscriptionEvent.event_type.in_(["purchase", "renewal"]),
            )
            .order_by(SubscriptionEvent.occurred_at.desc())
            .limit(1)
        )
        event = result.scalar_one_or_none()
        if event and isinstance(event.extra, dict):
            period_days = event.extra.get("period_days")
            if period_days:
                parsed = int(period_days)
                if available_periods and parsed not in available_periods:
                    return fallback_days
                return parsed
    except Exception as error:
        logger.warning("Failed to resolve last paid period for subscription %s: %s", subscription_id, error)

    return fallback_days


def _parse_uuid_list(raw_value: Optional[str]) -> List[str]:
    if not raw_value:
        return []
    items = []
    for chunk in str(raw_value).split(","):
        value = chunk.strip()
        if value:
            items.append(value)
    return items


def _get_tariff_squads(tariff_code: str) -> List[str]:
    if tariff_code == TariffCode.WHITE.value:
        return _parse_uuid_list(settings.WHITE_TARIFF_SQUADS)
    return _parse_uuid_list(settings.STANDARD_TARIFF_SQUADS)


def _get_tariff_display_name(tariff_code: str) -> str:
    if tariff_code == TariffCode.WHITE.value:
        return "White (⚪️Саша белый)"
    return "Standard (🕷Питер Паркер)"

def _get_default_period_days() -> int:
    periods = settings.get_available_subscription_periods()
    if periods:
        return periods[0]
    return 30

def _get_white_unlimited_end_date() -> datetime:
    return datetime(2099, 1, 1)


def _build_tariff_keyboard(language: str) -> InlineKeyboardMarkup:
    texts = get_texts(language)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🕷 Standard (Питер Паркер)",
                    callback_data="tariff_standard",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⚪️ White (Саша белый)",
                    callback_data="tariff_white",
                )
            ],
            [InlineKeyboardButton(text=texts.BACK, callback_data="back_to_menu")],
        ]
    )


async def _build_tariff_prompt(db_user, texts, db: AsyncSession) -> str:
    base_text = texts.t(
        "SUBSCRIPTION_TARIFF_SELECT_PROMPT",
        (
            "\n💎 <b>Настройка подписки</b>\n\n"
            "Давайте настроим вашу подписку под ваши потребности.\n\n"
            "Сначала выберите тип подписки:\n"
        ),
    ).rstrip()
    lines: List[str] = [base_text]

    promo_offer_hint = await _get_promo_offer_hint(db, db_user, texts)
    if promo_offer_hint:
        lines.extend(["", promo_offer_hint])

    promo_text = await _build_promo_group_discount_text(
        db_user,
        settings.get_available_subscription_periods(),
        texts=texts,
    )
    if promo_text:
        lines.extend(["", promo_text])

    return "\n".join(lines) + "\n"


def _build_period_keyboard_with_back(
    language: str,
    user,
) -> InlineKeyboardMarkup:
    base_markup = get_subscription_period_keyboard(language, user)
    keyboard = [list(row) for row in base_markup.inline_keyboard]
    if keyboard:
        back_row = keyboard[-1]
        if back_row and back_row[0].callback_data == "back_to_menu":
            back_button = InlineKeyboardButton(
                text=back_row[0].text,
                callback_data="subscription_config_back",
            )
            keyboard[-1] = [back_button]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def _build_tariff_traffic_keyboard(language: str) -> InlineKeyboardMarkup:
    texts = get_texts(language)
    keyboard: List[List[InlineKeyboardButton]] = []

    for package in settings.get_traffic_packages():
        if not package.get("enabled", False):
            continue
        gb = int(package.get("gb") or 0)
        price = int(package.get("price") or 0)
        if gb == 0:
            text = f"♾️ Безлимит - {settings.format_price(price)}"
        else:
            text = f"📊 {gb} ГБ - {settings.format_price(price)}"
        keyboard.append([InlineKeyboardButton(text=text, callback_data=f"traffic_{gb}")])

    if not keyboard:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=texts.t(
                        "TRAFFIC_PACKAGES_NOT_CONFIGURED",
                        "⚠️ Пакеты трафика не настроены",
                    ),
                    callback_data="no_traffic_packages",
                )
            ]
        )

    keyboard.append([InlineKeyboardButton(text=texts.BACK, callback_data="subscription_config_back")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def _show_tariff_selection(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
    db: AsyncSession,
) -> None:
    texts = get_texts(db_user.language)
    prompt_text = await _build_tariff_prompt(db_user, texts, db)
    await _edit_message_text_or_caption(
        callback.message,
        prompt_text,
        _build_tariff_keyboard(db_user.language),
    )
    await state.set_state(SubscriptionStates.selecting_period)


async def _show_period_selection(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
    db: AsyncSession,
) -> None:
    texts = get_texts(db_user.language)
    prompt_text = await _build_subscription_period_prompt(db_user, texts, db)
    await _edit_message_text_or_caption(
        callback.message,
        prompt_text,
        _build_period_keyboard_with_back(db_user.language, db_user),
    )
    await state.set_state(SubscriptionStates.selecting_period)


async def start_subscription_purchase(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
    db: AsyncSession,
):
    texts = get_texts(db_user.language)
    prompt_text = await _build_tariff_prompt(db_user, texts, db)

    await _edit_message_text_or_caption(
        callback.message,
        prompt_text,
        _build_tariff_keyboard(db_user.language),
    )

    if settings.is_devices_selection_enabled():
        initial_devices = settings.DEFAULT_DEVICE_LIMIT
    else:
        forced_limit = settings.get_disabled_mode_device_limit()
        initial_devices = (
            settings.DEFAULT_DEVICE_LIMIT
            if forced_limit is None
            else forced_limit
        )

    initial_data = {
        "tariff_code": None,
        "period_days": None,
        "countries": [],
        "devices": initial_devices,
        "total_price": 0,
        "traffic_gb": None,
    }

    await state.set_data(initial_data)
    await state.set_state(SubscriptionStates.selecting_period)
    await callback.answer()


async def select_tariff(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
    db: AsyncSession,
):
    existing_data = await state.get_data()
    extend_mode = bool(existing_data.get("spiderman_extend_mode"))
    raw_value = callback.data.split("_", 1)[1] if callback.data else ""
    tariff_code = normalize_tariff_code(raw_value)

    squads = _get_tariff_squads(tariff_code)
    if not squads:
        setting_name = (
            "WHITE_TARIFF_SQUADS"
            if tariff_code == TariffCode.WHITE.value
            else "STANDARD_TARIFF_SQUADS"
        )
        await callback.answer(
            f"⚠️ Не настроены сквады тарифа. Заполните {setting_name}.",
            show_alert=True,
        )
        return

    subscription = await subscription_crud.get_subscription_by_user_id(
        db,
        db_user.id,
        tariff_code=tariff_code,
    )

    if extend_mode and tariff_code == TariffCode.STANDARD.value:
        if not subscription:
            await callback.answer("Подписка Standard не найдена", show_alert=True)
            return

        available_periods = settings.get_available_subscription_periods()
        fallback_period = available_periods[0] if available_periods else 30
        period_days = await _get_last_paid_period_days(
            db,
            subscription.id,
            fallback_period,
            available_periods,
        )

        devices_selected = subscription.device_limit
        if devices_selected is None:
            devices_selected = settings.DEFAULT_DEVICE_LIMIT
        if not settings.is_devices_selection_enabled():
            forced_limit = settings.get_disabled_mode_device_limit()
            if forced_limit is not None:
                devices_selected = forced_limit

        countries = list(subscription.connected_squads or [])
        if not countries:
            countries = squads

        data: Dict[str, Any] = {
            "tariff_code": tariff_code,
            "period_days": period_days,
            "countries": countries,
            "devices": devices_selected,
            "total_price": 0,
            "traffic_gb": 0,
            "spiderman_extend_mode": True,
        }

        await state.set_data(data)
        texts = get_texts(db_user.language)
        if await present_subscription_summary(callback, state, db_user, texts):
            await callback.answer()
        return

    if settings.is_devices_selection_enabled():
        initial_devices = settings.DEFAULT_DEVICE_LIMIT
        if subscription and getattr(subscription, "device_limit", None) is not None:
            initial_devices = max(settings.DEFAULT_DEVICE_LIMIT, subscription.device_limit)
    else:
        forced_limit = settings.get_disabled_mode_device_limit()
        initial_devices = (
            settings.DEFAULT_DEVICE_LIMIT
            if forced_limit is None
            else forced_limit
        )

    data: Dict[str, Any] = {
        "tariff_code": tariff_code,
        "period_days": None,
        "countries": squads,
        "devices": initial_devices,
        "total_price": 0,
    }

    if tariff_code == TariffCode.STANDARD.value:
        data["traffic_gb"] = 0
    else:
        data["traffic_gb"] = None

    if tariff_code == TariffCode.WHITE.value:
        texts = get_texts(db_user.language)
        data["period_days"] = 0
        data["months_in_period"] = 1
        data["total_price"] = 0
        data["devices"] = 0
        await state.set_data(data)

        available_packages = [pkg for pkg in settings.get_traffic_packages() if pkg.get("enabled")]
        if not available_packages:
            await callback.answer("⚠️ Пакеты трафика не настроены", show_alert=True)
            return

        await _edit_message_text_or_caption(
            callback.message,
            texts.SELECT_TRAFFIC,
            _build_tariff_traffic_keyboard(db_user.language),
        )
        await state.set_state(SubscriptionStates.selecting_traffic)
        await callback.answer()
        return

    await state.set_data(data)
    await _show_period_selection(callback, state, db_user, db)
    await callback.answer()


async def select_period(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
):
    data = await state.get_data()
    tariff_raw = data.get("tariff_code")
    if not tariff_raw:
        await callback.answer()
        return

    tariff_code = normalize_tariff_code(tariff_raw)
    if tariff_code not in (TariffCode.STANDARD.value, TariffCode.WHITE.value):
        return await _ORIGINAL_SELECT_PERIOD(callback, state, db_user)

    period_days = int(callback.data.split("_")[1])
    texts = get_texts(db_user.language)

    data["period_days"] = period_days
    data["total_price"] = PERIOD_PRICES[period_days]

    if tariff_code == TariffCode.WHITE.value:
        available_packages = [pkg for pkg in settings.get_traffic_packages() if pkg["enabled"]]
        if not available_packages:
            await callback.answer("⚠️ Пакеты трафика не настроены", show_alert=True)
            return
        await state.set_data(data)
        await _edit_message_text_or_caption(
            callback.message,
            texts.SELECT_TRAFFIC,
            _build_tariff_traffic_keyboard(db_user.language),
        )
        await state.set_state(SubscriptionStates.selecting_traffic)
        await callback.answer()
        return

    data["traffic_gb"] = 0
    if not data.get("countries"):
        data["countries"] = _get_tariff_squads(tariff_code)

    await state.set_data(data)

    if settings.is_devices_selection_enabled():
        selected_devices = data.get("devices", settings.DEFAULT_DEVICE_LIMIT)
        await callback.message.edit_text(
            texts.SELECT_DEVICES,
            reply_markup=get_devices_keyboard(selected_devices, db_user.language),
        )
        await state.set_state(SubscriptionStates.selecting_devices)
        await callback.answer()
        return

    if await present_subscription_summary(callback, state, db_user, texts):
        await callback.answer()


async def select_traffic(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
):
    data = await state.get_data()
    tariff_raw = data.get("tariff_code")
    if not tariff_raw:
        return await _ORIGINAL_SELECT_TRAFFIC(callback, state, db_user)

    tariff_code = normalize_tariff_code(tariff_raw)
    if tariff_code != TariffCode.WHITE.value:
        return await _ORIGINAL_SELECT_TRAFFIC(callback, state, db_user)

    traffic_gb = int(callback.data.split("_")[1])
    texts = get_texts(db_user.language)

    data["traffic_gb"] = traffic_gb
    data["total_price"] = data.get("total_price", 0) + settings.get_traffic_price(traffic_gb)
    if not data.get("countries"):
        data["countries"] = _get_tariff_squads(tariff_code)

    await state.set_data(data)

    if await present_subscription_summary(callback, state, db_user, texts):
        await callback.answer()

async def _prepare_subscription_summary(
    db_user,
    data: Dict[str, Any],
    texts,
) -> Tuple[str, Dict[str, Any]]:
    tariff_raw = data.get("tariff_code")
    if not tariff_raw:
        return await _ORIGINAL_PREPARE_SUMMARY(db_user, data, texts)

    tariff_code = normalize_tariff_code(tariff_raw)
    if tariff_code not in (TariffCode.STANDARD.value, TariffCode.WHITE.value):
        return await _ORIGINAL_PREPARE_SUMMARY(db_user, data, texts)

    summary_data = dict(data)
    period_days = summary_data.get("period_days")
    if period_days is None:
        raise ValueError("Missing period for tariff purchase")

    if tariff_code == TariffCode.WHITE.value:
        months_in_period = 1
        period_display = "♾️"
        base_price_original = 0
        period_discount_percent = 0
        base_price = 0
        base_discount_total = 0
        period_days_for_discount = _get_default_period_days()
    else:
        if not period_days:
            raise ValueError("Missing period for tariff purchase")
        months_in_period = calculate_months_from_days(period_days)
        period_display = format_period_description(period_days, db_user.language)
        base_price_original = PERIOD_PRICES[period_days]
        period_discount_percent = db_user.get_promo_discount("period", period_days)
        base_price, base_discount_total = apply_percentage_discount(
            base_price_original,
            period_discount_percent,
        )
        period_days_for_discount = period_days

    if tariff_code == TariffCode.STANDARD.value:
        traffic_gb = 0
        traffic_display = "♾️"
        traffic_price_per_month = 0
        traffic_discount_percent = 0
    else:
        traffic_gb = int(summary_data.get("traffic_gb") or 0)
        traffic_display = "♾️" if traffic_gb == 0 else f"{traffic_gb} ГБ"
        traffic_price_per_month = settings.get_traffic_price(traffic_gb)
        traffic_discount_percent = db_user.get_promo_discount("traffic", period_days_for_discount)

    traffic_component = _apply_discount_to_monthly_component(
        traffic_price_per_month,
        traffic_discount_percent,
        months_in_period,
    )
    total_traffic_price = traffic_component["total"]

    devices_selection_enabled = settings.is_devices_selection_enabled()
    forced_disabled_limit: Optional[int] = None
    if devices_selection_enabled:
        devices_selected = summary_data.get("devices", settings.DEFAULT_DEVICE_LIMIT)
    else:
        forced_disabled_limit = settings.get_disabled_mode_device_limit()
        if forced_disabled_limit is None:
            devices_selected = settings.DEFAULT_DEVICE_LIMIT
        else:
            devices_selected = forced_disabled_limit
    if tariff_code == TariffCode.WHITE.value:
        devices_selected = 0

    summary_data["devices"] = devices_selected

    if tariff_code == TariffCode.WHITE.value:
        devices_display = "♾️"
        additional_devices = 0
        devices_price_per_month = 0
        devices_discount_percent = 0
        devices_component = _apply_discount_to_monthly_component(0, 0, months_in_period)
        total_devices_price = 0
    else:
        devices_display = str(devices_selected)
        additional_devices = max(0, devices_selected - settings.DEFAULT_DEVICE_LIMIT)
        devices_price_per_month = additional_devices * settings.PRICE_PER_DEVICE
        devices_discount_percent = db_user.get_promo_discount("devices", period_days_for_discount)
        devices_component = _apply_discount_to_monthly_component(
            devices_price_per_month,
            devices_discount_percent,
            months_in_period,
        )
        total_devices_price = devices_component["total"]

    countries_price_per_month = 0
    total_countries_price = 0
    discounted_servers_price_per_month = 0
    total_servers_discount = 0
    servers_discount_percent = 0

    total_price = base_price + total_traffic_price + total_countries_price + total_devices_price

    discounted_monthly_additions = (
        traffic_component["discounted_per_month"]
        + discounted_servers_price_per_month
        + devices_component["discounted_per_month"]
    )

    is_valid = validate_pricing_calculation(
        base_price,
        discounted_monthly_additions,
        months_in_period,
        total_price,
    )

    if not is_valid:
        raise ValueError("Subscription price calculation validation failed")

    original_total_price = total_price
    promo_offer_component = _apply_promo_offer_discount(db_user, total_price)
    if promo_offer_component["discount"] > 0:
        total_price = promo_offer_component["discounted"]

    summary_data["total_price"] = total_price
    if promo_offer_component["discount"] > 0:
        summary_data["promo_offer_discount_percent"] = promo_offer_component["percent"]
        summary_data["promo_offer_discount_value"] = promo_offer_component["discount"]
        summary_data["total_price_before_promo_offer"] = original_total_price
    else:
        summary_data.pop("promo_offer_discount_percent", None)
        summary_data.pop("promo_offer_discount_value", None)
        summary_data.pop("total_price_before_promo_offer", None)

    summary_data["server_prices_for_period"] = [0] * len(summary_data.get("countries", []))
    summary_data["months_in_period"] = months_in_period
    summary_data["base_price"] = base_price
    summary_data["base_price_original"] = base_price_original
    summary_data["base_discount_percent"] = period_discount_percent
    summary_data["base_discount_total"] = base_discount_total
    summary_data["final_traffic_gb"] = traffic_gb
    summary_data["traffic_price_per_month"] = traffic_price_per_month
    summary_data["traffic_discount_percent"] = traffic_component["discount_percent"]
    summary_data["traffic_discount_total"] = traffic_component["discount_total"]
    summary_data["traffic_discounted_price_per_month"] = traffic_component["discounted_per_month"]
    summary_data["total_traffic_price"] = total_traffic_price
    summary_data["servers_price_per_month"] = countries_price_per_month
    summary_data["countries_price_per_month"] = countries_price_per_month
    summary_data["servers_discount_percent"] = servers_discount_percent
    summary_data["servers_discount_total"] = total_servers_discount
    summary_data["servers_discounted_price_per_month"] = discounted_servers_price_per_month
    summary_data["total_servers_price"] = total_countries_price
    summary_data["total_countries_price"] = total_countries_price
    summary_data["devices_price_per_month"] = devices_price_per_month
    summary_data["devices_discount_percent"] = devices_component["discount_percent"]
    summary_data["devices_discount_total"] = devices_component["discount_total"]
    summary_data["devices_discounted_price_per_month"] = devices_component["discounted_per_month"]
    summary_data["total_devices_price"] = total_devices_price
    summary_data["discounted_monthly_additions"] = discounted_monthly_additions
    summary_data["tariff_code"] = tariff_code

    details_lines = []

    if base_discount_total > 0 and base_price > 0:
        base_line = (
            f"- Базовый период: <s>{texts.format_price(base_price_original)}</s> "
            f"{texts.format_price(base_price)}"
            f" (скидка {period_discount_percent}%:"
            f" -{texts.format_price(base_discount_total)})"
        )
        details_lines.append(base_line)
    elif base_price_original > 0:
        details_lines.append(f"- Базовый период: {texts.format_price(base_price_original)}")

    if total_traffic_price > 0:
        traffic_line = (
            f"- Трафик: {texts.format_price(traffic_price_per_month)}/мес × {months_in_period}"
            f" = {texts.format_price(total_traffic_price)}"
        )
        if traffic_component["discount_total"] > 0:
            traffic_line += (
                f" (скидка {traffic_component['discount_percent']}%:"
                f" -{texts.format_price(traffic_component['discount_total'])})"
            )
        details_lines.append(traffic_line)

    if devices_selection_enabled and total_devices_price > 0:
        devices_line = (
            f"- Доп. устройства: {texts.format_price(devices_price_per_month)}/мес × {months_in_period}"
            f" = {texts.format_price(total_devices_price)}"
        )
        if devices_component["discount_total"] > 0:
            devices_line += (
                f" (скидка {devices_component['discount_percent']}%:"
                f" -{texts.format_price(devices_component['discount_total'])})"
            )
        details_lines.append(devices_line)

    if promo_offer_component["discount"] > 0:
        details_lines.append(
            texts.t(
                "SUBSCRIPTION_SUMMARY_PROMO_DISCOUNT",
                "- Промо-предложение: -{amount} ({percent}% дополнительно)",
            ).format(
                amount=texts.format_price(promo_offer_component["discount"]),
                percent=promo_offer_component["percent"],
            )
        )

    details_text = "\n".join(details_lines)

    summary_lines = [
        "📋 <b>Сводка заказа</b>",
        "",
        f"🌍 <b>Тип:</b> {_get_tariff_display_name(tariff_code)}",
        f"📅 <b>Период:</b> {period_display}",
        f"📊 <b>Трафик:</b> {traffic_display}",
        f"📱 <b>Устройства:</b> {devices_display}",
        "",
        "💰 <b>Детализация стоимости:</b>",
        details_text,
        "",
        f"💎 <b>Общая стоимость:</b> {texts.format_price(total_price)}",
        "",
        "Подтверждаете покупку?",
    ]

    summary_text = "\n".join(summary_lines)
    return summary_text, summary_data

async def confirm_purchase(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
    db: AsyncSession,
):
    from app.services.admin_notification_service import AdminNotificationService

    is_blacklisted, blacklist_reason = await blacklist_service.is_user_blacklisted(
        callback.from_user.id,
        callback.from_user.username,
    )

    if is_blacklisted:
        logger.warning(
            "🚫 Пользователь %s находится в черном списке: %s",
            callback.from_user.id,
            blacklist_reason,
        )
        try:
            await callback.answer(
                f"🚫 Покупка подписки невозможна\n\n"
                f"Причина: {blacklist_reason}\n\n"
                f"Если вы считаете, что это ошибка, обратитесь в поддержку.",
                show_alert=True,
            )
        except Exception as error:
            logger.error("Ошибка при отправке сообщения о блокировке: %s", error)
        return

    if getattr(db_user, "restriction_subscription", False):
        reason = getattr(db_user, "restriction_reason", None) or "Действие ограничено администратором"
        texts = get_texts(db_user.language)
        support_url = settings.get_support_contact_url()
        keyboard = []
        if support_url:
            keyboard.append([types.InlineKeyboardButton(text="🆘 Обжаловать", url=support_url)])
        keyboard.append([types.InlineKeyboardButton(text=texts.BACK, callback_data="subscription")])

        await callback.message.edit_text(
            f"🚫 <b>Покупка/продление подписки ограничено</b>\n\n{reason}\n\n"
            "Если вы считаете это ошибкой, вы можете обжаловать решение.",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=keyboard),
        )
        await callback.answer()
        return

    data = await state.get_data()
    tariff_raw = data.get("tariff_code")
    if not tariff_raw:
        return await _ORIGINAL_CONFIRM_PURCHASE(callback, state, db_user, db)

    tariff_code = normalize_tariff_code(tariff_raw)
    if tariff_code not in (TariffCode.STANDARD.value, TariffCode.WHITE.value):
        return await _ORIGINAL_CONFIRM_PURCHASE(callback, state, db_user, db)

    texts = get_texts(db_user.language)

    await save_subscription_checkout_draft(db_user.id, dict(data))
    resume_callback = (
        "subscription_resume_checkout"
        if should_offer_checkout_resume(db_user, True)
        else None
    )

    period_days = data.get("period_days")
    if period_days is None:
        await callback.message.edit_text(
            texts.t(
                "SUBSCRIPTION_PURCHASE_ERROR",
                "Ошибка при оформлении подписки. Попробуйте начать сначала.",
            ),
            reply_markup=get_back_keyboard(db_user.language),
        )
        await callback.answer()
        return

    if tariff_code == TariffCode.WHITE.value:
        months_in_period = data.get("months_in_period") or 1
        base_price_original = 0
        base_discount_percent = 0
        base_price = 0
        base_discount_total = 0
        period_days_for_discount = _get_default_period_days()
    else:
        months_in_period = data.get("months_in_period", calculate_months_from_days(period_days))
        base_price_original = PERIOD_PRICES[period_days]
        base_discount_percent = db_user.get_promo_discount("period", period_days)
        base_price, base_discount_total = apply_percentage_discount(
            base_price_original,
            base_discount_percent,
        )
        period_days_for_discount = period_days

    selected_countries = data.get("countries", [])
    if not selected_countries:
        await callback.message.edit_text(
            texts.t(
                "COUNTRIES_MINIMUM_REQUIRED",
                "❌ Нельзя отключить все страны. Должна быть подключена хотя бы одна страна.",
            ),
            reply_markup=get_back_keyboard(db_user.language),
        )
        await callback.answer()
        return

    server_prices = data.get("server_prices_for_period") or []
    if not server_prices:
        server_prices = [0] * len(selected_countries)
    total_countries_price = data.get("total_servers_price", sum(server_prices))
    countries_price_per_month = data.get("servers_price_per_month", 0)
    discounted_servers_price_per_month = data.get(
        "servers_discounted_price_per_month",
        countries_price_per_month,
    )
    total_servers_discount = data.get("servers_discount_total", 0)
    servers_discount_percent = data.get("servers_discount_percent", 0)

    devices_selection_enabled = settings.is_devices_selection_enabled()
    forced_disabled_limit: Optional[int] = None
    if devices_selection_enabled:
        devices_selected = data.get("devices", settings.DEFAULT_DEVICE_LIMIT)
    else:
        forced_disabled_limit = settings.get_disabled_mode_device_limit()
        if forced_disabled_limit is None:
            devices_selected = settings.DEFAULT_DEVICE_LIMIT
        else:
            devices_selected = forced_disabled_limit
    if tariff_code == TariffCode.WHITE.value:
        devices_selected = 0

    additional_devices = max(0, devices_selected - settings.DEFAULT_DEVICE_LIMIT)
    devices_price_per_month = data.get(
        "devices_price_per_month",
        additional_devices * settings.PRICE_PER_DEVICE,
    )

    devices_discount_percent = 0
    discounted_devices_price_per_month = 0
    devices_discount_total = 0
    total_devices_price = 0

    if tariff_code == TariffCode.STANDARD.value and devices_selection_enabled and additional_devices > 0:
        if "devices_discount_percent" in data:
            devices_discount_percent = data.get("devices_discount_percent", 0)
            discounted_devices_price_per_month = data.get(
                "devices_discounted_price_per_month",
                devices_price_per_month,
            )
            devices_discount_total = data.get("devices_discount_total", 0)
            total_devices_price = data.get(
                "total_devices_price",
                discounted_devices_price_per_month * months_in_period,
            )
        else:
            devices_discount_percent = db_user.get_promo_discount("devices", period_days_for_discount)
            discounted_devices_price_per_month, discount_per_month = apply_percentage_discount(
                devices_price_per_month,
                devices_discount_percent,
            )
            devices_discount_total = discount_per_month * months_in_period
            total_devices_price = discounted_devices_price_per_month * months_in_period

    if tariff_code == TariffCode.STANDARD.value:
        final_traffic_gb = 0
        traffic_price_per_month = 0
    else:
        final_traffic_gb = data.get("final_traffic_gb", data.get("traffic_gb"))
        if final_traffic_gb is None:
            await callback.answer("⚠️ Выберите пакет трафика", show_alert=True)
            return
        traffic_price_per_month = data.get(
            "traffic_price_per_month",
            settings.get_traffic_price(final_traffic_gb),
        )

    if tariff_code == TariffCode.STANDARD.value:
        traffic_discount_percent = 0
        discounted_traffic_price_per_month = 0
        traffic_discount_total = 0
        total_traffic_price = 0
    elif "traffic_discount_percent" in data:
        traffic_discount_percent = data.get("traffic_discount_percent", 0)
        discounted_traffic_price_per_month = data.get(
            "traffic_discounted_price_per_month",
            traffic_price_per_month,
        )
        traffic_discount_total = data.get("traffic_discount_total", 0)
        total_traffic_price = data.get(
            "total_traffic_price",
            discounted_traffic_price_per_month * months_in_period,
        )
    else:
        traffic_discount_percent = db_user.get_promo_discount("traffic", period_days_for_discount)
        discounted_traffic_price_per_month, discount_per_month = apply_percentage_discount(
            traffic_price_per_month,
            traffic_discount_percent,
        )
        traffic_discount_total = discount_per_month * months_in_period
        total_traffic_price = discounted_traffic_price_per_month * months_in_period

    total_servers_price = data.get("total_servers_price", total_countries_price)

    cached_total_price = data.get("total_price", 0)
    cached_promo_discount_value = data.get("promo_offer_discount_value", 0)

    discounted_monthly_additions = (
        discounted_traffic_price_per_month
        + discounted_servers_price_per_month
        + discounted_devices_price_per_month
    )

    calculated_total_before_promo = base_price + (discounted_monthly_additions * months_in_period)

    validation_total_price = data.get("total_price_before_promo_offer")
    if validation_total_price is None and cached_promo_discount_value > 0:
        validation_total_price = cached_total_price + cached_promo_discount_value
    if validation_total_price is None:
        validation_total_price = cached_total_price

    current_promo_offer_percent = _get_promo_offer_discount_percent(db_user)

    if current_promo_offer_percent > 0:
        final_price, promo_offer_discount_value = apply_percentage_discount(
            calculated_total_before_promo,
            current_promo_offer_percent,
        )
        promo_offer_discount_percent = current_promo_offer_percent
    else:
        final_price = calculated_total_before_promo
        promo_offer_discount_value = 0
        promo_offer_discount_percent = 0

    price_difference = abs(final_price - cached_total_price)
    max_allowed_difference = max(500, int(final_price * 0.05))

    if price_difference > max_allowed_difference:
        logger.error(
            "Критическое расхождение цены для пользователя %s: кэш=%s₽, пересчет=%s₽, разница=%s₽.",
            db_user.telegram_id,
            cached_total_price / 100,
            final_price / 100,
            price_difference / 100,
        )
        await callback.answer(
            "Цена изменилась. Пожалуйста, начните оформление заново.",
            show_alert=True,
        )
        return
    if price_difference > 100:
        logger.warning(
            "Расхождение цены для пользователя %s: кэш=%s₽, пересчет=%s₽. Используем пересчитанную цену.",
            db_user.telegram_id,
            cached_total_price / 100,
            final_price / 100,
        )

    validation_total_price = calculated_total_before_promo

    period_label = "♾️" if tariff_code == TariffCode.WHITE.value else f"{period_days} дней"
    logger.info(
        "Расчет покупки подписки (%s) на %s (%s мес)",
        tariff_code,
        period_label,
        months_in_period,
    )
    if tariff_code != TariffCode.WHITE.value:
        base_log = f"   Период: {base_price_original / 100}₽"
        if base_discount_total and base_discount_total > 0:
            base_log += (
                f" → {base_price / 100}₽"
                f" (скидка {base_discount_percent}%: -{base_discount_total / 100}₽)"
            )
        logger.info(base_log)
    if total_traffic_price > 0:
        message = (
            f"   Трафик: {traffic_price_per_month / 100}₽/мес × {months_in_period}"
            f" = {total_traffic_price / 100}₽"
        )
        if traffic_discount_total > 0:
            message += (
                f" (скидка {traffic_discount_percent}%:"
                f" -{traffic_discount_total / 100}₽)"
            )
        logger.info(message)
    if total_servers_price > 0:
        message = (
            f"   Серверы: {countries_price_per_month / 100}₽/мес × {months_in_period}"
            f" = {total_servers_price / 100}₽"
        )
        if total_servers_discount > 0:
            message += (
                f" (скидка {servers_discount_percent}%:"
                f" -{total_servers_discount / 100}₽)"
            )
        logger.info(message)
    if total_devices_price > 0:
        message = (
            f"   Устройства: {devices_price_per_month / 100}₽/мес × {months_in_period}"
            f" = {total_devices_price / 100}₽"
        )
        if devices_discount_total > 0:
            message += (
                f" (скидка {devices_discount_percent}%:"
                f" -{devices_discount_total / 100}₽)"
            )
        logger.info(message)
    if promo_offer_discount_value > 0:
        logger.info(
            "   🎯 Промо-предложение: -%s₽ (%s%%)",
            promo_offer_discount_value / 100,
            promo_offer_discount_percent,
        )
    logger.info("   ИТОГО: %s₽", final_price / 100)

    if db_user.balance_kopeks < final_price:
        missing_kopeks = final_price - db_user.balance_kopeks
        message_text = texts.t(
            "ADDON_INSUFFICIENT_FUNDS_MESSAGE",
            (
                "⚠️ <b>Недостаточно средств</b>\n\n"
                "Стоимость услуги: {required}\n"
                "На балансе: {balance}\n"
                "Не хватает: {missing}\n\n"
                "Выберите способ пополнения. Сумма подставится автоматически."
            ),
        ).format(
            required=texts.format_price(final_price),
            balance=texts.format_price(db_user.balance_kopeks),
            missing=texts.format_price(missing_kopeks),
        )

        cart_data = {
            **data,
            "saved_cart": True,
            "missing_amount": missing_kopeks,
            "return_to_cart": True,
            "user_id": db_user.id,
        }
        await user_cart_service.save_user_cart(db_user.id, cart_data)

        await callback.message.edit_text(
            message_text,
            reply_markup=get_insufficient_balance_keyboard(
                db_user.language,
                resume_callback=resume_callback,
                amount_kopeks=missing_kopeks,
                has_saved_cart=True,
            ),
            parse_mode="HTML",
        )
        await callback.answer()
        return

    purchase_completed = False

    try:
        purchase_description = (
            "Покупка трафика (White)"
            if tariff_code == TariffCode.WHITE.value
            else f"Покупка подписки на {period_days} дней"
        )
        success = await subtract_user_balance(
            db,
            db_user,
            final_price,
            purchase_description,
            consume_promo_offer=promo_offer_discount_value > 0,
        )

        if not success:
            missing_kopeks = final_price - db_user.balance_kopeks
            message_text = texts.t(
                "ADDON_INSUFFICIENT_FUNDS_MESSAGE",
                (
                    "⚠️ <b>Недостаточно средств</b>\n\n"
                    "Стоимость услуги: {required}\n"
                    "На балансе: {balance}\n"
                    "Не хватает: {missing}\n\n"
                    "Выберите способ пополнения. Сумма подставится автоматически."
                ),
            ).format(
                required=texts.format_price(final_price),
                balance=texts.format_price(db_user.balance_kopeks),
                missing=texts.format_price(missing_kopeks),
            )

            await callback.message.edit_text(
                message_text,
                reply_markup=get_insufficient_balance_keyboard(
                    db_user.language,
                    resume_callback=resume_callback,
                    amount_kopeks=missing_kopeks,
                ),
                parse_mode="HTML",
            )
            await callback.answer()
            return

        existing_subscription = await subscription_crud.get_subscription_by_user_id(
            db,
            db_user.id,
            tariff_code=tariff_code,
        )

        if devices_selection_enabled:
            selected_devices = devices_selected
        else:
            selected_devices = forced_disabled_limit
        if tariff_code == TariffCode.WHITE.value:
            selected_devices = 0

        should_update_devices = selected_devices is not None

        was_trial_conversion = False
        current_time = datetime.utcnow()

        if existing_subscription:
            logger.info(
                "Обновляем существующую подписку пользователя %s (tariff=%s)",
                db_user.telegram_id,
                tariff_code,
            )

            bonus_period = timedelta()

            if existing_subscription.is_trial:
                logger.info(
                    "Конверсия из триала в платную для пользователя %s",
                    db_user.telegram_id,
                )
                was_trial_conversion = True

                trial_duration = (current_time - existing_subscription.start_date).days

                if settings.TRIAL_ADD_REMAINING_DAYS_TO_PAID and existing_subscription.end_date:
                    remaining_trial_delta = existing_subscription.end_date - current_time
                    if remaining_trial_delta.total_seconds() > 0:
                        bonus_period = remaining_trial_delta
                        logger.info(
                            "Добавляем оставшееся время триала (%s) к новой подписке пользователя %s",
                            bonus_period,
                            db_user.telegram_id,
                        )

                try:
                    from app.database.crud.subscription_conversion import create_subscription_conversion

                    await create_subscription_conversion(
                        db=db,
                        user_id=db_user.id,
                        trial_duration_days=trial_duration,
                        payment_method="balance",
                        first_payment_amount_kopeks=final_price,
                        first_paid_period_days=period_days,
                    )
                    logger.info(
                        "Записана конверсия: %s дн. триал → %s дн. платная за %s₽",
                        trial_duration,
                        period_days,
                        final_price / 100,
                    )
                except Exception as conversion_error:
                    logger.error("Ошибка записи конверсии: %s", conversion_error)

            existing_subscription.is_trial = False
            existing_subscription.status = SubscriptionStatus.ACTIVE.value
            if tariff_code == TariffCode.WHITE.value:
                traffic_gb = int(final_traffic_gb)
                if traffic_gb == 0:
                    existing_subscription.traffic_limit_gb = 0
                    existing_subscription.purchased_traffic_gb = 0
                else:
                    current_limit = int(existing_subscription.traffic_limit_gb or 0)
                    if current_limit != 0:
                        existing_subscription.traffic_limit_gb = current_limit + traffic_gb
                    current_purchased = int(getattr(existing_subscription, "purchased_traffic_gb", 0) or 0)
                    existing_subscription.purchased_traffic_gb = current_purchased + traffic_gb
            else:
                existing_subscription.traffic_limit_gb = final_traffic_gb
            if should_update_devices:
                existing_subscription.device_limit = selected_devices

            if not selected_countries:
                await callback.message.edit_text(
                    texts.t(
                        "COUNTRIES_MINIMUM_REQUIRED",
                        "❌ Нельзя отключить все страны. Должна быть подключена хотя бы одна страна.",
                    ),
                    reply_markup=get_back_keyboard(db_user.language),
                )
                await callback.answer()
                return

            existing_subscription.connected_squads = selected_countries

            extension_base_date = current_time
            if existing_subscription.end_date and existing_subscription.end_date > current_time:
                extension_base_date = existing_subscription.end_date
            else:
                existing_subscription.start_date = current_time

            existing_subscription.end_date = extension_base_date + timedelta(days=period_days) + bonus_period
            if tariff_code == TariffCode.WHITE.value:
                existing_subscription.end_date = _get_white_unlimited_end_date()
            existing_subscription.updated_at = current_time
            if tariff_code != TariffCode.WHITE.value:
                existing_subscription.traffic_used_gb = 0.0

            await db.commit()
            await db.refresh(existing_subscription)
            subscription = existing_subscription

        else:
            logger.info(
                "Создаем новую подписку для пользователя %s (tariff=%s)",
                db_user.telegram_id,
                tariff_code,
            )
            default_device_limit = getattr(settings, "DEFAULT_DEVICE_LIMIT", 1)
            resolved_device_limit = selected_devices

            if resolved_device_limit is None:
                if devices_selection_enabled:
                    resolved_device_limit = default_device_limit
                else:
                    resolved_device_limit = forced_disabled_limit or default_device_limit

            if not selected_countries:
                await callback.message.edit_text(
                    texts.t(
                        "COUNTRIES_MINIMUM_REQUIRED",
                        "❌ Нельзя отключить все страны. Должна быть подключена хотя бы одна страна.",
                    ),
                    reply_markup=get_back_keyboard(db_user.language),
                )
                await callback.answer()
                return

            subscription = await subscription_crud.create_paid_subscription(
                db=db,
                user_id=db_user.id,
                duration_days=period_days,
                traffic_limit_gb=final_traffic_gb,
                device_limit=resolved_device_limit,
                connected_squads=selected_countries,
                update_server_counters=False,
                tariff_code=tariff_code,
            )

            if tariff_code == TariffCode.WHITE.value:
                subscription.end_date = _get_white_unlimited_end_date()
                subscription.purchased_traffic_gb = 0 if int(final_traffic_gb) == 0 else int(final_traffic_gb)
                await db.commit()
                await db.refresh(subscription)

        from app.utils.user_utils import mark_user_as_had_paid_subscription

        await mark_user_as_had_paid_subscription(db, db_user)

        from app.database.crud.server_squad import add_user_to_servers, get_server_ids_by_uuids
        from app.database.crud.subscription import add_subscription_servers

        server_ids = await get_server_ids_by_uuids(db, selected_countries)
        if server_ids:
            if not server_prices or len(server_prices) != len(server_ids):
                server_prices = [0] * len(server_ids)
            await add_subscription_servers(db, subscription, server_ids, server_prices)
            await add_user_to_servers(db, server_ids)
            logger.info("Сохранены цены серверов за весь период: %s", server_prices)

        await db.refresh(db_user)

        subscription_service = SubscriptionService()

        if getattr(subscription, "remnawave_uuid", None):
            remnawave_user = await subscription_service.update_remnawave_user(
                db,
                subscription,
                reset_traffic=settings.RESET_TRAFFIC_ON_PAYMENT,
                reset_reason="покупка подписки",
            )
        else:
            remnawave_user = await subscription_service.create_remnawave_user(
                db,
                subscription,
                reset_traffic=settings.RESET_TRAFFIC_ON_PAYMENT,
                reset_reason="покупка подписки",
            )

        if not remnawave_user:
            logger.error(
                "Не удалось создать/обновить RemnaWave пользователя для %s",
                db_user.telegram_id,
            )
            remnawave_user = await subscription_service.create_remnawave_user(
                db,
                subscription,
                reset_traffic=settings.RESET_TRAFFIC_ON_PAYMENT,
                reset_reason="покупка подписки (повторная попытка)",
            )

        transaction_description = (
            "Подписка White"
            if tariff_code == TariffCode.WHITE.value
            else f"Подписка на {period_days} дней ({months_in_period} мес)"
        )
        transaction = await create_transaction(
            db=db,
            user_id=db_user.id,
            type=TransactionType.SUBSCRIPTION_PAYMENT,
            amount_kopeks=final_price,
            description=transaction_description,
        )

        try:
            notification_service = AdminNotificationService(callback.bot)
            await notification_service.send_subscription_purchase_notification(
                db,
                db_user,
                subscription,
                transaction,
                period_days,
                was_trial_conversion,
            )
        except Exception as error:
            logger.error("Ошибка отправки уведомления о покупке: %s", error)

        await db.refresh(db_user)
        await db.refresh(subscription)

        subscription_link = get_display_subscription_link(subscription)
        hide_subscription_link = settings.should_hide_subscription_link()

        discount_note = ""
        if promo_offer_discount_value > 0:
            discount_note = texts.t(
                "SUBSCRIPTION_PROMO_DISCOUNT_NOTE",
                "⚡ Доп. скидка {percent}%: -{amount}",
            ).format(
                percent=promo_offer_discount_percent,
                amount=texts.format_price(promo_offer_discount_value),
            )

        if remnawave_user and subscription_link:
            if settings.is_happ_cryptolink_mode():
                success_text = (
                    f"{texts.SUBSCRIPTION_PURCHASED}\n\n"
                    + texts.t(
                        "SUBSCRIPTION_HAPP_LINK_PROMPT",
                        "🔒 Ссылка на подписку создана. Нажмите кнопку \"Подключиться\" ниже, чтобы открыть её в Happ.",
                    )
                    + "\n\n"
                    + texts.t(
                        "SUBSCRIPTION_IMPORT_INSTRUCTION_PROMPT",
                        "📱 Нажмите кнопку ниже, чтобы получить инструкцию по настройке VPN на вашем устройстве",
                    )
                )
            elif hide_subscription_link:
                success_text = (
                    f"{texts.SUBSCRIPTION_PURCHASED}\n\n"
                    + texts.t(
                        "SUBSCRIPTION_LINK_HIDDEN_NOTICE",
                        "ℹ️ Ссылка подписки доступна по кнопкам ниже или в разделе \"Моя подписка\".",
                    )
                    + "\n\n"
                    + texts.t(
                        "SUBSCRIPTION_IMPORT_INSTRUCTION_PROMPT",
                        "📱 Нажмите кнопку ниже, чтобы получить инструкцию по настройке VPN на вашем устройстве",
                    )
                )
            else:
                import_link_section = texts.t(
                    "SUBSCRIPTION_IMPORT_LINK_SECTION",
                    "🔗 <b>Ваша ссылка для импорта в VPN приложение:</b>\n<code>{subscription_url}</code>",
                ).format(subscription_url=subscription_link)

                success_text = (
                    f"{texts.SUBSCRIPTION_PURCHASED}\n\n"
                    f"{import_link_section}\n\n"
                    f"{texts.t('SUBSCRIPTION_IMPORT_INSTRUCTION_PROMPT', '📱 Нажмите кнопку ниже, чтобы получить инструкцию по настройке VPN на вашем устройстве')}"
                )

            if discount_note:
                success_text = f"{success_text}\n\n{discount_note}"

            connect_mode = settings.CONNECT_BUTTON_MODE

            if connect_mode == "miniapp_subscription":
                connect_keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text=texts.t("CONNECT_BUTTON", "🔗 Подключиться"),
                                web_app=types.WebAppInfo(url=subscription_link),
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                text=texts.t("BACK_TO_MAIN_MENU_BUTTON", "⬅️ В главное меню"),
                                callback_data="back_to_menu",
                            )
                        ],
                    ]
                )
            elif connect_mode == "miniapp_custom":
                if not settings.MINIAPP_CUSTOM_URL:
                    await callback.answer(
                        texts.t(
                            "CUSTOM_MINIAPP_URL_NOT_SET",
                            "⚠ Кастомная ссылка для мини-приложения не настроена",
                        ),
                        show_alert=True,
                    )
                    return

                connect_keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text=texts.t("CONNECT_BUTTON", "🔗 Подключиться"),
                                web_app=types.WebAppInfo(url=settings.MINIAPP_CUSTOM_URL),
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                text=texts.t("BACK_TO_MAIN_MENU_BUTTON", "⬅️ В главное меню"),
                                callback_data="back_to_menu",
                            )
                        ],
                    ]
                )
            elif connect_mode == "link":
                rows = [
                    [
                        InlineKeyboardButton(
                            text=texts.t("CONNECT_BUTTON", "🔗 Подключиться"),
                            url=subscription_link,
                        )
                    ]
                ]
                happ_row = get_happ_download_button_row(texts)
                if happ_row:
                    rows.append(happ_row)
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=texts.t("BACK_TO_MAIN_MENU_BUTTON", "⬅️ В главное меню"),
                            callback_data="back_to_menu",
                        )
                    ]
                )
                connect_keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
            elif connect_mode == "happ_cryptolink":
                rows = [
                    [
                        InlineKeyboardButton(
                            text=texts.t("CONNECT_BUTTON", "🔗 Подключиться"),
                            callback_data="open_subscription_link",
                        )
                    ]
                ]
                happ_row = get_happ_download_button_row(texts)
                if happ_row:
                    rows.append(happ_row)
                rows.append(
                    [
                        InlineKeyboardButton(
                            text=texts.t("BACK_TO_MAIN_MENU_BUTTON", "⬅️ В главное меню"),
                            callback_data="back_to_menu",
                        )
                    ]
                )
                connect_keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
            else:
                connect_keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text=texts.t("CONNECT_BUTTON", "🔗 Подключиться"),
                                callback_data="subscription_connect",
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                text=texts.t("BACK_TO_MAIN_MENU_BUTTON", "⬅️ В главное меню"),
                                callback_data="back_to_menu",
                            )
                        ],
                    ]
                )

            await callback.message.edit_text(
                success_text,
                reply_markup=connect_keyboard,
                parse_mode="HTML",
            )
        else:
            purchase_text = texts.SUBSCRIPTION_PURCHASED
            if discount_note:
                purchase_text = f"{purchase_text}\n\n{discount_note}"
            await callback.message.edit_text(
                texts.t(
                    "SUBSCRIPTION_LINK_GENERATING_NOTICE",
                    "{purchase_text}\n\nСсылка генерируется, перейдите в раздел 'Моя подписка' через несколько секунд.",
                ).format(purchase_text=purchase_text),
                reply_markup=get_back_keyboard(db_user.language),
            )

        purchase_completed = True
        logger.info(
            "Пользователь %s купил подписку (%s) на %s дней за %s₽",
            db_user.telegram_id,
            tariff_code,
            period_days,
            final_price / 100,
        )

    except Exception as error:
        logger.error("Ошибка покупки подписки: %s", error)
        await callback.message.edit_text(
            texts.ERROR,
            reply_markup=get_back_keyboard(db_user.language),
        )

    if purchase_completed:
        await clear_subscription_checkout_draft(db_user.id)
        await user_cart_service.delete_user_cart(db_user.id)

    await state.clear()
    await callback.answer()

async def handle_subscription_config_back(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user,
    db: AsyncSession,
):
    data = await state.get_data()
    if data.get("spiderman_extend_mode"):
        from app.handlers.subscription.purchase import show_subscription_info
        await show_subscription_info(callback, db_user, db)
        if state is not None:
            await state.clear()
        await callback.answer()
        return

    tariff_raw = data.get("tariff_code")
    if not tariff_raw:
        return await _ORIGINAL_HANDLE_CONFIG_BACK(callback, state, db_user, db)

    tariff_code = normalize_tariff_code(tariff_raw)
    if tariff_code not in (TariffCode.STANDARD.value, TariffCode.WHITE.value):
        return await _ORIGINAL_HANDLE_CONFIG_BACK(callback, state, db_user, db)

    current_state = await state.get_state()
    texts = get_texts(db_user.language)

    if current_state == SubscriptionStates.selecting_period.state:
        await _show_tariff_selection(callback, state, db_user, db)
    elif current_state == SubscriptionStates.selecting_traffic.state:
        if tariff_code == TariffCode.WHITE.value:
            await _show_tariff_selection(callback, state, db_user, db)
        else:
            await _show_period_selection(callback, state, db_user, db)
    elif current_state == SubscriptionStates.selecting_devices.state:
        if tariff_code == TariffCode.WHITE.value:
            await callback.message.edit_text(
                texts.SELECT_TRAFFIC,
                reply_markup=_build_tariff_traffic_keyboard(db_user.language),
            )
            await state.set_state(SubscriptionStates.selecting_traffic)
        else:
            await _show_period_selection(callback, state, db_user, db)
    elif current_state == SubscriptionStates.confirming_purchase.state:
        if tariff_code == TariffCode.WHITE.value:
            await callback.message.edit_text(
                texts.SELECT_TRAFFIC,
                reply_markup=_build_tariff_traffic_keyboard(db_user.language),
            )
            await state.set_state(SubscriptionStates.selecting_traffic)
        elif settings.is_devices_selection_enabled():
            selected_devices = data.get("devices", settings.DEFAULT_DEVICE_LIMIT)
            await callback.message.edit_text(
                texts.SELECT_DEVICES,
                reply_markup=get_devices_keyboard(selected_devices, db_user.language),
            )
            await state.set_state(SubscriptionStates.selecting_devices)
        else:
            await _show_period_selection(callback, state, db_user, db)
    else:
        from app.handlers.menu import show_main_menu

        await show_main_menu(callback, db_user, db)
        await state.clear()

    await callback.answer()


def register_handlers(dp):
    import app.handlers.subscription.purchase as purchase

    if not hasattr(purchase, "extend_standard_back"):
        async def extend_standard_back(
            callback: types.CallbackQuery,
            state: FSMContext,
            db_user,
            db: AsyncSession,
        ):
            if state is not None:
                await state.clear()
            from app.handlers.subscription.purchase import show_subscription_info

            await show_subscription_info(callback, db_user, db)
            await callback.answer()

        purchase.extend_standard_back = extend_standard_back

    _ORIGINAL_REGISTER_HANDLERS(dp)
    dp.callback_query.register(
        select_tariff,
        F.data.in_(["tariff_standard", "tariff_white"]),
    )


def apply_subscription_purchase_patches() -> None:
    import app.handlers.subscription.autopay as autopay
    import app.handlers.subscription.pricing as pricing
    import app.handlers.subscription.purchase as purchase
    import app.handlers.subscription.traffic as traffic
    import app.handlers.subscription as subscription_pkg

    if getattr(purchase, "_spiderman_tariff_purchase_patched", False):
        return

    global _ORIGINAL_START_PURCHASE
    global _ORIGINAL_SELECT_PERIOD
    global _ORIGINAL_SELECT_TRAFFIC
    global _ORIGINAL_CONFIRM_PURCHASE
    global _ORIGINAL_PREPARE_SUMMARY
    global _ORIGINAL_HANDLE_CONFIG_BACK
    global _ORIGINAL_REGISTER_HANDLERS

    _ORIGINAL_START_PURCHASE = purchase.start_subscription_purchase
    _ORIGINAL_SELECT_PERIOD = purchase.select_period
    _ORIGINAL_CONFIRM_PURCHASE = purchase.confirm_purchase
    _ORIGINAL_REGISTER_HANDLERS = purchase.register_handlers
    _ORIGINAL_SELECT_TRAFFIC = traffic.select_traffic
    _ORIGINAL_PREPARE_SUMMARY = pricing._prepare_subscription_summary
    _ORIGINAL_HANDLE_CONFIG_BACK = autopay.handle_subscription_config_back

    purchase.start_subscription_purchase = start_subscription_purchase
    purchase.select_period = select_period
    purchase.confirm_purchase = confirm_purchase
    purchase.select_traffic = select_traffic
    purchase._prepare_subscription_summary = _prepare_subscription_summary
    purchase.register_handlers = register_handlers
    traffic.select_traffic = select_traffic
    pricing._prepare_subscription_summary = _prepare_subscription_summary
    autopay.handle_subscription_config_back = handle_subscription_config_back
    purchase.handle_subscription_config_back = handle_subscription_config_back

    subscription_pkg.start_subscription_purchase = start_subscription_purchase
    subscription_pkg.select_period = select_period
    subscription_pkg.confirm_purchase = confirm_purchase
    subscription_pkg.select_traffic = select_traffic
    subscription_pkg.handle_subscription_config_back = handle_subscription_config_back
    subscription_pkg.register_handlers = register_handlers

    purchase._spiderman_tariff_purchase_patched = True
