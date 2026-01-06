import logging

from aiogram import types
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import set_committed_value

from app.database.models import User
from app.spiderman.tariff_context import TariffCode
from app.spiderman.subscription_crud_patch import get_subscription_by_user_id
from app.keyboards import inline as inline_kb

logger = logging.getLogger(__name__)


async def _run_with_standard_subscription(callback: types.CallbackQuery, db_user: User, db: AsyncSession, handler):
    subscription = await get_subscription_by_user_id(
        db,
        db_user.id,
        tariff_code=TariffCode.STANDARD.value,
    )
    if not subscription:
        await callback.answer("Подписка Standard не найдена", show_alert=True)
        return

    original_subscription = getattr(db_user, "subscription", None)
    original_uuid = getattr(db_user, "remnawave_uuid", None)

    user_uuid = getattr(db_user, "remnawave_uuid", None)
    effective_uuid = getattr(subscription, "remnawave_uuid", None) or user_uuid

    set_committed_value(db_user, "subscription", subscription)
    set_committed_value(db_user, "remnawave_uuid", effective_uuid)

    try:
        return await handler(callback, db_user, db)
    finally:
        set_committed_value(db_user, "subscription", original_subscription)
        set_committed_value(db_user, "remnawave_uuid", original_uuid)


async def _run_with_standard_subscription_monthly_pricing(
    callback: types.CallbackQuery,
    db_user: User,
    db: AsyncSession,
    handler,
):
    import app.handlers.subscription.devices as devices

    original_calc = devices.calculate_prorated_price
    original_remaining = devices.get_remaining_months

    def _monthly_price(price_per_month, _end_date):
        return price_per_month, 1

    def _one_month(_end_date):
        return 1

    devices.calculate_prorated_price = _monthly_price
    devices.get_remaining_months = _one_month
    try:
        return await _run_with_standard_subscription(callback, db_user, db, handler)
    finally:
        devices.calculate_prorated_price = original_calc
        devices.get_remaining_months = original_remaining


_ORIGINAL_GET_CHANGE_DEVICES_KEYBOARD = inline_kb.get_change_devices_keyboard
_ORIGINAL_GET_CONFIRM_CHANGE_DEVICES_KEYBOARD = inline_kb.get_confirm_change_devices_keyboard


def _swap_keyboard_back_callback(markup: types.InlineKeyboardMarkup, target_callback: str) -> None:
    if not markup or not markup.inline_keyboard:
        return
    last_row = markup.inline_keyboard[-1]
    if not last_row:
        return
    for button in last_row:
        if button.callback_data in {"subscription_settings", "subscription_config_back"}:
            button.callback_data = target_callback


def _patched_get_change_devices_keyboard(
    current_devices: int,
    language: str = inline_kb.DEFAULT_LANGUAGE,
    subscription_end_date=None,
    discount_percent: int = 0,
) -> types.InlineKeyboardMarkup:
    keyboard = _ORIGINAL_GET_CHANGE_DEVICES_KEYBOARD(
        current_devices,
        language,
        None,
        discount_percent,
    )
    _swap_keyboard_back_callback(keyboard, "menu_subscription")
    return keyboard


def _patched_get_confirm_change_devices_keyboard(
    new_devices_count: int,
    price: int,
    language: str = inline_kb.DEFAULT_LANGUAGE,
) -> types.InlineKeyboardMarkup:
    keyboard = _ORIGINAL_GET_CONFIRM_CHANGE_DEVICES_KEYBOARD(new_devices_count, price, language)
    _swap_keyboard_back_callback(keyboard, "menu_subscription")
    return keyboard


def apply_subscription_devices_patches() -> None:
    import app.handlers.subscription.devices as devices
    import app.handlers.subscription.purchase as purchase
    import app.handlers.subscription as subscription_pkg

    if getattr(devices, "_spiderman_standard_devices_patched", False):
        return

    async def handle_change_devices(callback: types.CallbackQuery, db_user: User, db: AsyncSession):
        return await _run_with_standard_subscription(callback, db_user, db, _ORIGINAL_HANDLE_CHANGE_DEVICES)

    async def confirm_change_devices(callback: types.CallbackQuery, db_user: User, db: AsyncSession):
        return await _run_with_standard_subscription_monthly_pricing(
            callback,
            db_user,
            db,
            _ORIGINAL_CONFIRM_CHANGE_DEVICES,
        )

    async def handle_device_management(callback: types.CallbackQuery, db_user: User, db: AsyncSession):
        return await _run_with_standard_subscription(callback, db_user, db, _ORIGINAL_HANDLE_DEVICE_MANAGEMENT)

    async def handle_devices_page(callback: types.CallbackQuery, db_user: User, db: AsyncSession):
        return await _run_with_standard_subscription(callback, db_user, db, _ORIGINAL_HANDLE_DEVICES_PAGE)

    async def handle_single_device_reset(callback: types.CallbackQuery, db_user: User, db: AsyncSession):
        return await _run_with_standard_subscription(callback, db_user, db, _ORIGINAL_HANDLE_SINGLE_DEVICE_RESET)

    async def handle_all_devices_reset_from_management(callback: types.CallbackQuery, db_user: User, db: AsyncSession):
        return await _run_with_standard_subscription(callback, db_user, db, _ORIGINAL_HANDLE_ALL_DEVICES_RESET)

    async def show_device_connection_help(callback: types.CallbackQuery, db_user: User, db: AsyncSession):
        return await _run_with_standard_subscription(callback, db_user, db, _ORIGINAL_SHOW_DEVICE_CONNECTION_HELP)

    async def handle_reset_devices(callback: types.CallbackQuery, db_user: User, db: AsyncSession):
        return await _run_with_standard_subscription(callback, db_user, db, _ORIGINAL_HANDLE_RESET_DEVICES)

    async def execute_change_devices(callback: types.CallbackQuery, db_user: User, db: AsyncSession):
        return await _run_with_standard_subscription_monthly_pricing(
            callback,
            db_user,
            db,
            _ORIGINAL_EXECUTE_CHANGE_DEVICES,
        )

    global _ORIGINAL_HANDLE_CHANGE_DEVICES
    global _ORIGINAL_CONFIRM_CHANGE_DEVICES
    global _ORIGINAL_HANDLE_DEVICE_MANAGEMENT
    global _ORIGINAL_HANDLE_DEVICES_PAGE
    global _ORIGINAL_HANDLE_SINGLE_DEVICE_RESET
    global _ORIGINAL_HANDLE_ALL_DEVICES_RESET
    global _ORIGINAL_SHOW_DEVICE_CONNECTION_HELP
    global _ORIGINAL_HANDLE_RESET_DEVICES
    global _ORIGINAL_EXECUTE_CHANGE_DEVICES

    _ORIGINAL_HANDLE_CHANGE_DEVICES = devices.handle_change_devices
    _ORIGINAL_CONFIRM_CHANGE_DEVICES = devices.confirm_change_devices
    _ORIGINAL_HANDLE_DEVICE_MANAGEMENT = devices.handle_device_management
    _ORIGINAL_HANDLE_DEVICES_PAGE = devices.handle_devices_page
    _ORIGINAL_HANDLE_SINGLE_DEVICE_RESET = devices.handle_single_device_reset
    _ORIGINAL_HANDLE_ALL_DEVICES_RESET = devices.handle_all_devices_reset_from_management
    _ORIGINAL_SHOW_DEVICE_CONNECTION_HELP = devices.show_device_connection_help
    _ORIGINAL_HANDLE_RESET_DEVICES = devices.handle_reset_devices
    _ORIGINAL_EXECUTE_CHANGE_DEVICES = devices.execute_change_devices

    devices.handle_change_devices = handle_change_devices
    devices.confirm_change_devices = confirm_change_devices
    devices.handle_device_management = handle_device_management
    devices.handle_devices_page = handle_devices_page
    devices.handle_single_device_reset = handle_single_device_reset
    devices.handle_all_devices_reset_from_management = handle_all_devices_reset_from_management
    devices.show_device_connection_help = show_device_connection_help
    devices.handle_reset_devices = handle_reset_devices
    devices.execute_change_devices = execute_change_devices

    purchase.handle_change_devices = handle_change_devices
    purchase.confirm_change_devices = confirm_change_devices
    purchase.handle_device_management = handle_device_management
    purchase.handle_devices_page = handle_devices_page
    purchase.handle_single_device_reset = handle_single_device_reset
    purchase.handle_all_devices_reset_from_management = handle_all_devices_reset_from_management
    purchase.show_device_connection_help = show_device_connection_help
    purchase.handle_reset_devices = handle_reset_devices
    purchase.execute_change_devices = execute_change_devices

    subscription_pkg.handle_change_devices = handle_change_devices
    subscription_pkg.confirm_change_devices = confirm_change_devices
    subscription_pkg.handle_device_management = handle_device_management
    subscription_pkg.handle_devices_page = handle_devices_page
    subscription_pkg.handle_single_device_reset = handle_single_device_reset
    subscription_pkg.handle_all_devices_reset_from_management = handle_all_devices_reset_from_management
    subscription_pkg.show_device_connection_help = show_device_connection_help
    subscription_pkg.handle_reset_devices = handle_reset_devices
    subscription_pkg.execute_change_devices = execute_change_devices

    devices.get_change_devices_keyboard = _patched_get_change_devices_keyboard
    devices.get_confirm_change_devices_keyboard = _patched_get_confirm_change_devices_keyboard

    devices._spiderman_standard_devices_patched = True
