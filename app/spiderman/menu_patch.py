from typing import Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.config import settings
from app.localization.texts import get_texts

_ORIGINAL_GET_MAIN_MENU_KEYBOARD_ASYNC = None
_ORIGINAL_GET_MAIN_MENU_KEYBOARD = None
_ORIGINAL_EVALUATE_MENU_CONDITIONS = None


def _extract_language(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    language = kwargs.get("language")
    if isinstance(language, str) and language:
        return language
    if len(args) >= 2 and isinstance(args[1], str) and args[1]:
        return args[1]
    user = kwargs.get("user")
    if user is not None:
        user_language = getattr(user, "language", None)
        if isinstance(user_language, str) and user_language:
            return user_language
    return settings.DEFAULT_LANGUAGE


def _has_buy_button(keyboard: InlineKeyboardMarkup) -> bool:
    for row in keyboard.inline_keyboard:
        for button in row:
            if getattr(button, "callback_data", None) == "menu_buy":
                return True
    return False


def _detach_button(
    keyboard: InlineKeyboardMarkup,
    callback_data: str,
) -> InlineKeyboardButton | None:
    for row_index, row in enumerate(keyboard.inline_keyboard):
        for button_index, button in enumerate(row):
            if getattr(button, "callback_data", None) == callback_data:
                row.pop(button_index)
                if not row:
                    keyboard.inline_keyboard.pop(row_index)
                return button
    return None


def _detach_button_by_text(
    keyboard: InlineKeyboardMarkup,
    text: str,
) -> InlineKeyboardButton | None:
    for row_index, row in enumerate(keyboard.inline_keyboard):
        for button_index, button in enumerate(row):
            if getattr(button, "text", None) == text:
                row.pop(button_index)
                if not row:
                    keyboard.inline_keyboard.pop(row_index)
                return button
    return None


def _find_row_with_button(
    keyboard: InlineKeyboardMarkup,
    callback_data: str,
) -> tuple[int, int] | None:
    for row_index, row in enumerate(keyboard.inline_keyboard):
        for button_index, button in enumerate(row):
            if getattr(button, "callback_data", None) == callback_data:
                return row_index, button_index
    return None


def _place_buy_button_near_subscription(
    keyboard: InlineKeyboardMarkup,
    language: str,
) -> InlineKeyboardMarkup:
    buy_button = _detach_button(keyboard, "menu_buy")
    if buy_button is None:
        texts = get_texts(language)
        buy_button = InlineKeyboardButton(
            text=texts.t("MENU_BUY_SUBSCRIPTION", "Buy subscription"),
            callback_data="menu_buy",
        )

    target = _find_row_with_button(keyboard, "menu_subscription")
    if target is None:
        target = _find_row_with_button(keyboard, "menu_trial")

    if target is None:
        keyboard.inline_keyboard.append([buy_button])
        return keyboard

    row_index, target_index = target
    row = keyboard.inline_keyboard[row_index]

    if any(getattr(button, "callback_data", None) == "menu_buy" for button in row):
        return keyboard

    if len(row) < 2:
        row.append(buy_button)
        return keyboard

    target_button = row[target_index]
    other_buttons = [button for index, button in enumerate(row) if index != target_index]
    row[:] = [target_button, buy_button]

    insert_index = row_index + 1
    for i in range(0, len(other_buttons), 2):
        keyboard.inline_keyboard.insert(insert_index, other_buttons[i : i + 2])
        insert_index += 1

    return keyboard


def _append_buy_button(keyboard: InlineKeyboardMarkup, language: str) -> InlineKeyboardMarkup:
    if not _has_buy_button(keyboard):
        texts = get_texts(language)
        keyboard.inline_keyboard.append(
            [InlineKeyboardButton(text=texts.t("MENU_BUY_SUBSCRIPTION", "Buy subscription"), callback_data="menu_buy")]
        )
    return _place_buy_button_near_subscription(keyboard, language)


def _remove_buy_traffic_button(keyboard: InlineKeyboardMarkup) -> InlineKeyboardMarkup:
    _detach_button(keyboard, "buy_traffic")
    return keyboard


def _reorder_menu_for_active_subscription(
    keyboard: InlineKeyboardMarkup,
    language: str,
) -> InlineKeyboardMarkup:
    if _find_row_with_button(keyboard, "menu_subscription") is None:
        return keyboard

    texts = get_texts(language)
    connect_text = texts.t("CONNECT_BUTTON", "🔗 Подключиться")

    connect_button = _detach_button(keyboard, "subscription_connect")
    if connect_button is None:
        connect_button = _detach_button_by_text(keyboard, connect_text)

    balance_button = _detach_button(keyboard, "menu_balance")
    subscription_button = _detach_button(keyboard, "menu_subscription")
    buy_button = _detach_button(keyboard, "menu_buy")
    promo_button = _detach_button(keyboard, "menu_promocode")
    partner_button = _detach_button(keyboard, "menu_referrals")
    language_button = _detach_button(keyboard, "menu_language")
    info_button = _detach_button(keyboard, "menu_info")
    support_button = _detach_button(keyboard, "menu_support")

    rows: list[list[InlineKeyboardButton]] = []
    if connect_button is not None:
        rows.append([connect_button])
    if balance_button is not None:
        rows.append([balance_button])
    if subscription_button is not None or buy_button is not None:
        row = [button for button in (subscription_button, buy_button) if button is not None]
        if row:
            rows.append(row)
    row = [button for button in (promo_button, partner_button) if button is not None]
    if row:
        rows.append(row)
    row = [button for button in (language_button, info_button) if button is not None]
    if row:
        rows.append(row)
    if support_button is not None:
        rows.append([support_button])

    if rows:
        keyboard.inline_keyboard = rows

    return keyboard


async def get_main_menu_keyboard_async_patched(*args, **kwargs) -> InlineKeyboardMarkup:
    keyboard = await _ORIGINAL_GET_MAIN_MENU_KEYBOARD_ASYNC(*args, **kwargs)
    if settings.SPIDERMAN_HIDE_BUY_TRAFFIC:
        _remove_buy_traffic_button(keyboard)
    if not settings.SPIDERMAN_ALWAYS_SHOW_BUY_SUBSCRIPTION:
        return keyboard
    language = _extract_language(args, kwargs)
    keyboard = _append_buy_button(keyboard, language)
    return _reorder_menu_for_active_subscription(keyboard, language)

def get_main_menu_keyboard_patched(*args, **kwargs) -> InlineKeyboardMarkup:
    keyboard = _ORIGINAL_GET_MAIN_MENU_KEYBOARD(*args, **kwargs)
    if settings.SPIDERMAN_HIDE_BUY_TRAFFIC:
        _remove_buy_traffic_button(keyboard)
    if not settings.SPIDERMAN_ALWAYS_SHOW_BUY_SUBSCRIPTION:
        return keyboard
    language = _extract_language(args, kwargs)
    keyboard = _append_buy_button(keyboard, language)
    return _reorder_menu_for_active_subscription(keyboard, language)

def _evaluate_menu_conditions_patched(conditions, context) -> bool:
    if (
        settings.SPIDERMAN_ALWAYS_SHOW_BUY_SUBSCRIPTION
        and isinstance(conditions, dict)
        and conditions.get("show_buy") is True
    ):
        patched_conditions = dict(conditions)
        patched_conditions.pop("show_buy", None)
        if not patched_conditions:
            return True
        return _ORIGINAL_EVALUATE_MENU_CONDITIONS(patched_conditions, context)
    return _ORIGINAL_EVALUATE_MENU_CONDITIONS(conditions, context)


def apply_menu_patches() -> None:
    import app.keyboards.inline as inline_keyboards
    from app.services.menu_layout.service import MenuLayoutService

    if getattr(inline_keyboards, "_spiderman_menu_patched", False):
        return

    global _ORIGINAL_GET_MAIN_MENU_KEYBOARD_ASYNC
    global _ORIGINAL_GET_MAIN_MENU_KEYBOARD
    global _ORIGINAL_EVALUATE_MENU_CONDITIONS
    _ORIGINAL_GET_MAIN_MENU_KEYBOARD_ASYNC = inline_keyboards.get_main_menu_keyboard_async
    _ORIGINAL_GET_MAIN_MENU_KEYBOARD = inline_keyboards.get_main_menu_keyboard
    _ORIGINAL_EVALUATE_MENU_CONDITIONS = MenuLayoutService._evaluate_conditions
    inline_keyboards.get_main_menu_keyboard_async = get_main_menu_keyboard_async_patched
    inline_keyboards.get_main_menu_keyboard = get_main_menu_keyboard_patched
    MenuLayoutService._evaluate_conditions = _evaluate_menu_conditions_patched
    inline_keyboards._spiderman_menu_patched = True
