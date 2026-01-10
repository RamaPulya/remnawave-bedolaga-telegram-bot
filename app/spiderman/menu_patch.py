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


def _detach_buttons_by_predicate(
    keyboard: InlineKeyboardMarkup,
    predicate,
) -> list[InlineKeyboardButton]:
    extracted: list[InlineKeyboardButton] = []

    row_index = 0
    while row_index < len(keyboard.inline_keyboard):
        row = keyboard.inline_keyboard[row_index]
        button_index = 0
        while button_index < len(row):
            button = row[button_index]
            if predicate(button):
                extracted.append(button)
                row.pop(button_index)
                continue
            button_index += 1

        if not row:
            keyboard.inline_keyboard.pop(row_index)
            continue
        row_index += 1

    return extracted


def _sort_connect_buttons(buttons: list[InlineKeyboardButton]) -> list[InlineKeyboardButton]:
    def _priority(btn: InlineKeyboardButton) -> int:
        text = (getattr(btn, "text", "") or "").lower()
        callback_data = (getattr(btn, "callback_data", "") or "").lower()
        if "white" in callback_data or "⚪" in text:
            return 1
        return 0

    return sorted(buttons, key=_priority)


def _detach_connect_buttons(
    keyboard: InlineKeyboardMarkup,
    language: str,
) -> list[InlineKeyboardButton]:
    texts = get_texts(language)
    connect_text = texts.t("CONNECT_BUTTON", "🔗 Подключиться")
    connect_text_lower = (connect_text or "").strip().lower()

    def _is_connect_button(button: InlineKeyboardButton) -> bool:
        callback_data = getattr(button, "callback_data", None)
        if callback_data in {"subscription_connect", "open_subscription_link", "open_subscription_link_white"}:
            return True

        text = (getattr(button, "text", "") or "").strip().lower()
        if not text:
            return False

        if "подключ" in text or "connect" in text:
            return True

        if connect_text_lower and connect_text_lower in text:
            return True

        return False

    extracted = _detach_buttons_by_predicate(keyboard, _is_connect_button)
    return _sort_connect_buttons(extracted)[:2]


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
    target = _find_row_with_button(keyboard, "menu_subscription")
    if target is None:
        target = _find_row_with_button(keyboard, "menu_trial")

    if target is None:
        return keyboard

    buy_button = _detach_button(keyboard, "menu_buy")
    if buy_button is None:
        texts = get_texts(language)
        buy_button = InlineKeyboardButton(
            text=texts.t("MENU_BUY_SUBSCRIPTION", "Buy subscription"),
            callback_data="menu_buy",
        )

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


def _rebuild_main_menu_layout(
    keyboard: InlineKeyboardMarkup,
    language: str,
) -> InlineKeyboardMarkup:
    connect_buttons = _detach_connect_buttons(keyboard, language)

    happ_button = _detach_button(keyboard, "subscription_happ_download")
    balance_button = _detach_button(keyboard, "menu_balance")
    subscription_button = _detach_button(keyboard, "menu_subscription")
    trial_button = _detach_button(keyboard, "menu_trial")
    buy_button = _detach_button(keyboard, "menu_buy")
    promo_button = _detach_button(keyboard, "menu_promocode")
    partner_button = _detach_button(keyboard, "menu_referrals")
    language_button = _detach_button(keyboard, "menu_language")
    info_button = _detach_button(keyboard, "menu_info")
    support_button = _detach_button(keyboard, "menu_support")
    admin_button = _detach_button(keyboard, "admin_panel")
    moderator_button = _detach_button(keyboard, "moderator_panel")

    extra_buttons: list[InlineKeyboardButton] = []
    for row in keyboard.inline_keyboard:
        extra_buttons.extend(row)

    rows: list[list[InlineKeyboardButton]] = []
    if connect_buttons:
        rows.append(connect_buttons)
    if happ_button is not None:
        rows.append([happ_button])
    if balance_button is not None:
        rows.append([balance_button])

    subscription_row: list[InlineKeyboardButton] = []
    left_subscription_button = subscription_button or trial_button
    if left_subscription_button is not None:
        subscription_row.append(left_subscription_button)
    if buy_button is not None:
        subscription_row.append(buy_button)
    if subscription_row:
        rows.append(subscription_row)

    promo_row = [button for button in (promo_button, partner_button) if button is not None]
    if promo_row:
        rows.append(promo_row)

    language_row: list[InlineKeyboardButton] = []
    if language_button is not None:
        language_row.append(language_button)
    if info_button is not None:
        language_row.append(info_button)
    if language_row:
        rows.append(language_row)

    for i in range(0, len(extra_buttons), 2):
        rows.append(extra_buttons[i : i + 2])

    if support_button is not None:
        rows.append([support_button])
    if admin_button is not None:
        rows.append([admin_button])
    if moderator_button is not None:
        rows.append([moderator_button])

    if rows:
        keyboard.inline_keyboard = rows

    return keyboard


async def get_main_menu_keyboard_async_patched(*args, **kwargs) -> InlineKeyboardMarkup:
    keyboard = await _ORIGINAL_GET_MAIN_MENU_KEYBOARD_ASYNC(*args, **kwargs)
    if settings.SPIDERMAN_HIDE_BUY_TRAFFIC:
        _remove_buy_traffic_button(keyboard)
    language = _extract_language(args, kwargs)
    if settings.SPIDERMAN_ALWAYS_SHOW_BUY_SUBSCRIPTION:
        keyboard = _append_buy_button(keyboard, language)
    return _rebuild_main_menu_layout(keyboard, language)

def get_main_menu_keyboard_patched(*args, **kwargs) -> InlineKeyboardMarkup:
    keyboard = _ORIGINAL_GET_MAIN_MENU_KEYBOARD(*args, **kwargs)
    if settings.SPIDERMAN_HIDE_BUY_TRAFFIC:
        _remove_buy_traffic_button(keyboard)
    language = _extract_language(args, kwargs)
    if settings.SPIDERMAN_ALWAYS_SHOW_BUY_SUBSCRIPTION:
        keyboard = _append_buy_button(keyboard, language)
    return _rebuild_main_menu_layout(keyboard, language)

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
