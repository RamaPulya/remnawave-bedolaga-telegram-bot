import inspect
import logging
from datetime import datetime
from typing import Any, Awaitable, Callable, Optional, Tuple

from aiogram import types
from aiogram.fsm.context import FSMContext
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database.crud.user import get_user_by_id
from app.database.models import Subscription, SubscriptionStatus, User, UserStatus
from app.localization.texts import get_texts
from app.services.user_service import UserService
from app.spiderman.tariff_context import TariffCode, normalize_tariff_code, use_tariff_code

logger = logging.getLogger(__name__)

_PATCHED = False

_STATE_SELECTED_USER_ID = "spiderman_admin_selected_user_id"
_STATE_SELECTED_TARIFF = "spiderman_admin_selected_tariff"


def _format_tariff_label(tariff_code: str) -> str:
    tariff_code = normalize_tariff_code(tariff_code)
    return "White" if tariff_code == TariffCode.WHITE.value else "Standard"


async def _set_selected_tariff(
    state: Optional[FSMContext],
    *,
    user_id: int,
    tariff_code: str,
) -> None:
    if not state:
        return
    await state.update_data(
        **{
            _STATE_SELECTED_USER_ID: user_id,
            _STATE_SELECTED_TARIFF: normalize_tariff_code(tariff_code),
        }
    )


async def _get_selected_tariff_for_user(
    state: Optional[FSMContext],
    *,
    user_id: Optional[int],
) -> str:
    if not state:
        return TariffCode.STANDARD.value

    data = await state.get_data()
    selected_user_id = data.get(_STATE_SELECTED_USER_ID)
    selected_tariff = normalize_tariff_code(data.get(_STATE_SELECTED_TARIFF))

    if user_id is None:
        return selected_tariff
    if selected_user_id == user_id:
        return selected_tariff
    return TariffCode.STANDARD.value


async def _get_selected_tariff_for_callback(
    state: Optional[FSMContext],
    *,
    callback_data: str,
) -> str:
    if not state:
        return TariffCode.STANDARD.value

    data = await state.get_data()
    selected_user_id = data.get(_STATE_SELECTED_USER_ID)
    selected_tariff = normalize_tariff_code(data.get(_STATE_SELECTED_TARIFF))

    if not selected_user_id:
        return TariffCode.STANDARD.value

    user_id_token = str(selected_user_id)
    if callback_data.endswith(user_id_token) or f"_{user_id_token}_" in callback_data or f"_{user_id_token}" in callback_data:
        return selected_tariff

    return TariffCode.STANDARD.value


def _call_supported_kwargs(func: Callable[..., Any], **kwargs: Any) -> dict:
    params = inspect.signature(func).parameters
    return {key: value for key, value in kwargs.items() if key in params}


def _wrap_callback_handler_with_tariff(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    async def wrapped(
        callback: types.CallbackQuery,
        db_user: User,
        db: Optional[AsyncSession] = None,
        state: Optional[FSMContext] = None,
    ) -> Any:
        tariff_code = await _get_selected_tariff_for_callback(
            state,
            callback_data=str(callback.data),
        )
        with use_tariff_code(tariff_code):
            return await func(**_call_supported_kwargs(func, callback=callback, db_user=db_user, db=db, state=state))

    return wrapped


def _wrap_message_handler_with_tariff(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    async def wrapped(
        message: types.Message,
        db_user: User,
        state: FSMContext,
        db: Optional[AsyncSession] = None,
    ) -> Any:
        data = await state.get_data()
        user_id: Optional[int] = None
        for key in (
            "editing_devices_user_id",
            "editing_traffic_user_id",
            "extending_user_id",
            "adding_traffic_user_id",
            "granting_user_id",
        ):
            if data.get(key):
                try:
                    user_id = int(data.get(key))
                    break
                except Exception:
                    pass

        tariff_code = await _get_selected_tariff_for_user(state, user_id=user_id)
        with use_tariff_code(tariff_code):
            return await func(**_call_supported_kwargs(func, message=message, db_user=db_user, state=state, db=db))

    return wrapped


async def _get_subscription_pair(
    db: AsyncSession,
    user_id: int,
) -> Tuple[Optional[Subscription], Optional[Subscription]]:
    from app.database.crud.subscription import get_subscription_by_user_id

    standard = await get_subscription_by_user_id(db, user_id, tariff_code=TariffCode.STANDARD.value)
    white = await get_subscription_by_user_id(db, user_id, tariff_code=TariffCode.WHITE.value)
    return standard, white


def _format_datetime(dt) -> str:
    from app.handlers.admin.users import format_datetime as _format_datetime_impl

    return _format_datetime_impl(dt)


def _format_time_ago(dt, language: str) -> str:
    from app.handlers.admin.users import format_time_ago as _format_time_ago_impl

    return _format_time_ago_impl(dt, language)


async def _render_subscription_overview(
    callback: types.CallbackQuery,
    *,
    db: AsyncSession,
    user_id: int,
    tariff_code: str,
) -> bool:
    from app.database.crud.server_squad import get_server_squad_by_uuid
    from app.database.crud.subscription import get_subscription_by_user_id

    user = await get_user_by_id(db, user_id)
    if not user:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return False

    subscription = await get_subscription_by_user_id(db, user_id, tariff_code=tariff_code)

    text = "📱 <b>Подписка и настройки пользователя</b>\n\n"
    user_link = f'<a href="tg://user?id={user.telegram_id}">{user.full_name}</a>'
    text += f"👤 {user_link} (ID: <code>{user.telegram_id}</code>)\n\n"
    text += f"Тариф: <b>{_format_tariff_label(tariff_code)}</b>\n\n"

    keyboard: list[list[types.InlineKeyboardButton]] = []

    if not subscription:
        text += "❌ <b>Подписка отсутствует</b>\n"
        keyboard.append(
            [types.InlineKeyboardButton(text="🔀 Выбор тарифа", callback_data=f"admin_user_subscription_choose_{user_id}")]
        )
        keyboard.append(
            [types.InlineKeyboardButton(text="⬅️ К пользователю", callback_data=f"admin_user_manage_{user_id}")]
        )
        await callback.message.edit_text(
            text,
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=keyboard),
        )
        return True

    status_emoji = "✅" if subscription.is_active else "❌"
    type_emoji = "🆓" if subscription.is_trial else "💎"

    traffic_display = f"{subscription.traffic_used_gb:.1f}/"
    traffic_display += "∞ ГБ" if subscription.traffic_limit_gb == 0 else f"{subscription.traffic_limit_gb} ГБ"

    text += f"Статус: {status_emoji} {'Активна' if subscription.is_active else 'Неактивна'}\n"
    text += f"Тип: {type_emoji} {'Триал' if subscription.is_trial else 'Платная'}\n"
    text += f"Начало: {_format_datetime(subscription.start_date)}\n"
    text += f"Окончание: {_format_datetime(subscription.end_date)}\n"
    text += f"Трафик: {traffic_display}\n"
    text += f"Устройства: {subscription.device_limit}\n"
    if subscription.is_active:
        text += f"Осталось дней: {(subscription.end_date - datetime.utcnow()).days}\n"

    current_squads = subscription.connected_squads or []
    if current_squads:
        text += "\nПодключенные серверы:\n"
        for squad_uuid in current_squads:
            server = await get_server_squad_by_uuid(db, squad_uuid)
            text += f"• {server.display_name if server else squad_uuid[:8] + '...'}\n"
    else:
        text += "\nПодключенные серверы:\n• отсутствуют\n"

    keyboard = [
        [
            types.InlineKeyboardButton(text="⏳ Продлить", callback_data=f"admin_sub_extend_{user_id}"),
            types.InlineKeyboardButton(text="💳 Купить", callback_data=f"admin_sub_buy_{user_id}"),
        ],
        [
            types.InlineKeyboardButton(text="🔄 Тип подписки", callback_data=f"admin_sub_change_type_{user_id}"),
            types.InlineKeyboardButton(text="📈 Добавить трафик", callback_data=f"admin_sub_traffic_{user_id}"),
        ],
        [
            types.InlineKeyboardButton(text="🛰️ Серверы", callback_data=f"admin_user_change_server_{user_id}"),
            types.InlineKeyboardButton(text="📱 Устройства", callback_data=f"admin_user_devices_{user_id}"),
        ],
        [
            types.InlineKeyboardButton(text="🧾 Лимит трафика", callback_data=f"admin_user_traffic_{user_id}"),
            types.InlineKeyboardButton(text="🔄 Сброс устройств", callback_data=f"admin_user_reset_devices_{user_id}"),
        ],
    ]

    if settings.is_modem_enabled():
        modem_status = "✅" if getattr(subscription, "modem_enabled", False) else "❌"
        keyboard.append(
            [types.InlineKeyboardButton(text=f"📶 Модем ({modem_status})", callback_data=f"admin_user_modem_{user_id}")]
        )

    if subscription.is_active:
        keyboard.append([types.InlineKeyboardButton(text="⛔ Деактивировать", callback_data=f"admin_sub_deactivate_{user_id}")])
    else:
        keyboard.append([types.InlineKeyboardButton(text="✅ Активировать", callback_data=f"admin_sub_activate_{user_id}")])

    keyboard.append([types.InlineKeyboardButton(text="🔀 Выбор тарифа", callback_data=f"admin_user_subscription_choose_{user_id}")])
    keyboard.append([types.InlineKeyboardButton(text="⬅️ К пользователю", callback_data=f"admin_user_manage_{user_id}")])

    await callback.message.edit_text(
        text,
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=keyboard),
    )
    return True


async def _show_tariff_selector(
    callback: types.CallbackQuery,
    *,
    db: AsyncSession,
    user_id: int,
) -> None:
    user = await get_user_by_id(db, user_id)
    if not user:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return

    standard, white = await _get_subscription_pair(db, user_id)

    user_link = f'<a href="tg://user?id={user.telegram_id}">{user.full_name}</a>'
    text = "📱 <b>Подписка и настройки пользователя</b>\n\n"
    text += f"👤 {user_link} (ID: <code>{user.telegram_id}</code>)\n\n"
    text += "Выберите тариф, который будем настраивать:\n\n"
    text += f"• Standard: {'✅ есть' if standard else '❌ отсутствует'}\n"
    text += f"• White: {'✅ есть' if white else '❌ отсутствует'}\n"

    keyboard = [
        [
            types.InlineKeyboardButton(text="Standard", callback_data=f"admin_user_subscription_select_standard_{user_id}"),
            types.InlineKeyboardButton(text="White", callback_data=f"admin_user_subscription_select_white_{user_id}"),
        ],
        [types.InlineKeyboardButton(text="⬅️ К пользователю", callback_data=f"admin_user_manage_{user_id}")],
    ]

    await callback.message.edit_text(
        text,
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=keyboard),
    )
    await callback.answer()


async def show_user_subscription(
    callback: types.CallbackQuery,
    db_user: User,
    db: AsyncSession,
    state: FSMContext,
):
    parts = str(callback.data).split("_")
    user_id = int(parts[-1])

    if "choose" in parts:
        await _set_selected_tariff(state, user_id=user_id, tariff_code=TariffCode.STANDARD.value)
        return await _show_tariff_selector(callback, db=db, user_id=user_id)

    if "select" in parts:
        tariff_code = normalize_tariff_code(parts[-2])
        await _set_selected_tariff(state, user_id=user_id, tariff_code=tariff_code)
        with use_tariff_code(tariff_code):
            ok = await _render_subscription_overview(callback, db=db, user_id=user_id, tariff_code=tariff_code)
        if ok:
            await callback.answer()
        return

    selected_user_id = (await state.get_data()).get(_STATE_SELECTED_USER_ID)
    if selected_user_id != user_id:
        await _set_selected_tariff(state, user_id=user_id, tariff_code=TariffCode.STANDARD.value)
        return await _show_tariff_selector(callback, db=db, user_id=user_id)

    tariff_code = await _get_selected_tariff_for_user(state, user_id=user_id)
    with use_tariff_code(tariff_code):
        ok = await _render_subscription_overview(callback, db=db, user_id=user_id, tariff_code=tariff_code)
    if ok:
        await callback.answer()


def _format_subscription_management_block(
    *,
    texts,
    label: str,
    subscription: Optional[Subscription],
) -> str:
    if not subscription:
        return f"<b>Подписка {label}:</b> Отсутствует"

    subscription_type = (
        texts.ADMIN_USER_SUBSCRIPTION_TYPE_TRIAL
        if subscription.is_trial
        else texts.ADMIN_USER_SUBSCRIPTION_TYPE_PAID
    )
    subscription_status = (
        texts.ADMIN_USER_SUBSCRIPTION_STATUS_ACTIVE
        if subscription.is_active
        else texts.ADMIN_USER_SUBSCRIPTION_STATUS_INACTIVE
    )
    traffic_usage = texts.ADMIN_USER_TRAFFIC_USAGE.format(
        used=f"{subscription.traffic_used_gb:.1f}",
        limit=subscription.traffic_limit_gb,
    )
    countries = len(subscription.connected_squads or [])

    return (
        f"<b>Подписка {label}:</b>\n"
        f"• Тип: {subscription_type}\n"
        f"• Статус: {subscription_status}\n"
        f"• До: {_format_datetime(subscription.end_date)}\n"
        f"• Трафик: {traffic_usage}\n"
        f"• Устройства: {subscription.device_limit}\n"
        f"• Стран: {countries}"
    )


async def show_users_statistics(
    callback: types.CallbackQuery,
    db_user: User,
    db: AsyncSession,
):
    user_service = UserService()
    stats = await user_service.get_user_statistics(db)

    current_time = datetime.utcnow()

    users_with_subscription_query = (
        select(func.count(func.distinct(Subscription.user_id)))
        .join(User, Subscription.user_id == User.id)
        .where(
            User.status == UserStatus.ACTIVE.value,
            Subscription.end_date > current_time,
            Subscription.status.in_([SubscriptionStatus.ACTIVE.value, SubscriptionStatus.TRIAL.value]),
        )
    )
    users_with_subscription = (await db.execute(users_with_subscription_query)).scalar() or 0

    trial_users_query = (
        select(func.count(func.distinct(Subscription.user_id)))
        .join(User, Subscription.user_id == User.id)
        .where(
            User.status == UserStatus.ACTIVE.value,
            Subscription.end_date > current_time,
            or_(
                Subscription.status == SubscriptionStatus.TRIAL.value,
                Subscription.is_trial.is_(True),
            ),
        )
    )
    trial_users = (await db.execute(trial_users_query)).scalar() or 0

    users_without_subscription = max(stats["active_users"] - users_with_subscription, 0)

    avg_balance_result = await db.execute(
        select(func.avg(User.balance_kopeks)).where(User.status == UserStatus.ACTIVE.value)
    )
    avg_balance = avg_balance_result.scalar() or 0

    text = f"""
📊 <b>Детальная статистика пользователей</b>

👥 <b>Общие показатели:</b>
• Всего: {stats['total_users']}
• Активных: {stats['active_users']}
• Заблокированных: {stats['blocked_users']}

📱 <b>Подписки:</b>
• С активной подпиской: {users_with_subscription}
• На триале: {trial_users}
• Без подписки: {users_without_subscription}

💰 <b>Финансы:</b>
• Средний баланс: {settings.format_price(int(avg_balance))}

📈 <b>Регистрации:</b>
• Сегодня: {stats['new_today']}
• За неделю: {stats['new_week']}
• За месяц: {stats['new_month']}

📊 <b>Активность:</b>
• Конверсия в подписку: {(users_with_subscription / max(stats['active_users'], 1) * 100):.1f}%
• Доля триальных: {(trial_users / max(users_with_subscription, 1) * 100):.1f}%
"""

    await callback.message.edit_text(
        text,
        reply_markup=types.InlineKeyboardMarkup(
            inline_keyboard=[
                [types.InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_users_stats")],
                [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")],
            ]
        ),
    )
    await callback.answer()


async def show_user_management(
    callback: types.CallbackQuery,
    db_user: User,
    db: AsyncSession,
    state: FSMContext,
):
    import app.handlers.admin.users as admin_users

    parts = str(callback.data).split("_")
    try:
        user_id = int(parts[3])
    except Exception:
        user_id = int(parts[-1])

    origin_ticket_id = None
    if "from" in parts and "ticket" in parts:
        try:
            origin_ticket_id = int(parts[-1])
        except Exception:
            origin_ticket_id = None

    try:
        if origin_ticket_id:
            await state.update_data(origin_ticket_id=origin_ticket_id, origin_ticket_user_id=user_id)
    except Exception:
        pass

    if origin_ticket_id is None:
        try:
            data_state = await state.get_data()
            if data_state.get("origin_ticket_user_id") == user_id:
                origin_ticket_id = data_state.get("origin_ticket_id")
        except Exception:
            pass

    back_callback = "admin_users_list"

    user_service = UserService()
    profile = await user_service.get_user_profile(db, user_id)
    if not profile:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return

    user = profile["user"]
    subscription_standard, subscription_white = await _get_subscription_pair(db, user_id)

    texts = get_texts(db_user.language)

    status_map = {
        UserStatus.ACTIVE.value: texts.ADMIN_USER_STATUS_ACTIVE,
        UserStatus.BLOCKED.value: texts.ADMIN_USER_STATUS_BLOCKED,
        UserStatus.DELETED.value: texts.ADMIN_USER_STATUS_DELETED,
    }
    status_text = status_map.get(user.status, texts.ADMIN_USER_STATUS_UNKNOWN)
    username_display = f"@{user.username}" if user.username else texts.ADMIN_USER_USERNAME_NOT_SET
    last_activity = (
        _format_time_ago(user.last_activity, db_user.language)
        if user.last_activity
        else texts.ADMIN_USER_LAST_ACTIVITY_UNKNOWN
    )

    sections = [
        texts.ADMIN_USER_MANAGEMENT_PROFILE.format(
            name=user.full_name,
            telegram_id=user.telegram_id,
            username=username_display,
            status=status_text,
            language=user.language,
            balance=settings.format_price(user.balance_kopeks),
            transactions=profile["transactions_count"],
            registration=_format_datetime(user.created_at),
            last_activity=last_activity,
            registration_days=profile["registration_days"],
        ),
        _format_subscription_management_block(texts=texts, label="Standard", subscription=subscription_standard),
        _format_subscription_management_block(texts=texts, label="White", subscription=subscription_white),
    ]

    primary_group = user.get_primary_promo_group()
    if primary_group:
        sections.append(
            texts.t(
                "ADMIN_USER_PROMO_GROUPS_PRIMARY",
                "⭐️ Основная: {name} (Priority: {priority})",
            ).format(name=primary_group.name, priority=getattr(primary_group, "priority", 0))
        )
        sections.append(
            texts.ADMIN_USER_MANAGEMENT_PROMO_GROUP.format(
                name=primary_group.name,
                server_discount=primary_group.server_discount_percent,
                traffic_discount=primary_group.traffic_discount_percent,
                device_discount=primary_group.device_discount_percent,
            )
        )
    else:
        sections.append(texts.ADMIN_USER_MANAGEMENT_PROMO_GROUP_NONE)

    restriction_topup = getattr(user, "restriction_topup", False)
    restriction_subscription = getattr(user, "restriction_subscription", False)
    if restriction_topup or restriction_subscription:
        restriction_lines = ["⚠️ <b>Ограничения:</b>"]
        if restriction_topup:
            restriction_lines.append("  • ⛔ Пополнение запрещено")
        if restriction_subscription:
            restriction_lines.append("  • ⛔ Подписка/продление запрещено")
        restriction_reason = getattr(user, "restriction_reason", None)
        if restriction_reason:
            restriction_lines.append(f"  • 📝 Причина: {restriction_reason}")
        sections.append("\n".join(restriction_lines))

    text = "\n\n".join(sections)

    current_state = await state.get_state()
    if current_state == admin_users.AdminStates.viewing_user_from_balance_list:
        back_callback = "admin_users_balance_filter"
    elif current_state == admin_users.AdminStates.viewing_user_from_traffic_list:
        back_callback = "admin_users_traffic_filter"
    elif current_state == admin_users.AdminStates.viewing_user_from_last_activity_list:
        back_callback = "admin_users_activity_filter"
    elif current_state == admin_users.AdminStates.viewing_user_from_spending_list:
        back_callback = "admin_users_spending_filter"
    elif current_state == admin_users.AdminStates.viewing_user_from_purchases_list:
        back_callback = "admin_users_purchases_filter"
    elif current_state == admin_users.AdminStates.viewing_user_from_campaign_list:
        back_callback = "admin_users_campaign_filter"
    elif current_state == admin_users.AdminStates.viewing_user_from_ready_to_renew_list:
        back_callback = "admin_users_ready_to_renew_filter"

    kb = admin_users.get_user_management_keyboard(user.id, user.status, db_user.language, back_callback)
    try:
        if origin_ticket_id:
            kb.inline_keyboard.insert(
                0,
                [
                    [
                        types.InlineKeyboardButton(
                            text="⛔ Вернуться в тикет",
                            callback_data=f"admin_view_ticket_{origin_ticket_id}",
                        )
                    ]
                ][0],
            )
    except Exception:
        pass

    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()


async def show_user_statistics(
    callback: types.CallbackQuery,
    db_user: User,
    db: AsyncSession,
):
    user_id = int(str(callback.data).split("_")[-1])

    user_service = UserService()
    profile = await user_service.get_user_profile(db, user_id)
    if not profile:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return

    user = profile["user"]
    subscription_standard, subscription_white = await _get_subscription_pair(db, user_id)

    def _sub_brief(sub: Optional[Subscription]) -> str:
        if not sub:
            return "отсутствует"
        status = "✅ активна" if sub.is_active else "❌ неактивна"
        typ = "🆓 триал" if sub.is_trial else "💎 платная"
        if sub.traffic_limit_gb == 0:
            traffic = f"{sub.traffic_used_gb:.1f}/∞ ГБ"
        else:
            traffic = f"{sub.traffic_used_gb:.1f}/{sub.traffic_limit_gb} ГБ"
        return f"{status} ({typ}), трафик {traffic}, устройств {sub.device_limit}, стран {len(sub.connected_squads or [])}"

    text = "📊 <b>Статистика пользователя</b>\n\n"
    user_link = f'<a href="tg://user?id={user.telegram_id}">{user.full_name}</a>'
    text += f"👤 {user_link} (ID: <code>{user.telegram_id}</code>)\n\n"
    text += "<b>Основные показатели:</b>\n"
    text += f"• Дней с регистрации: {profile['registration_days']}\n"
    text += f"• Баланс: {settings.format_price(user.balance_kopeks)}\n"
    text += f"• Транзакций: {profile['transactions_count']}\n"
    text += f"• Язык: {user.language}\n\n"
    text += "<b>Подписки:</b>\n"
    text += f"• Standard: {_sub_brief(subscription_standard)}\n"
    text += f"• White: {_sub_brief(subscription_white)}\n"

    await callback.message.edit_text(
        text,
        reply_markup=types.InlineKeyboardMarkup(
            inline_keyboard=[
                [types.InlineKeyboardButton(text="⬅️ К пользователю", callback_data=f"admin_user_manage_{user_id}")],
            ]
        ),
    )
    await callback.answer()


async def _update_user_devices(
    db: AsyncSession,
    user_id: int,
    devices: int,
    admin_id: int,
) -> bool:
    from app.database.crud.subscription import get_subscription_by_user_id
    from app.services.subscription_service import SubscriptionService

    try:
        subscription = await get_subscription_by_user_id(db, user_id)
        if not subscription:
            logger.error("❌ Подписка не найдена для пользователя %s", user_id)
            return False

        subscription.device_limit = devices
        subscription.updated_at = datetime.utcnow()
        await db.commit()

        subscription_service = SubscriptionService()
        await subscription_service.update_remnawave_user(db, subscription)

        logger.info("✅ Админ %s обновил лимит устройств user=%s -> %s", admin_id, user_id, devices)
        return True
    except Exception as e:
        logger.error("❌ Ошибка обновления устройств: %s", e)
        await db.rollback()
        return False


async def _update_user_traffic(
    db: AsyncSession,
    user_id: int,
    traffic_gb: int,
    admin_id: int,
) -> bool:
    from app.database.crud.subscription import get_subscription_by_user_id
    from app.services.subscription_service import SubscriptionService

    try:
        subscription = await get_subscription_by_user_id(db, user_id)
        if not subscription:
            logger.error("❌ Подписка не найдена для пользователя %s", user_id)
            return False

        subscription.traffic_limit_gb = traffic_gb
        subscription.updated_at = datetime.utcnow()
        await db.commit()

        subscription_service = SubscriptionService()
        await subscription_service.update_remnawave_user(db, subscription)

        logger.info("✅ Админ %s обновил лимит трафика user=%s -> %s", admin_id, user_id, traffic_gb)
        return True
    except Exception as e:
        logger.error("❌ Ошибка обновления трафика: %s", e)
        await db.rollback()
        return False


async def _show_servers_for_user(
    callback: types.CallbackQuery,
    user_id: int,
    db: AsyncSession,
):
    from app.database.crud.server_squad import get_all_server_squads
    from app.database.crud.subscription import get_subscription_by_user_id

    subscription = await get_subscription_by_user_id(db, user_id)
    current_squads = list(subscription.connected_squads or []) if subscription else []

    all_servers, _ = await get_all_server_squads(db, available_only=False)
    servers_to_show = [s for s in all_servers if s.is_available or s.squad_uuid in current_squads]

    if not servers_to_show:
        await callback.message.edit_text(
            "❌ Доступных серверов не найдено",
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin_user_subscription_{user_id}")]]
            ),
        )
        return

    text = "🛰️ <b>Управление серверами</b>\n\n"
    text += "Нажмите на сервер, чтобы включить/выключить.\n\n"

    keyboard: list[list[types.InlineKeyboardButton]] = []
    for server in servers_to_show:
        emoji = "✅" if server.squad_uuid in current_squads else "☑️"
        keyboard.append(
            [
                types.InlineKeyboardButton(
                    text=f"{emoji} {server.display_name}",
                    callback_data=f"admin_user_toggle_server_{user_id}_{server.id}",
                )
            ]
        )

    keyboard.append([types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin_user_subscription_{user_id}")])

    await callback.message.edit_text(
        text,
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=keyboard),
    )


async def toggle_user_server(
    callback: types.CallbackQuery,
    db_user: User,
    db: AsyncSession,
    state: FSMContext,
):
    parts = str(callback.data).split("_")
    user_id = int(parts[-2])
    server_id = int(parts[-1])

    from app.database.crud.server_squad import get_server_squad_by_id
    from app.database.crud.subscription import get_subscription_by_user_id
    from app.services.subscription_service import SubscriptionService

    subscription = await get_subscription_by_user_id(db, user_id)
    if not subscription:
        await callback.answer("❌ Подписка не найдена", show_alert=True)
        return

    server = await get_server_squad_by_id(db, server_id)
    if not server:
        await callback.answer("❌ Сервер не найден", show_alert=True)
        return

    current_squads = list(subscription.connected_squads or [])
    if server.squad_uuid in current_squads:
        current_squads.remove(server.squad_uuid)
    else:
        current_squads.append(server.squad_uuid)

    subscription.connected_squads = current_squads
    subscription.updated_at = datetime.utcnow()
    await db.commit()

    try:
        subscription_service = SubscriptionService()
        await subscription_service.update_remnawave_user(db, subscription)
    except Exception as e:
        logger.error("❌ Ошибка синхронизации RemnaWave (серверы): %s", e)

    await _show_servers_for_user(callback, user_id, db)
    await callback.answer()


async def reset_user_devices(
    callback: types.CallbackQuery,
    db_user: User,
    db: AsyncSession,
    state: FSMContext,
):
    user_id = int(str(callback.data).split("_")[-1])
    from app.database.crud.subscription import get_subscription_by_user_id
    from app.services.subscription_service import SubscriptionService

    subscription = await get_subscription_by_user_id(db, user_id)
    if not subscription:
        await callback.answer("❌ Подписка не найдена", show_alert=True)
        return

    subscription_service = SubscriptionService()
    try:
        await subscription_service.update_remnawave_user(db, subscription)
    except Exception:
        pass

    remnawave_uuid = getattr(subscription, "remnawave_uuid", None)
    if not remnawave_uuid:
        await callback.answer("❌ Не найден RemnaWave UUID", show_alert=True)
        return

    try:
        from app.services.remnawave_service import RemnaWaveService

        remnawave_service = RemnaWaveService()
        async with remnawave_service.get_api_client() as api:
            success = await api.reset_user_devices(remnawave_uuid)
    except Exception as e:
        logger.error("❌ Ошибка сброса устройств: %s", e)
        await callback.answer("❌ Ошибка сброса устройств", show_alert=True)
        return

    if success:
        await callback.message.edit_text(
            "✅ Устройства сброшены",
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=[[types.InlineKeyboardButton(text="📱 К подписке", callback_data=f"admin_user_subscription_{user_id}")]]
            ),
        )
    else:
        await callback.message.edit_text(
            "❌ Не удалось сбросить устройства",
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=[[types.InlineKeyboardButton(text="📱 К подписке", callback_data=f"admin_user_subscription_{user_id}")]]
            ),
        )
    await callback.answer()


async def toggle_user_modem(
    callback: types.CallbackQuery,
    db_user: User,
    db: AsyncSession,
    state: FSMContext,
):
    user_id = int(str(callback.data).split("_")[-1])
    from app.database.crud.subscription import get_subscription_by_user_id
    from app.services.subscription_service import SubscriptionService

    subscription = await get_subscription_by_user_id(db, user_id)
    if not subscription:
        await callback.answer("❌ Подписка не найдена", show_alert=True)
        return

    modem_enabled = bool(getattr(subscription, "modem_enabled", False))
    if modem_enabled:
        subscription.modem_enabled = False
        if subscription.device_limit and subscription.device_limit > 1:
            subscription.device_limit = subscription.device_limit - 1
        action_text = "выключен"
    else:
        subscription.modem_enabled = True
        subscription.device_limit = (subscription.device_limit or 1) + 1
        action_text = "включен"

    subscription.updated_at = datetime.utcnow()
    await db.commit()

    try:
        subscription_service = SubscriptionService()
        await subscription_service.update_remnawave_user(db, subscription)
    except Exception as e:
        logger.error("❌ Ошибка синхронизации RemnaWave (модем): %s", e)

    await callback.message.edit_text(
        f"📶 <b>Модем {action_text}</b>\n\nУстройства: {subscription.device_limit}",
        reply_markup=types.InlineKeyboardMarkup(
            inline_keyboard=[[types.InlineKeyboardButton(text="📱 К подписке", callback_data=f"admin_user_subscription_{user_id}")]]
        ),
        parse_mode="HTML",
    )
    await callback.answer()


def apply_admin_users_multi_tariff_patches() -> None:
    global _PATCHED
    if _PATCHED:
        return

    import app.handlers.admin.users as admin_users

    if getattr(admin_users, "_spiderman_admin_multi_tariff_patched", False):
        _PATCHED = True
        return

    # Screens: statistics + user cards + subscription selector
    admin_users.show_users_statistics = show_users_statistics
    admin_users.show_user_management = show_user_management
    admin_users.show_user_statistics = show_user_statistics
    admin_users.show_user_subscription = show_user_subscription

    # Subscription-scoped actions that used standard-only relations
    admin_users._update_user_devices = _update_user_devices
    admin_users._update_user_traffic = _update_user_traffic
    admin_users._show_servers_for_user = _show_servers_for_user
    admin_users.toggle_user_server = toggle_user_server
    admin_users.reset_user_devices = reset_user_devices
    admin_users.toggle_user_modem = toggle_user_modem

    # Ensure admin actions respect выбранный тариф (Standard/White)
    callback_handlers = (
        "process_subscription_extension_days",
        "add_subscription_traffic",
        "process_traffic_addition_button",
        "deactivate_user_subscription",
        "confirm_subscription_deactivation",
        "activate_user_subscription",
        "grant_trial_subscription",
        "grant_paid_subscription",
        "process_subscription_grant_days",
        "show_server_selection",
        "toggle_user_server",
        "set_user_devices_button",
        "set_user_traffic_button",
        "confirm_reset_devices",
        "reset_user_devices",
        "toggle_user_modem",
        "change_subscription_type",
        "change_subscription_type_confirm",
        "admin_buy_subscription",
        "admin_buy_subscription_confirm",
        "admin_buy_subscription_execute",
    )
    message_handlers = (
        "process_subscription_extension_text",
        "process_traffic_addition_text",
        "process_subscription_grant_text",
        "process_devices_edit_text",
        "process_traffic_edit_text",
    )

    for name in callback_handlers:
        if not hasattr(admin_users, name):
            continue
        original = getattr(admin_users, name)
        if getattr(original, "_spiderman_multi_tariff_wrapped", False):
            continue
        wrapped = _wrap_callback_handler_with_tariff(original)
        setattr(wrapped, "_spiderman_multi_tariff_wrapped", True)
        setattr(admin_users, name, wrapped)

    for name in message_handlers:
        if not hasattr(admin_users, name):
            continue
        original = getattr(admin_users, name)
        if getattr(original, "_spiderman_multi_tariff_wrapped", False):
            continue
        wrapped = _wrap_message_handler_with_tariff(original)
        setattr(wrapped, "_spiderman_multi_tariff_wrapped", True)
        setattr(admin_users, name, wrapped)

    admin_users._spiderman_admin_multi_tariff_patched = True
    _PATCHED = True
    logger.info("🕷️ SpiderMan: патчи админки пользователей (multi-tariff) применены")
