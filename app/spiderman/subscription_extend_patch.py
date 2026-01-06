import logging

from aiogram import types
from aiogram.fsm.context import FSMContext
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database.models import User

logger = logging.getLogger(__name__)

_ORIGINAL_HANDLE_EXTEND_SUBSCRIPTION = None


async def handle_extend_subscription(
    callback: types.CallbackQuery,
    state: FSMContext,
    db_user: User,
    db: AsyncSession,
):
    if _ORIGINAL_HANDLE_EXTEND_SUBSCRIPTION is None:
        raise RuntimeError("Spiderman extend patch was not initialized")

    if not settings.MULTI_TARIFF_ENABLED:
        return await _ORIGINAL_HANDLE_EXTEND_SUBSCRIPTION(callback, db_user, db)

    from app.spiderman.subscription_purchase_patch import start_subscription_purchase

    await start_subscription_purchase(callback, state, db_user, db)

    data = await state.get_data()
    data["spiderman_extend_mode"] = True
    await state.set_data(data)


def apply_subscription_extend_patches() -> None:
    import app.handlers.subscription.purchase as purchase
    import app.handlers.subscription as subscription_pkg

    if getattr(purchase, "_spiderman_extend_subscription_patched", False):
        return

    global _ORIGINAL_HANDLE_EXTEND_SUBSCRIPTION
    _ORIGINAL_HANDLE_EXTEND_SUBSCRIPTION = purchase.handle_extend_subscription

    purchase.handle_extend_subscription = handle_extend_subscription
    subscription_pkg.handle_extend_subscription = handle_extend_subscription

    purchase._spiderman_extend_subscription_patched = True
