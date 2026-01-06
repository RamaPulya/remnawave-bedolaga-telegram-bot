import logging

from app.config import settings

logger = logging.getLogger(__name__)

_PATCHED = False


def apply_spiderman_patches() -> None:
    global _PATCHED
    if _PATCHED:
        return

    if not settings.SPIDERMAN_MODE:
        logger.info("Spiderman patches disabled (SPIDERMAN_MODE=false)")
        return

    from app.spiderman.menu_patch import apply_menu_patches

    apply_menu_patches()

    if not settings.MULTI_TARIFF_ENABLED:
        logger.info("Spiderman patches disabled (MULTI_TARIFF_ENABLED=false)")
        _PATCHED = True
        return

    from app.spiderman.subscription_crud_patch import apply_subscription_crud_patches
    from app.spiderman.subscription_utils_patch import apply_subscription_utils_patches
    from app.spiderman.remnawave_patch import apply_remnawave_patches
    from app.spiderman.subscription_menu_patch import apply_subscription_menu_patches
    from app.spiderman.subscription_purchase_patch import apply_subscription_purchase_patches
    from app.spiderman.subscription_extend_patch import apply_subscription_extend_patches
    from app.spiderman.subscription_devices_patch import apply_subscription_devices_patches

    apply_subscription_crud_patches()
    apply_subscription_utils_patches()
    apply_remnawave_patches()
    apply_subscription_menu_patches()
    apply_subscription_purchase_patches()
    apply_subscription_extend_patches()
    apply_subscription_devices_patches()

    _PATCHED = True
    logger.info("Spiderman patches applied")
