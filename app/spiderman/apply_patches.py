import logging

from app.config import settings

logger = logging.getLogger(__name__)

_PATCHED = False


def apply_spiderman_patches() -> None:
    global _PATCHED
    if _PATCHED:
        return

    if not settings.SPIDERMAN_MODE:
        logger.info("🕷️ Патчи SpiderMan отключены (SPIDERMAN_MODE=false)")
        return

    from app.spiderman.menu_patch import apply_menu_patches
    from app.spiderman.menu_media_patch import apply_menu_media_patches
    from app.spiderman.admin_media_patch import apply_admin_menu_media_patches
    from app.spiderman.admin_panel_media_patch import apply_admin_panel_media_patches

    apply_menu_patches()
    apply_menu_media_patches()
    apply_admin_menu_media_patches()
    apply_admin_panel_media_patches()

    from app.spiderman.user_service_patch import apply_user_service_patches

    apply_user_service_patches()

    if not settings.MULTI_TARIFF_ENABLED:
        logger.info("🕷️ Патчи SpiderMan для мульти‑тарифа отключены (MULTI_TARIFF_ENABLED=false)")
        _PATCHED = True
        return

    from app.spiderman.subscription_crud_patch import apply_subscription_crud_patches
    from app.spiderman.subscription_utils_patch import apply_subscription_utils_patches
    from app.spiderman.remnawave_patch import apply_remnawave_patches
    from app.spiderman.subscription_menu_patch import apply_subscription_menu_patches
    from app.spiderman.subscription_purchase_patch import apply_subscription_purchase_patches
    from app.spiderman.subscription_extend_patch import apply_subscription_extend_patches
    from app.spiderman.subscription_devices_patch import apply_subscription_devices_patches
    from app.spiderman.subscription_auto_purchase_patch import apply_subscription_auto_purchase_patches
    from app.spiderman.admin_users_multi_tariff_patch import apply_admin_users_multi_tariff_patches

    apply_subscription_crud_patches()
    apply_subscription_utils_patches()
    apply_remnawave_patches()
    apply_subscription_menu_patches()
    apply_subscription_purchase_patches()
    apply_subscription_extend_patches()
    apply_subscription_devices_patches()
    apply_subscription_auto_purchase_patches()
    apply_admin_users_multi_tariff_patches()

    _PATCHED = True
    logger.info("🕷️ Патчи SpiderMan применены")
