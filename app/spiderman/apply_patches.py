import importlib
import logging

from app.config import settings


logger = logging.getLogger(__name__)

_PATCHED = False


def apply_spiderman_patches() -> None:
    global _PATCHED
    if _PATCHED:
        return

    if not settings.SPIDERMAN_MODE:
        logger.info('🕷️ Патчи SpiderMan отключены (SPIDERMAN_MODE=false)')
        return

    apply_admin_menu_media_patches = importlib.import_module('app.spiderman.admin_media_patch').apply_admin_menu_media_patches
    apply_admin_panel_media_patches = (
        importlib.import_module('app.spiderman.admin_panel_media_patch').apply_admin_panel_media_patches
    )
    apply_campaigns_patches = importlib.import_module('app.spiderman.campaigns_patch').apply_campaigns_patches
    apply_menu_media_patches = importlib.import_module('app.spiderman.menu_media_patch').apply_menu_media_patches
    apply_reply_main_menu_patches = (
        importlib.import_module('app.spiderman.reply_main_menu_patch').apply_reply_main_menu_patches
    )

    apply_menu_media_patches()
    apply_admin_menu_media_patches()
    apply_admin_panel_media_patches()
    apply_reply_main_menu_patches()
    apply_campaigns_patches()

    _PATCHED = True
    logger.info('🕷️ Патчи SpiderMan применены')
