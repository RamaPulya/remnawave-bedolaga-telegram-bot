import importlib

from app.spiderman.menu_media import (
    SLOT_MAIN_MENU,
    SLOT_REFERRAL,
    SLOT_SUPPORT,
    edit_or_answer_media,
)


def _build_wrapper(slot: str):
    async def wrapper(
        callback,
        caption,
        keyboard,
        parse_mode: str | None = 'HTML',
        *,
        force_text: bool = False,
    ):
        return await edit_or_answer_media(
            callback=callback,
            slot=slot,
            caption=caption,
            keyboard=keyboard,
            parse_mode=parse_mode,
            force_text=force_text,
        )

    return wrapper


def apply_menu_media_patches() -> None:
    menu_handlers = importlib.import_module('app.handlers.menu')
    referral_handlers = importlib.import_module('app.handlers.referral')
    support_handlers = importlib.import_module('app.handlers.support')

    if getattr(menu_handlers, '_spiderman_menu_media_patched', False):
        return

    menu_handlers.edit_or_answer_photo = _build_wrapper(SLOT_MAIN_MENU)
    support_handlers.edit_or_answer_photo = _build_wrapper(SLOT_SUPPORT)
    referral_handlers.edit_or_answer_photo = _build_wrapper(SLOT_REFERRAL)

    menu_handlers._spiderman_menu_media_patched = True
