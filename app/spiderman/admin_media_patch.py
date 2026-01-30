import importlib

from aiogram.types import InlineKeyboardButton

from app.localization.texts import get_texts


_ORIGINAL_GET_ADMIN_MAIN_KEYBOARD = None
_ORIGINAL_ADMIN_MAIN_REGISTER = None
_ADMIN_HANDLER_MODULES = (
    'app.handlers.admin.main',
    'app.handlers.admin.maintenance',
    'app.handlers.admin.monitoring',
    'app.handlers.admin.user_messages',
    'app.handlers.admin.welcome_text',
)


def _patch_admin_main_keyboard() -> None:
    admin_keyboards = importlib.import_module('app.keyboards.admin')

    global _ORIGINAL_GET_ADMIN_MAIN_KEYBOARD
    if getattr(admin_keyboards, '_spiderman_menu_media_patched', False):
        return

    _ORIGINAL_GET_ADMIN_MAIN_KEYBOARD = admin_keyboards.get_admin_main_keyboard

    def get_admin_main_keyboard_patched(language: str = 'ru'):
        keyboard = _ORIGINAL_GET_ADMIN_MAIN_KEYBOARD(language)
        texts = get_texts(language)
        button = InlineKeyboardButton(
            text=texts.t('ADMIN_SPIDERMAN_MENU_BUTTON', '🕷️ Spiderman menu'),
            callback_data='admin_spiderman_menu',
        )
        if keyboard.inline_keyboard:
            keyboard.inline_keyboard.insert(-1, [button])
        else:
            keyboard.inline_keyboard.append([button])
        return keyboard

    admin_keyboards.get_admin_main_keyboard = get_admin_main_keyboard_patched
    admin_keyboards._spiderman_menu_media_patched = True

    for module_name in _ADMIN_HANDLER_MODULES:
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        if getattr(module, '_spiderman_menu_media_patched', False):
            continue
        module.get_admin_main_keyboard = get_admin_main_keyboard_patched
        module._spiderman_menu_media_patched = True


def _patch_admin_main_register() -> None:
    admin_main = importlib.import_module('app.handlers.admin.main')

    global _ORIGINAL_ADMIN_MAIN_REGISTER
    if getattr(admin_main, '_spiderman_admin_menu_media_registered', False):
        return

    _ORIGINAL_ADMIN_MAIN_REGISTER = admin_main.register_handlers

    def register_handlers_patched(dp):
        _ORIGINAL_ADMIN_MAIN_REGISTER(dp)
        register_spiderman_channel_post = importlib.import_module('app.spiderman.admin_channel_post').register_handlers
        register_spiderman_menu_media = importlib.import_module('app.spiderman.admin_menu_media').register_handlers

        register_spiderman_menu_media(dp)
        register_spiderman_channel_post(dp)

    admin_main.register_handlers = register_handlers_patched
    admin_main._spiderman_admin_menu_media_registered = True


def apply_admin_menu_media_patches() -> None:
    _patch_admin_main_keyboard()
    _patch_admin_main_register()
