import importlib
import re


_CAMPAIGN_PARAM_REGEX = re.compile(r'^[A-Za-z0-9_-]{2,32}$')
_INVALID_PARAM_TEXT = '❌ Разрешены только латинские буквы, цифры, символы - и _. Длина 2-32 символа.'


def apply_campaigns_patches() -> None:
    campaigns_handlers = importlib.import_module('app.handlers.admin.campaigns')

    if getattr(campaigns_handlers, '_spiderman_campaigns_patched', False):
        return

    campaigns_handlers._CAMPAIGN_PARAM_REGEX = _CAMPAIGN_PARAM_REGEX

    original_process_create = campaigns_handlers.process_campaign_start_parameter
    original_process_edit = campaigns_handlers.process_edit_campaign_start_parameter

    async def process_campaign_start_parameter(message, db_user, state, db):
        start_param = message.text.strip()
        if not _CAMPAIGN_PARAM_REGEX.match(start_param):
            await message.answer(_INVALID_PARAM_TEXT)
            return None
        return await original_process_create(message, db_user, state, db)

    async def process_edit_campaign_start_parameter(message, db_user, state, db):
        new_param = message.text.strip()
        if not _CAMPAIGN_PARAM_REGEX.match(new_param):
            await message.answer(_INVALID_PARAM_TEXT)
            return None
        return await original_process_edit(message, db_user, state, db)

    campaigns_handlers.process_campaign_start_parameter = process_campaign_start_parameter
    campaigns_handlers.process_edit_campaign_start_parameter = process_edit_campaign_start_parameter
    campaigns_handlers._spiderman_campaigns_patched = True
