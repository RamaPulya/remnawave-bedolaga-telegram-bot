from aiogram.fsm.state import State, StatesGroup


class SpidermanMediaStates(StatesGroup):
    waiting_for_menu_media = State()


class SpidermanChannelPostStates(StatesGroup):
    waiting_for_post = State()
    confirming_post = State()
