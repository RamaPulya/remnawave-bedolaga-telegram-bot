from contextlib import contextmanager
from contextvars import ContextVar
from enum import Enum
from typing import Optional


class TariffCode(str, Enum):
    STANDARD = "standard"
    WHITE = "white"


DEFAULT_TARIFF_CODE = TariffCode.STANDARD.value


_CURRENT_TARIFF_CODE: ContextVar[str] = ContextVar(
    "spiderman_tariff_code",
    default=DEFAULT_TARIFF_CODE,
)


def get_current_tariff_code() -> str:
    return _CURRENT_TARIFF_CODE.get()


def normalize_tariff_code(value: Optional[str]) -> str:
    if not value:
        return get_current_tariff_code()

    normalized = str(value).strip().lower()
    if normalized in (TariffCode.WHITE.value, "w", "white"):
        return TariffCode.WHITE.value
    if normalized in (TariffCode.STANDARD.value, "s", "standard"):
        return TariffCode.STANDARD.value

    return DEFAULT_TARIFF_CODE


@contextmanager
def use_tariff_code(value: Optional[str]):
    token = _CURRENT_TARIFF_CODE.set(normalize_tariff_code(value))
    try:
        yield
    finally:
        _CURRENT_TARIFF_CODE.reset(token)
