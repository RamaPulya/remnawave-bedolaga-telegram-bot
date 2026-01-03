from enum import Enum
from typing import Optional


class TariffCode(str, Enum):
    STANDARD = "standard"
    WHITE = "white"


DEFAULT_TARIFF_CODE = TariffCode.STANDARD.value


def normalize_tariff_code(value: Optional[str]) -> str:
    if not value:
        return DEFAULT_TARIFF_CODE

    normalized = str(value).strip().lower()
    if normalized in (TariffCode.WHITE.value, "w", "white"):
        return TariffCode.WHITE.value
    if normalized in (TariffCode.STANDARD.value, "s", "standard"):
        return TariffCode.STANDARD.value

    return DEFAULT_TARIFF_CODE
