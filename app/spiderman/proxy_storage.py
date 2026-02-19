from __future__ import annotations

import asyncio
import random
import string
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.database import AsyncSessionLocal
from app.database.models import ButtonClickLog
from app.services.menu_layout_service import MenuLayoutService


PROXY_EVENT_SCREEN_OPEN = 'proxy_screen_open'
PROXY_EVENT_GET_BATCH_CLICK = 'proxy_get_batch_click'
PROXY_EVENT_LINK_CLICK = 'proxy_link_click'
PROXY_EVENT_NOT_WORKING_OPEN = 'proxy_not_working_open'
PROXY_EVENT_NOT_WORKING_SUBMIT = 'proxy_not_working_submit'

PROXY_TRACKED_EVENTS = (
    PROXY_EVENT_SCREEN_OPEN,
    PROXY_EVENT_GET_BATCH_CLICK,
    PROXY_EVENT_LINK_CLICK,
    PROXY_EVENT_NOT_WORKING_OPEN,
    PROXY_EVENT_NOT_WORKING_SUBMIT,
)

# Temporary testing value. Restore to 60 for production.
PROXY_BATCH_COOLDOWN_SECONDS = 5
PROXY_BATCH_DAILY_LIMIT = 10
PROXY_BATCH_SIZE = 3

_TABLES_READY = False
_TABLES_LOCK = asyncio.Lock()
_ID_ALPHABET = string.ascii_lowercase + string.digits
_SYSTEM_RANDOM = random.SystemRandom()


@dataclass(slots=True)
class ProxyLink:
    id: str
    name: str | None
    url: str
    is_active: bool
    created_by: int | None
    created_at: datetime | None
    updated_at: datetime | None


@dataclass(slots=True)
class ProxyMetric:
    total: int = 0
    unique_users: int = 0


def _generate_id(length: int = 12) -> str:
    return ''.join(_SYSTEM_RANDOM.choice(_ID_ALPHABET) for _ in range(length))


def _coerce_datetime(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value
    normalized = str(value).strip()
    if not normalized:
        return None
    normalized = normalized.replace('Z', '+00:00')
    if ' ' in normalized and 'T' not in normalized:
        normalized = normalized.replace(' ', 'T', 1)
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _row_to_proxy_link(row: dict) -> ProxyLink:
    return ProxyLink(
        id=str(row['id']),
        name=row.get('name'),
        url=str(row['url']),
        is_active=bool(row.get('is_active', False)),
        created_by=row.get('created_by'),
        created_at=_coerce_datetime(row.get('created_at')),
        updated_at=_coerce_datetime(row.get('updated_at')),
    )


def _normalize_proxy_url(raw_url: str) -> str:
    return (raw_url or '').strip()


def validate_proxy_url(raw_url: str) -> tuple[bool, str, str]:
    url = _normalize_proxy_url(raw_url)
    if not url:
        return False, '', 'Ссылка пустая.'

    parsed = urlparse(url)
    scheme = (parsed.scheme or '').lower()
    host = (parsed.netloc or '').lower()
    path = (parsed.path or '').rstrip('/')

    if scheme == 'https':
        if host not in {'t.me', 'www.t.me', 'telegram.me'} or path != '/proxy':
            return False, '', 'Разрешены только ссылки вида https://t.me/proxy?...'
    elif scheme == 'tg':
        if host != 'proxy':
            return False, '', 'Разрешены только ссылки вида tg://proxy?...'
    else:
        return False, '', 'Разрешены только ссылки https://t.me/proxy?... или tg://proxy?...'

    query = parse_qs(parsed.query or '')
    for key in ('server', 'port', 'secret'):
        value = (query.get(key) or [''])[0].strip()
        if not value:
            return False, '', f'В ссылке отсутствует параметр "{key}".'

    try:
        port = int((query.get('port') or ['0'])[0])
    except ValueError:
        return False, '', 'Параметр port должен быть числом.'
    if port < 1 or port > 65535:
        return False, '', 'Параметр port должен быть в диапазоне 1..65535.'

    return True, url, ''


def parse_proxy_admin_input(raw_input: str) -> tuple[str | None, str, str]:
    raw = (raw_input or '').strip()
    if not raw:
        return None, '', 'Сообщение пустое. Отправьте ссылку или "Название | ссылка".'

    if '|' in raw:
        left, right = raw.split('|', 1)
        name = left.strip() or None
        url_raw = right.strip()
    else:
        name = None
        url_raw = raw

    is_valid, normalized_url, error = validate_proxy_url(url_raw)
    if not is_valid:
        return None, '', error

    return name, normalized_url, ''


async def ensure_proxy_tables() -> None:
    global _TABLES_READY
    if _TABLES_READY:
        return

    async with _TABLES_LOCK:
        if _TABLES_READY:
            return

        async with AsyncSessionLocal() as db:
            await db.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS spiderman_proxy_links (
                        id VARCHAR(32) PRIMARY KEY,
                        name TEXT NULL,
                        url TEXT NOT NULL UNIQUE,
                        is_active BOOLEAN NOT NULL DEFAULT TRUE,
                        created_by BIGINT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            await db.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS spiderman_proxy_issue_history (
                        id VARCHAR(32) PRIMARY KEY,
                        user_telegram_id BIGINT NOT NULL,
                        batch_id VARCHAR(32) NOT NULL,
                        proxy_link_id VARCHAR(32) NOT NULL,
                        issued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            await db.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_spider_proxy_links_active
                    ON spiderman_proxy_links (is_active)
                    """
                )
            )
            await db.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_spider_proxy_history_user_time
                    ON spiderman_proxy_issue_history (user_telegram_id, issued_at)
                    """
                )
            )
            await db.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_spider_proxy_history_user_link
                    ON spiderman_proxy_issue_history (user_telegram_id, proxy_link_id)
                    """
                )
            )
            await db.commit()

        _TABLES_READY = True


async def log_proxy_event(
    user_telegram_id: int,
    event_id: str,
    *,
    callback_data: str | None = None,
    button_text: str | None = None,
) -> None:
    if not event_id:
        return
    async with AsyncSessionLocal() as db:
        await MenuLayoutService.log_button_click(
            db,
            button_id=event_id,
            user_id=user_telegram_id,
            callback_data=callback_data,
            button_type='callback',
            button_text=button_text,
        )


async def create_proxy_link(
    db: AsyncSession,
    *,
    url: str,
    name: str | None = None,
    created_by: int | None = None,
) -> tuple[bool, str]:
    await ensure_proxy_tables()
    normalized_name = (name or '').strip() or None

    for _ in range(5):
        link_id = _generate_id()
        try:
            await db.execute(
                text(
                    """
                    INSERT INTO spiderman_proxy_links (
                        id,
                        name,
                        url,
                        is_active,
                        created_by
                    )
                    VALUES (
                        :id,
                        :name,
                        :url,
                        TRUE,
                        :created_by
                    )
                    """
                ),
                {
                    'id': link_id,
                    'name': normalized_name,
                    'url': url,
                    'created_by': created_by,
                },
            )
            await db.commit()
            return True, link_id
        except Exception as error:
            await db.rollback()
            message = str(error).lower()
            if 'unique' in message and 'url' in message:
                return False, 'duplicate_url'
            if 'unique' in message and 'id' in message:
                continue
            raise

    return False, 'id_generation_failed'


async def get_proxy_link(db: AsyncSession, link_id: str) -> ProxyLink | None:
    await ensure_proxy_tables()
    result = await db.execute(
        text(
            """
            SELECT
                id,
                name,
                url,
                is_active,
                created_by,
                created_at,
                updated_at
            FROM spiderman_proxy_links
            WHERE id = :id
            """
        ),
        {'id': link_id},
    )
    row = result.mappings().first()
    return _row_to_proxy_link(row) if row else None


async def list_proxy_links(
    db: AsyncSession,
    *,
    page: int = 1,
    page_size: int = 8,
) -> tuple[int, list[ProxyLink]]:
    await ensure_proxy_tables()
    safe_page = max(1, page)
    safe_page_size = max(1, min(page_size, 50))
    offset = (safe_page - 1) * safe_page_size

    total_result = await db.execute(text("SELECT COUNT(*) AS total FROM spiderman_proxy_links"))
    total = int(total_result.scalar_one() or 0)

    rows_result = await db.execute(
        text(
            """
            SELECT
                id,
                name,
                url,
                is_active,
                created_by,
                created_at,
                updated_at
            FROM spiderman_proxy_links
            ORDER BY created_at DESC, id DESC
            LIMIT :limit
            OFFSET :offset
            """
        ),
        {'limit': safe_page_size, 'offset': offset},
    )
    items = [_row_to_proxy_link(row) for row in rows_result.mappings().all()]
    return total, items


async def set_proxy_link_active(db: AsyncSession, link_id: str, is_active: bool) -> bool:
    await ensure_proxy_tables()
    result = await db.execute(
        text(
            """
            UPDATE spiderman_proxy_links
            SET
                is_active = :is_active,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :id
            """
        ),
        {
            'id': link_id,
            'is_active': bool(is_active),
        },
    )
    await db.commit()
    return bool(result.rowcount)


async def delete_proxy_link(db: AsyncSession, link_id: str) -> bool:
    await ensure_proxy_tables()
    await db.execute(
        text("DELETE FROM spiderman_proxy_issue_history WHERE proxy_link_id = :id"),
        {'id': link_id},
    )
    result = await db.execute(
        text("DELETE FROM spiderman_proxy_links WHERE id = :id"),
        {'id': link_id},
    )
    await db.commit()
    return bool(result.rowcount)


async def get_proxy_link_counts(db: AsyncSession) -> tuple[int, int]:
    await ensure_proxy_tables()
    total_result = await db.execute(text("SELECT COUNT(*) FROM spiderman_proxy_links"))
    active_result = await db.execute(text("SELECT COUNT(*) FROM spiderman_proxy_links WHERE is_active = TRUE"))
    return int(total_result.scalar_one() or 0), int(active_result.scalar_one() or 0)


async def check_proxy_batch_limits(
    db: AsyncSession,
    user_telegram_id: int,
    *,
    apply_cooldown: bool = True,
) -> tuple[bool, int, int]:
    await ensure_proxy_tables()
    now = datetime.now(UTC)
    result = await db.execute(
        text(
            """
            SELECT
                MAX(issued_at) AS last_issued_at,
                COUNT(DISTINCT batch_id) AS daily_batches
            FROM spiderman_proxy_issue_history
            WHERE
                user_telegram_id = :user_telegram_id
                AND issued_at >= (CURRENT_TIMESTAMP - INTERVAL '1 day')
            """
        ),
        {
            'user_telegram_id': user_telegram_id,
        },
    )
    row = result.mappings().first() or {}

    last_issued_at = _coerce_datetime(row.get('last_issued_at'))
    daily_batches = int(row.get('daily_batches') or 0)
    remaining_daily = max(0, PROXY_BATCH_DAILY_LIMIT - daily_batches)

    if daily_batches >= PROXY_BATCH_DAILY_LIMIT:
        return False, 0, remaining_daily

    if apply_cooldown and last_issued_at is not None:
        cooldown_deadline = last_issued_at + timedelta(seconds=PROXY_BATCH_COOLDOWN_SECONDS)
        if cooldown_deadline > now:
            wait_seconds = int((cooldown_deadline - now).total_seconds()) + 1
            return False, max(1, wait_seconds), remaining_daily

    return True, 0, remaining_daily


async def _get_active_proxy_links(db: AsyncSession) -> list[ProxyLink]:
    result = await db.execute(
        text(
            """
            SELECT
                id,
                name,
                url,
                is_active,
                created_by,
                created_at,
                updated_at
            FROM spiderman_proxy_links
            WHERE is_active = TRUE
            """
        )
    )
    return [_row_to_proxy_link(row) for row in result.mappings().all()]


async def _get_user_seen_proxy_ids(db: AsyncSession, user_telegram_id: int) -> set[str]:
    result = await db.execute(
        text(
            """
            SELECT DISTINCT proxy_link_id
            FROM spiderman_proxy_issue_history
            WHERE user_telegram_id = :user_telegram_id
            """
        ),
        {'user_telegram_id': user_telegram_id},
    )
    return {str(value) for value in result.scalars().all()}


async def issue_proxy_batch(
    db: AsyncSession,
    *,
    user_telegram_id: int,
    batch_size: int = PROXY_BATCH_SIZE,
) -> tuple[str, list[ProxyLink]]:
    await ensure_proxy_tables()

    active_links = await _get_active_proxy_links(db)
    if not active_links:
        return '', []

    seen_ids = await _get_user_seen_proxy_ids(db, user_telegram_id)

    unseen_links = [link for link in active_links if link.id not in seen_ids]
    selected: list[ProxyLink] = []
    take_from_unseen = min(batch_size, len(unseen_links))
    if take_from_unseen > 0:
        selected.extend(_SYSTEM_RANDOM.sample(unseen_links, k=take_from_unseen))

    if len(selected) < batch_size:
        selected_ids = {link.id for link in selected}
        remaining_links = [link for link in active_links if link.id not in selected_ids]
        need_more = min(batch_size - len(selected), len(remaining_links))
        if need_more > 0:
            selected.extend(_SYSTEM_RANDOM.sample(remaining_links, k=need_more))

    if not selected:
        return '', []

    batch_id = _generate_id()

    for link in selected:
        await db.execute(
            text(
                """
                INSERT INTO spiderman_proxy_issue_history (
                    id,
                    user_telegram_id,
                    batch_id,
                    proxy_link_id
                )
                VALUES (
                    :id,
                    :user_telegram_id,
                    :batch_id,
                    :proxy_link_id
                )
                """
            ),
            {
                'id': _generate_id(),
                'user_telegram_id': user_telegram_id,
                'batch_id': batch_id,
                'proxy_link_id': link.id,
            },
        )
    await db.commit()

    return batch_id, selected


async def get_batch_links(
    db: AsyncSession,
    *,
    user_telegram_id: int,
    batch_id: str,
) -> list[ProxyLink]:
    await ensure_proxy_tables()
    result = await db.execute(
        text(
            """
            SELECT
                l.id,
                l.name,
                l.url,
                l.is_active,
                l.created_by,
                l.created_at,
                l.updated_at
            FROM spiderman_proxy_issue_history h
            JOIN spiderman_proxy_links l ON l.id = h.proxy_link_id
            WHERE
                h.user_telegram_id = :user_telegram_id
                AND h.batch_id = :batch_id
            ORDER BY h.issued_at ASC
            """
        ),
        {
            'user_telegram_id': user_telegram_id,
            'batch_id': batch_id,
        },
    )
    return [_row_to_proxy_link(row) for row in result.mappings().all()]


async def get_batch_link(
    db: AsyncSession,
    *,
    user_telegram_id: int,
    batch_id: str,
    link_id: str,
) -> ProxyLink | None:
    await ensure_proxy_tables()
    result = await db.execute(
        text(
            """
            SELECT
                l.id,
                l.name,
                l.url,
                l.is_active,
                l.created_by,
                l.created_at,
                l.updated_at
            FROM spiderman_proxy_issue_history h
            JOIN spiderman_proxy_links l ON l.id = h.proxy_link_id
            WHERE
                h.user_telegram_id = :user_telegram_id
                AND h.batch_id = :batch_id
                AND h.proxy_link_id = :link_id
            LIMIT 1
            """
        ),
        {
            'user_telegram_id': user_telegram_id,
            'batch_id': batch_id,
            'link_id': link_id,
        },
    )
    row = result.mappings().first()
    return _row_to_proxy_link(row) if row else None


def _get_stats_period_start(period_key: str) -> datetime | None:
    now = datetime.now(UTC)
    normalized = (period_key or '').strip().lower()
    if normalized == 'today':
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if normalized == '7d':
        return now - timedelta(days=7)
    if normalized == '30d':
        return now - timedelta(days=30)
    return None


async def get_proxy_stats(
    db: AsyncSession,
    *,
    period_key: str,
) -> dict[str, ProxyMetric]:
    start_at = _get_stats_period_start(period_key)
    # In production, callback clicks are consistently tracked by middleware
    # with button_id=callback_data. Use those values as the source of truth.
    event_filters = {
        PROXY_EVENT_SCREEN_OPEN: ('menu_free_proxy', False),
        PROXY_EVENT_GET_BATCH_CLICK: ('proxy_get_batch', False),
        PROXY_EVENT_LINK_CLICK: ('proxy_click:', True),
        PROXY_EVENT_NOT_WORKING_OPEN: ('proxy_not_working:', True),
        PROXY_EVENT_NOT_WORKING_SUBMIT: ('proxy_not_working_select:', True),
    }

    stats: dict[str, ProxyMetric] = {}
    for event_id, (value, is_prefix) in event_filters.items():
        query = select(
            func.count(ButtonClickLog.id).label('total'),
            func.count(func.distinct(ButtonClickLog.user_id)).label('unique_users'),
        )
        if is_prefix:
            query = query.where(ButtonClickLog.button_id.like(f'{value}%'))
        else:
            query = query.where(ButtonClickLog.button_id == value)
        if start_at is not None:
            query = query.where(ButtonClickLog.clicked_at >= start_at)

        result = await db.execute(query)
        row = result.first()
        stats[event_id] = ProxyMetric(
            total=int(row.total or 0) if row else 0,
            unique_users=int(row.unique_users or 0) if row else 0,
        )

    return stats
