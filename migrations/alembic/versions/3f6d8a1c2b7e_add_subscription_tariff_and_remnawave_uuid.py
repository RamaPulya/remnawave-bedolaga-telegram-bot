"""add subscription tariff and remnawave uuid

Revision ID: 3f6d8a1c2b7e
Revises: 1b2e3d4f5a6b
Create Date: 2025-01-01 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector


revision: str = "3f6d8a1c2b7e"
down_revision: Union[str, None] = "1b2e3d4f5a6b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SUBSCRIPTIONS_TABLE = "subscriptions"
USERS_TABLE = "users"
USER_ID_COLUMN = "user_id"
TARIFF_CODE_COLUMN = "tariff_code"
REMNAWAVE_UUID_COLUMN = "remnawave_uuid"

UQ_USER_TARIFF = "uq_subscriptions_user_id_tariff_code"
UQ_REMNAWAVE_UUID = "uq_subscriptions_remnawave_uuid"


def _table_exists(inspector: Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    columns = {col["name"] for col in inspector.get_columns(table_name)}
    return column_name in columns


def _find_unique_constraint(inspector: Inspector, table_name: str, columns: list[str]) -> str | None:
    for constraint in inspector.get_unique_constraints(table_name):
        constraint_columns = constraint.get("column_names") or []
        if constraint_columns == columns:
            return constraint.get("name")
    return None


def _unique_constraint_exists(
    inspector: Inspector,
    table_name: str,
    name: str,
    columns: list[str],
) -> bool:
    for constraint in inspector.get_unique_constraints(table_name):
        if constraint.get("name") == name:
            return True
        if (constraint.get("column_names") or []) == columns:
            return True
    return False


def _find_unique_index(inspector: Inspector, table_name: str, columns: list[str]) -> str | None:
    for index in inspector.get_indexes(table_name):
        if not index.get("unique"):
            continue
        if (index.get("column_names") or []) == columns:
            return index.get("name")
    return None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _table_exists(inspector, SUBSCRIPTIONS_TABLE):
        return

    has_tariff_code = _column_exists(inspector, SUBSCRIPTIONS_TABLE, TARIFF_CODE_COLUMN)
    has_remnawave_uuid = _column_exists(inspector, SUBSCRIPTIONS_TABLE, REMNAWAVE_UUID_COLUMN)

    user_id_unique_constraint = _find_unique_constraint(
        inspector, SUBSCRIPTIONS_TABLE, [USER_ID_COLUMN]
    )
    user_id_unique_index = _find_unique_index(
        inspector, SUBSCRIPTIONS_TABLE, [USER_ID_COLUMN]
    )

    with op.batch_alter_table(SUBSCRIPTIONS_TABLE) as batch_op:
        if not has_tariff_code:
            batch_op.add_column(
                sa.Column(
                    TARIFF_CODE_COLUMN,
                    sa.String(length=20),
                    nullable=False,
                    server_default="standard",
                )
            )
        if not has_remnawave_uuid:
            batch_op.add_column(
                sa.Column(
                    REMNAWAVE_UUID_COLUMN,
                    sa.String(length=255),
                    nullable=True,
                )
            )

        if user_id_unique_constraint:
            batch_op.drop_constraint(user_id_unique_constraint, type_="unique")
        elif user_id_unique_index:
            batch_op.drop_index(user_id_unique_index)

        if not _unique_constraint_exists(
            inspector,
            SUBSCRIPTIONS_TABLE,
            UQ_USER_TARIFF,
            [USER_ID_COLUMN, TARIFF_CODE_COLUMN],
        ):
            batch_op.create_unique_constraint(
                UQ_USER_TARIFF,
                [USER_ID_COLUMN, TARIFF_CODE_COLUMN],
            )

        if not _unique_constraint_exists(
            inspector,
            SUBSCRIPTIONS_TABLE,
            UQ_REMNAWAVE_UUID,
            [REMNAWAVE_UUID_COLUMN],
        ):
            batch_op.create_unique_constraint(
                UQ_REMNAWAVE_UUID,
                [REMNAWAVE_UUID_COLUMN],
            )

    if _column_exists(inspector, SUBSCRIPTIONS_TABLE, TARIFF_CODE_COLUMN):
        op.execute(
            sa.text(
                "UPDATE subscriptions SET tariff_code = 'standard' "
                "WHERE tariff_code IS NULL"
            )
        )

    if (
        _column_exists(inspector, USERS_TABLE, REMNAWAVE_UUID_COLUMN)
        and _column_exists(inspector, SUBSCRIPTIONS_TABLE, REMNAWAVE_UUID_COLUMN)
    ):
        if bind.dialect.name == "postgresql":
            op.execute(
                sa.text(
                    "UPDATE subscriptions AS s "
                    "SET remnawave_uuid = u.remnawave_uuid "
                    "FROM users AS u "
                    "WHERE s.user_id = u.id AND s.remnawave_uuid IS NULL"
                )
            )
        else:
            op.execute(
                sa.text(
                    "UPDATE subscriptions "
                    "SET remnawave_uuid = ("
                    "SELECT remnawave_uuid FROM users WHERE users.id = subscriptions.user_id"
                    ") "
                    "WHERE remnawave_uuid IS NULL"
                )
            )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _table_exists(inspector, SUBSCRIPTIONS_TABLE):
        return

    user_tariff_constraint = _find_unique_constraint(
        inspector, SUBSCRIPTIONS_TABLE, [USER_ID_COLUMN, TARIFF_CODE_COLUMN]
    )
    remnawave_uuid_constraint = _find_unique_constraint(
        inspector, SUBSCRIPTIONS_TABLE, [REMNAWAVE_UUID_COLUMN]
    )
    user_id_unique_constraint = _find_unique_constraint(
        inspector, SUBSCRIPTIONS_TABLE, [USER_ID_COLUMN]
    )

    with op.batch_alter_table(SUBSCRIPTIONS_TABLE) as batch_op:
        if user_tariff_constraint:
            batch_op.drop_constraint(user_tariff_constraint, type_="unique")
        if remnawave_uuid_constraint:
            batch_op.drop_constraint(remnawave_uuid_constraint, type_="unique")

        if not user_id_unique_constraint:
            batch_op.create_unique_constraint(
                "uq_subscriptions_user_id",
                [USER_ID_COLUMN],
            )

        if _column_exists(inspector, SUBSCRIPTIONS_TABLE, REMNAWAVE_UUID_COLUMN):
            batch_op.drop_column(REMNAWAVE_UUID_COLUMN)
        if _column_exists(inspector, SUBSCRIPTIONS_TABLE, TARIFF_CODE_COLUMN):
            batch_op.drop_column(TARIFF_CODE_COLUMN)
