from __future__ import annotations

from sqlalchemy import text


async def add_subscription_tariff_columns(
    *,
    engine,
    check_column_exists,
    get_database_type,
    logger,
) -> bool:
    tariff_exists = await check_column_exists("subscriptions", "tariff_code")
    uuid_exists = await check_column_exists("subscriptions", "remnawave_uuid")

    if tariff_exists and uuid_exists:
        logger.info("Subscription columns tariff_code/remnawave_uuid already exist [SPIDERTEST]")
        return True

    try:
        async with engine.begin() as conn:
            db_type = await get_database_type()

            if not tariff_exists:
                if db_type == "sqlite":
                    await conn.execute(
                        text(
                            "ALTER TABLE subscriptions "
                            "ADD COLUMN tariff_code TEXT NOT NULL DEFAULT 'standard'"
                        )
                    )
                elif db_type == "postgresql":
                    await conn.execute(
                        text(
                            "ALTER TABLE subscriptions "
                            "ADD COLUMN tariff_code VARCHAR(20) NOT NULL DEFAULT 'standard'"
                        )
                    )
                elif db_type == "mysql":
                    await conn.execute(
                        text(
                            "ALTER TABLE subscriptions "
                            "ADD COLUMN tariff_code VARCHAR(20) NOT NULL DEFAULT 'standard'"
                        )
                    )
                else:
                    logger.error("Unsupported DB type for tariff_code: %s", db_type)
                    return False

            if not uuid_exists:
                if db_type == "sqlite":
                    await conn.execute(
                        text("ALTER TABLE subscriptions ADD COLUMN remnawave_uuid TEXT")
                    )
                elif db_type == "postgresql":
                    await conn.execute(
                        text("ALTER TABLE subscriptions ADD COLUMN remnawave_uuid VARCHAR(255)")
                    )
                elif db_type == "mysql":
                    await conn.execute(
                        text("ALTER TABLE subscriptions ADD COLUMN remnawave_uuid VARCHAR(255)")
                    )
                else:
                    logger.error("Unsupported DB type for remnawave_uuid: %s", db_type)
                    return False

            await conn.execute(
                text(
                    "UPDATE subscriptions SET tariff_code = 'standard' "
                    "WHERE tariff_code IS NULL OR tariff_code = ''"
                )
            )

            if await check_column_exists("users", "remnawave_uuid"):
                if db_type == "postgresql":
                    await conn.execute(
                        text(
                            "UPDATE subscriptions AS s "
                            "SET remnawave_uuid = u.remnawave_uuid "
                            "FROM users AS u "
                            "WHERE s.user_id = u.id AND s.remnawave_uuid IS NULL"
                        )
                    )
                else:
                    await conn.execute(
                        text(
                            "UPDATE subscriptions "
                            "SET remnawave_uuid = ("
                            "SELECT remnawave_uuid FROM users WHERE users.id = subscriptions.user_id"
                            ") "
                            "WHERE remnawave_uuid IS NULL"
                        )
                    )

            if db_type == "postgresql":
                await conn.execute(
                    text(
                        "ALTER TABLE subscriptions "
                        "DROP CONSTRAINT IF EXISTS subscriptions_user_id_key"
                    )
                )
                await conn.execute(text("DROP INDEX IF EXISTS ix_subscriptions_user_id"))
                await conn.execute(text("DROP INDEX IF EXISTS subscriptions_user_id_idx"))

            await conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_subscriptions_user_id_tariff_code "
                    "ON subscriptions(user_id, tariff_code)"
                )
            )
            await conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "uq_subscriptions_remnawave_uuid "
                    "ON subscriptions(remnawave_uuid)"
                )
            )

        logger.info("Subscription columns tariff_code/remnawave_uuid added [SPIDERTEST]")
        return True
    except Exception as exc:
        logger.error("Failed to add subscription tariff columns: %s", exc)
        return False


async def run_spiderman_migrations(
    *,
    engine,
    check_column_exists,
    get_database_type,
    logger,
) -> bool:
    return await add_subscription_tariff_columns(
        engine=engine,
        check_column_exists=check_column_exists,
        get_database_type=get_database_type,
        logger=logger,
    )
