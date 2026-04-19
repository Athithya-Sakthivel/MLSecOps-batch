from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

try:
    import asyncpg
except Exception as exc:  # pragma: no cover
    asyncpg = None  # type: ignore[assignment]
    _ASYNC_PG_IMPORT_ERROR = exc
else:
    _ASYNC_PG_IMPORT_ERROR = None

Pool = asyncpg.Pool if asyncpg is not None else Any


def _now() -> datetime:
    return datetime.now(UTC)


def _as_dict(row: Any | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(row)


def _identity_get(identity: Any, key: str, default: Any = None) -> Any:
    if identity is None:
        return default
    if isinstance(identity, Mapping):
        return identity.get(key, default)
    if hasattr(identity, key):
        return getattr(identity, key)
    try:
        return identity[key]  # type: ignore[index]
    except Exception:
        return default


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _ensure_provider_fields(
    *,
    provider: str,
    sub: str,
    email: str | None,
    name: str | None,
    tenant_id: str | None,
) -> tuple[str, str, str | None, str | None, str | None]:
    provider = provider.strip().lower()
    sub = sub.strip()
    if not provider:
        raise ValueError("provider is required")
    if not sub:
        raise ValueError("provider subject is required")
    return provider, sub, email, name, tenant_id


async def create_pool(dsn: str, min_size: int, max_size: int) -> Pool:
    if asyncpg is None:
        raise RuntimeError(
            f"asyncpg is required but could not be imported: {_ASYNC_PG_IMPORT_ERROR!r}"
        )
    if min_size <= 0:
        raise ValueError("min_size must be > 0")
    if max_size <= 0:
        raise ValueError("max_size must be > 0")
    if max_size < min_size:
        raise ValueError("max_size must be >= min_size")
    return await asyncpg.create_pool(
        dsn=dsn,
        min_size=min_size,
        max_size=max_size,
        command_timeout=20,
    )


async def init_schema(pool: Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                primary_email TEXT,
                display_name TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_login_at TIMESTAMPTZ
            );
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_identities (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                provider TEXT NOT NULL,
                provider_sub TEXT NOT NULL,
                provider_email TEXT,
                provider_name TEXT,
                tenant_id TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (provider, provider_sub)
            );
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_transactions (
                state TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                code_verifier TEXT NOT NULL,
                nonce TEXT NOT NULL,
                redirect_uri TEXT NOT NULL,
                return_to TEXT NOT NULL DEFAULT '/',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at TIMESTAMPTZ NOT NULL,
                consumed_at TIMESTAMPTZ,
                ip_addr TEXT,
                user_agent TEXT
            );
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_sessions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                provider TEXT,
                provider_sub TEXT,
                provider_email TEXT,
                provider_name TEXT,
                tenant_id TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at TIMESTAMPTZ NOT NULL,
                revoked_at TIMESTAMPTZ,
                last_seen_at TIMESTAMPTZ,
                ip_addr TEXT,
                user_agent TEXT,
                csrf_token TEXT NOT NULL,
                session_version INTEGER NOT NULL DEFAULT 1,
                CONSTRAINT app_sessions_session_version_positive CHECK (session_version > 0)
            );
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_events (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                event_type TEXT NOT NULL,
                provider TEXT,
                user_id TEXT,
                session_id TEXT,
                subject TEXT,
                detail JSONB NOT NULL DEFAULT '{}'::jsonb,
                ip_addr TEXT,
                user_agent TEXT
            );
            """
        )

        # Idempotent upgrades for existing databases.
        await conn.execute("ALTER TABLE app_sessions ADD COLUMN IF NOT EXISTS provider TEXT;")
        await conn.execute("ALTER TABLE app_sessions ADD COLUMN IF NOT EXISTS provider_sub TEXT;")
        await conn.execute("ALTER TABLE app_sessions ADD COLUMN IF NOT EXISTS provider_email TEXT;")
        await conn.execute("ALTER TABLE app_sessions ADD COLUMN IF NOT EXISTS provider_name TEXT;")
        await conn.execute("ALTER TABLE app_sessions ADD COLUMN IF NOT EXISTS tenant_id TEXT;")
        await conn.execute("ALTER TABLE app_sessions ADD COLUMN IF NOT EXISTS csrf_token TEXT;")
        await conn.execute("ALTER TABLE app_sessions ADD COLUMN IF NOT EXISTS session_version INTEGER;")
        await conn.execute(
            "UPDATE app_sessions SET session_version = COALESCE(session_version, 1) WHERE session_version IS NULL;"
        )
        await conn.execute(
            "UPDATE app_sessions SET csrf_token = COALESCE(csrf_token, md5(random()::text || clock_timestamp()::text)) WHERE csrf_token IS NULL;"
        )
        await conn.execute("ALTER TABLE app_sessions ALTER COLUMN csrf_token SET NOT NULL;")
        await conn.execute("ALTER TABLE app_sessions ALTER COLUMN session_version SET DEFAULT 1;")
        await conn.execute("ALTER TABLE app_sessions ALTER COLUMN session_version SET NOT NULL;")

        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_auth_transactions_expires_at ON auth_transactions (expires_at);"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_auth_transactions_consumed_at ON auth_transactions (consumed_at);"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_app_sessions_user_id ON app_sessions (user_id);"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_app_sessions_expires_at ON app_sessions (expires_at);"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_app_sessions_revoked_at ON app_sessions (revoked_at);"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_external_identities_user_id ON external_identities (user_id);"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_audit_events_created_at ON audit_events (created_at DESC);"
        )

        # Light cleanup on startup to keep the tables bounded.
        await conn.execute("DELETE FROM auth_transactions WHERE expires_at < now() - interval '1 day';")
        await conn.execute(
            "DELETE FROM app_sessions WHERE expires_at < now() - interval '7 days' AND revoked_at IS NOT NULL;"
        )


async def prune(pool: Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM auth_transactions WHERE expires_at < now();")
        await conn.execute(
            """
            UPDATE app_sessions
            SET revoked_at = COALESCE(revoked_at, now())
            WHERE expires_at < now()
              AND revoked_at IS NULL
            """
        )


async def audit(
    pool: Pool,
    event_type: str,
    *,
    provider: str | None = None,
    user_id: str | None = None,
    session_id: str | None = None,
    subject: str | None = None,
    detail: dict[str, Any] | None = None,
    ip_addr: str | None = None,
    user_agent: str | None = None,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO audit_events (
                event_type, provider, user_id, session_id, subject, detail, ip_addr, user_agent
            )
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
            """,
            event_type,
            provider,
            user_id,
            session_id,
            subject,
            json.dumps(detail or {}, separators=(",", ":")),
            ip_addr,
            user_agent,
        )


async def insert_auth_transaction(
    pool: Pool,
    *,
    state: str,
    provider: str,
    code_verifier: str,
    nonce: str,
    redirect_uri: str,
    return_to: str,
    expires_at: datetime,
    ip_addr: str | None,
    user_agent: str | None,
) -> None:
    provider = provider.strip().lower()
    state = state.strip()
    code_verifier = code_verifier.strip()
    nonce = nonce.strip()
    redirect_uri = redirect_uri.strip()
    return_to = return_to.strip() or "/"

    if not state:
        raise ValueError("state is required")
    if not provider:
        raise ValueError("provider is required")
    if not code_verifier:
        raise ValueError("code_verifier is required")
    if not nonce:
        raise ValueError("nonce is required")
    if not redirect_uri:
        raise ValueError("redirect_uri is required")
    if expires_at.tzinfo is None:
        raise ValueError("expires_at must be timezone-aware")

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO auth_transactions (
                state, provider, code_verifier, nonce, redirect_uri, return_to,
                created_at, expires_at, consumed_at, ip_addr, user_agent
            )
            VALUES ($1, $2, $3, $4, $5, $6, now(), $7, NULL, $8, $9)
            ON CONFLICT (state) DO NOTHING
            """,
            state,
            provider,
            code_verifier,
            nonce,
            redirect_uri,
            return_to,
            expires_at,
            ip_addr,
            user_agent,
        )


async def consume_auth_transaction(
    pool: Pool,
    state: str,
    *,
    expected_provider: str | None = None,
    expected_redirect_uri: str | None = None,
) -> dict[str, Any]:
    state = state.strip()
    if not state:
        raise ValueError("state is required")

    expected_provider = _clean_optional_text(expected_provider)
    expected_redirect_uri = _clean_optional_text(expected_redirect_uri)

    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM auth_transactions WHERE state = $1 FOR UPDATE",
                state,
            )
            if not row:
                raise ValueError("invalid_state")

            row_dict = dict(row)

            if expected_provider is not None and row_dict["provider"] != expected_provider.lower():
                raise ValueError("provider_mismatch")
            if expected_redirect_uri is not None and row_dict["redirect_uri"] != expected_redirect_uri:
                raise ValueError("redirect_uri_mismatch")
            if row_dict["consumed_at"] is not None:
                raise ValueError("state_consumed")
            if row_dict["expires_at"] < _now():
                raise ValueError("state_expired")

            await conn.execute(
                "UPDATE auth_transactions SET consumed_at = now() WHERE state = $1",
                state,
            )
            row_dict["consumed_at"] = _now()
            return row_dict


async def find_or_create_user(pool: Pool, identity: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    provider = _clean_optional_text(_identity_get(identity, "provider"))
    sub = _clean_optional_text(_identity_get(identity, "sub"))
    email = _clean_optional_text(_identity_get(identity, "email"))
    name = _clean_optional_text(_identity_get(identity, "name"))
    tenant_id = _clean_optional_text(_identity_get(identity, "tenant_id"))

    provider, sub, email, name, tenant_id = _ensure_provider_fields(
        provider=provider or "",
        sub=sub or "",
        email=email,
        name=name,
        tenant_id=tenant_id,
    )

    async with pool.acquire() as conn:
        async with conn.transaction():
            ext = await conn.fetchrow(
                "SELECT * FROM external_identities WHERE provider = $1 AND provider_sub = $2",
                provider,
                sub,
            )

            if ext:
                ext_dict = dict(ext)
                user = await conn.fetchrow("SELECT * FROM users WHERE id = $1", ext_dict["user_id"])
                if not user:
                    raise RuntimeError("dangling_external_identity")

                user_id = dict(user)["id"]
                await conn.execute(
                    """
                    UPDATE users
                    SET primary_email = COALESCE($2, primary_email),
                        display_name = COALESCE(NULLIF($3, ''), display_name),
                        status = 'active',
                        updated_at = now(),
                        last_login_at = now()
                    WHERE id = $1
                    """,
                    user_id,
                    email,
                    name,
                )
                await conn.execute(
                    """
                    UPDATE external_identities
                    SET provider_email = $2,
                        provider_name = NULLIF($3, ''),
                        tenant_id = $4,
                        updated_at = now()
                    WHERE provider = $1 AND provider_sub = $5
                    """,
                    provider,
                    email,
                    name,
                    tenant_id,
                    sub,
                )

                user = await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)
                ext = await conn.fetchrow(
                    "SELECT * FROM external_identities WHERE provider = $1 AND provider_sub = $2",
                    provider,
                    sub,
                )
                return dict(user), dict(ext)

            user_id = str(uuid.uuid4())
            display_name = name or email or sub
            user = await conn.fetchrow(
                """
                INSERT INTO users (
                    id, primary_email, display_name, status, created_at, updated_at, last_login_at
                )
                VALUES ($1, $2, $3, 'active', now(), now(), now())
                RETURNING *
                """,
                user_id,
                email,
                display_name,
            )
            ext = await conn.fetchrow(
                """
                INSERT INTO external_identities (
                    user_id, provider, provider_sub, provider_email, provider_name, tenant_id,
                    created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, now(), now())
                RETURNING *
                """,
                user_id,
                provider,
                sub,
                email,
                name,
                tenant_id,
            )
            return dict(user), dict(ext)


async def create_session(
    pool: Pool,
    *,
    user_id: str,
    ttl_seconds: int,
    ip_addr: str | None,
    user_agent: str | None,
    provider: str | None = None,
    provider_sub: str | None = None,
    provider_email: str | None = None,
    provider_name: str | None = None,
    tenant_id: str | None = None,
    csrf_token: str | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    user_id = str(user_id).strip()
    if not user_id:
        raise ValueError("user_id is required")
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be > 0")

    session_id = _clean_optional_text(session_id) or str(uuid.uuid4())
    csrf_token = _clean_optional_text(csrf_token) or str(uuid.uuid4())
    provider = _clean_optional_text(provider)
    provider_sub = _clean_optional_text(provider_sub)
    provider_email = _clean_optional_text(provider_email)
    provider_name = _clean_optional_text(provider_name)
    tenant_id = _clean_optional_text(tenant_id)
    expires_at = _now() + timedelta(seconds=ttl_seconds)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO app_sessions (
                id, user_id, provider, provider_sub, provider_email, provider_name, tenant_id,
                created_at, expires_at, revoked_at, last_seen_at, ip_addr, user_agent,
                csrf_token, session_version
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, now(), $8, NULL, now(), $9, $10, $11, 1)
            RETURNING *
            """,
            session_id,
            user_id,
            provider,
            provider_sub,
            provider_email,
            provider_name,
            tenant_id,
            expires_at,
            ip_addr,
            user_agent,
            csrf_token,
        )
        return dict(row)


async def touch_session(pool: Pool, session_id: str) -> None:
    session_id = session_id.strip()
    if not session_id:
        raise ValueError("session_id is required")

    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE app_sessions
            SET last_seen_at = now()
            WHERE id = $1
              AND revoked_at IS NULL
              AND expires_at > now()
            """,
            session_id,
        )


async def get_session(pool: Pool, session_id: str) -> dict[str, Any] | None:
    session_id = session_id.strip()
    if not session_id:
        return None

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                s.*,
                u.primary_email AS user_email,
                u.display_name AS user_name,
                u.status AS user_status
            FROM app_sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.id = $1
            """,
            session_id,
        )
        if not row:
            return None

        record = dict(row)
        if record["revoked_at"] is not None:
            return None
        if record["expires_at"] < _now():
            return None
        if record["user_status"] != "active":
            return None
        return record


async def get_active_session(pool: Pool, session_id: str) -> dict[str, Any] | None:
    """
    Canonical active-session lookup for machine validation routes such as /validate.
    Returns the session, user, and provider context only when the session is active.
    """
    return await get_session(pool, session_id)


async def validate_session(pool: Pool, session_id: str) -> dict[str, Any] | None:
    """
    Compatibility alias for callers that want validation semantics rather than a raw read.
    """
    return await get_active_session(pool, session_id)


async def revoke_session(pool: Pool, session_id: str) -> None:
    session_id = session_id.strip()
    if not session_id:
        raise ValueError("session_id is required")

    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE app_sessions
            SET revoked_at = COALESCE(revoked_at, now())
            WHERE id = $1
            """,
            session_id,
        )


async def list_user_sessions(pool: Pool, user_id: str) -> list[dict[str, Any]]:
    user_id = user_id.strip()
    if not user_id:
        return []

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT *
            FROM app_sessions
            WHERE user_id = $1
            ORDER BY created_at DESC
            """,
            user_id,
        )
        return [dict(row) for row in rows]