"""
Database schema: objects table, indexes, RLS policies, and user provisioning.
All DDL runs as app_admin (the table owner).
"""

import psycopg2

GROUP_ROLE = "app_user"
ADMIN_ROLE = "app_admin"


def bootstrap_schema(admin_conn):
    """Create the objects table, indexes, and RLS policies. Idempotent."""
    admin_conn.autocommit = True
    with admin_conn.cursor() as cur:
        # ── Table ────────────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS objects (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                type_name   TEXT NOT NULL,
                owner       TEXT NOT NULL DEFAULT current_user,
                readers     TEXT[] NOT NULL DEFAULT '{}',
                writers     TEXT[] NOT NULL DEFAULT '{}',
                data        JSONB NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

        # ── Indexes ──────────────────────────────────────────────────
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_objects_type
                ON objects (type_name);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_objects_owner
                ON objects (owner);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_objects_data
                ON objects USING GIN (data);
        """)

        # ── Enable RLS ───────────────────────────────────────────────
        cur.execute("ALTER TABLE objects ENABLE ROW LEVEL SECURITY;")

        # Force RLS even for the table owner (app_admin uses its own policy)
        cur.execute("ALTER TABLE objects FORCE ROW LEVEL SECURITY;")

        # ── Drop existing policies (idempotent re-create) ────────────
        for policy in [
            "admin_all", "user_select", "user_insert", "user_update", "user_delete"
        ]:
            cur.execute(f"DROP POLICY IF EXISTS {policy} ON objects;")

        # ── Admin policy: full access ────────────────────────────────
        cur.execute(f"""
            CREATE POLICY admin_all ON objects
                FOR ALL
                TO {ADMIN_ROLE}
                USING (true)
                WITH CHECK (true);
        """)

        # ── User SELECT: owner, or listed in readers, or listed in writers
        cur.execute(f"""
            CREATE POLICY user_select ON objects
                FOR SELECT
                TO {GROUP_ROLE}
                USING (
                    owner = current_user
                    OR current_user = ANY(readers)
                    OR current_user = ANY(writers)
                );
        """)

        # ── User INSERT: can only create rows you own ────────────────
        cur.execute(f"""
            CREATE POLICY user_insert ON objects
                FOR INSERT
                TO {GROUP_ROLE}
                WITH CHECK (owner = current_user);
        """)

        # ── User UPDATE: owner or listed in writers ──────────────────
        cur.execute(f"""
            CREATE POLICY user_update ON objects
                FOR UPDATE
                TO {GROUP_ROLE}
                USING (
                    owner = current_user
                    OR current_user = ANY(writers)
                )
                WITH CHECK (
                    owner = current_user
                    OR current_user = ANY(writers)
                );
        """)

        # ── User DELETE: owner only ──────────────────────────────────
        cur.execute(f"""
            CREATE POLICY user_delete ON objects
                FOR DELETE
                TO {GROUP_ROLE}
                USING (owner = current_user);
        """)

        # ── Grant table permissions to group role ────────────────────
        cur.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON objects TO {GROUP_ROLE};")


def provision_user(admin_conn, username, password):
    """
    Create a new PG role for a user. Zero-trust: NOSUPERUSER, NOCREATEDB,
    NOCREATEROLE, NOBYPASSRLS, LOGIN with password, inherits app_user.
    """
    admin_conn.autocommit = True
    with admin_conn.cursor() as cur:
        # Check if role already exists
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (username,))
        if cur.fetchone() is None:
            # Use format string for role name (can't parameterize identifiers)
            # but validate the username first
            _validate_identifier(username)
            cur.execute(
                f"CREATE ROLE \"{username}\" LOGIN PASSWORD %s "
                f"NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS",
                (password,),
            )
            cur.execute(f"GRANT {GROUP_ROLE} TO \"{username}\";")
        else:
            # Update password
            _validate_identifier(username)
            cur.execute(
                f"ALTER ROLE \"{username}\" PASSWORD %s", (password,)
            )


def _validate_identifier(name):
    """Prevent SQL injection in role names."""
    if not name.isalnum() and not all(c.isalnum() or c == '_' for c in name):
        raise ValueError(f"Invalid identifier: {name!r}")
    if len(name) > 63:
        raise ValueError(f"Identifier too long: {name!r}")
