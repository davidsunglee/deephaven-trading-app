"""
Permissions helpers — share/unshare entities between users.
Sharing updates readers/writers on ALL versions of an entity.
All operations run as the entity owner (enforced by RLS).
"""

import psycopg2


def share_read(conn, entity_id, to_user):
    """Grant read access on all versions of an entity to another user.
    Only the owner (or a writer) can do this — RLS enforces."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE object_events
            SET readers = array_append(readers, %s)
            WHERE entity_id = %s
              AND NOT (%s = ANY(readers))
            RETURNING entity_id
            """,
            (to_user, entity_id, to_user),
        )
        return cur.fetchone() is not None


def share_write(conn, entity_id, to_user):
    """Grant read+write access on all versions of an entity to another user.
    Only the owner (or a writer) can do this — RLS enforces."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE object_events
            SET writers = array_append(writers, %s)
            WHERE entity_id = %s
              AND NOT (%s = ANY(writers))
            RETURNING entity_id
            """,
            (to_user, entity_id, to_user),
        )
        return cur.fetchone() is not None


def unshare_read(conn, entity_id, from_user):
    """Revoke read access from a user on all versions."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE object_events
            SET readers = array_remove(readers, %s)
            WHERE entity_id = %s
            RETURNING entity_id
            """,
            (from_user, entity_id),
        )
        return cur.fetchone() is not None


def unshare_write(conn, entity_id, from_user):
    """Revoke write access from a user on all versions."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE object_events
            SET writers = array_remove(writers, %s)
            WHERE entity_id = %s
            RETURNING entity_id
            """,
            (from_user, entity_id),
        )
        return cur.fetchone() is not None


def list_shared_with(conn, entity_id):
    """List who has read/write access to an entity (from latest version).
    Returns dict with readers/writers lists. Only visible if you can see the entity."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT readers, writers FROM object_events
            WHERE entity_id = %s
            ORDER BY version DESC LIMIT 1
            """,
            (entity_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return {"readers": row[0] or [], "writers": row[1] or []}
