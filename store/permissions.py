"""
Permissions helpers — share/unshare objects between users.
All operations run as the object owner (enforced by RLS).
"""

import psycopg2


def share_read(conn, obj_id, to_user):
    """Grant read access on an object to another user.
    Only the owner (or a writer) can do this — RLS enforces."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE objects
            SET readers = array_append(readers, %s),
                updated_at = now()
            WHERE id = %s
              AND NOT (%s = ANY(readers))
            RETURNING id
            """,
            (to_user, obj_id, to_user),
        )
        return cur.fetchone() is not None


def share_write(conn, obj_id, to_user):
    """Grant read+write access on an object to another user.
    Only the owner (or a writer) can do this — RLS enforces."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE objects
            SET writers = array_append(writers, %s),
                updated_at = now()
            WHERE id = %s
              AND NOT (%s = ANY(writers))
            RETURNING id
            """,
            (to_user, obj_id, to_user),
        )
        return cur.fetchone() is not None


def unshare_read(conn, obj_id, from_user):
    """Revoke read access from a user."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE objects
            SET readers = array_remove(readers, %s),
                updated_at = now()
            WHERE id = %s
            RETURNING id
            """,
            (from_user, obj_id),
        )
        return cur.fetchone() is not None


def unshare_write(conn, obj_id, from_user):
    """Revoke write access from a user."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE objects
            SET writers = array_remove(writers, %s),
                updated_at = now()
            WHERE id = %s
            RETURNING id
            """,
            (from_user, obj_id),
        )
        return cur.fetchone() is not None


def list_shared_with(conn, obj_id):
    """List who has read/write access to an object.
    Returns (readers, writers) tuple. Only visible if you can see the row."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT readers, writers FROM objects WHERE id = %s",
            (obj_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return {"readers": row[0] or [], "writers": row[1] or []}
