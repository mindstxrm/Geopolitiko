"""Platform audit_log + users.role (Postgres-friendly).

Revision ID: 20260508_0001
Revises:
Create Date: 2026-05-08

On SQLite, `app.models.init_db` already creates these via ensure_* helpers.
This revision targets Postgres cutover / dual-write experiments.
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "20260508_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name

    # users.role — only if users table already exists (after full schema load)
    insp = sa.inspect(bind)
    tables = set(insp.get_table_names())
    if "users" in tables:
        cols = {c["name"] for c in insp.get_columns("users")}
        if "role" not in cols:
            op.add_column("users", sa.Column("role", sa.Text(), server_default="viewer"))

    if "audit_log" not in tables:
        op.create_table(
            "audit_log",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("created_at", sa.Text(), nullable=False),
            sa.Column("actor", sa.Text()),
            sa.Column("actor_role", sa.Text()),
            sa.Column("action", sa.Text(), nullable=False),
            sa.Column("entity_type", sa.Text()),
            sa.Column("entity_id", sa.Text()),
            sa.Column("detail_json", sa.Text()),
            sa.Column("request_id", sa.Text()),
            sa.Column("ip", sa.Text()),
        )
        op.create_index("idx_audit_created", "audit_log", ["created_at"], unique=False)
        op.create_index("idx_audit_action", "audit_log", ["action", "created_at"], unique=False)

    # dialect reserved for future PG-specific types
    _ = dialect


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    tables = set(insp.get_table_names())
    if "audit_log" in tables:
        op.drop_index("idx_audit_action", table_name="audit_log")
        op.drop_index("idx_audit_created", table_name="audit_log")
        op.drop_table("audit_log")
    if "users" in tables:
        cols = {c["name"] for c in insp.get_columns("users")}
        if "role" in cols:
            op.drop_column("users", "role")
