"""crea tabla dq_rejections

Revision ID: 8d084b2e9346
Revises: b7b40ab8fb4b
Create Date: 2026-02-06 18:42:50.330553

"""
from alembic import op
import sqlalchemy as sa

revision = "8d084b2e9346"
down_revision = 'b7b40ab8fb4b'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "dq_rejections",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("reason", sa.String(), nullable=False),
        sa.Column("row_data", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("idx_dq_rejections_created_at", "dq_rejections", ["created_at"])



def downgrade():
    op.drop_index("idx_dq_rejections_created_at", table_name="dq_rejections")
    op.drop_table("dq_rejections")

