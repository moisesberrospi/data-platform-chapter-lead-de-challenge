"""fix: crea tabla dq_rejections

Revision ID: a7084292dbfe
Revises: 8d084b2e9346
Create Date: 2026-02-06 18:59:38.791885

"""
from alembic import op
import sqlalchemy as sa

revision = "a7084292dbfe"
down_revision = '8d084b2e9346'
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

