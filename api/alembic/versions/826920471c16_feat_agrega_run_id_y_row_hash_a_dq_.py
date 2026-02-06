from alembic import op
import sqlalchemy as sa

revision = "826920471c16"
down_revision = "a7084292dbfe"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("dq_rejections", sa.Column("run_id", sa.String(), nullable=True))
    op.add_column("dq_rejections", sa.Column("row_hash", sa.String(), nullable=True))

    op.execute("UPDATE dq_rejections SET run_id = 'legacy' WHERE run_id IS NULL;")
    op.execute("UPDATE dq_rejections SET row_hash = md5((row_data::text || ':' || id::text)) WHERE row_hash IS NULL;")

    op.alter_column("dq_rejections", "run_id", nullable=False)
    op.alter_column("dq_rejections", "row_hash", nullable=False)

    op.create_index("idx_dq_rejections_run_id", "dq_rejections", ["run_id"])
    op.create_unique_constraint(
        "uq_dq_rejections_run_hash_reason",
        "dq_rejections",
        ["run_id", "row_hash", "reason"],
    )



def downgrade():
    op.drop_constraint("uq_dq_rejections_run_hash_reason", "dq_rejections", type_="unique")
    op.drop_index("idx_dq_rejections_run_id", table_name="dq_rejections")
    op.drop_column("dq_rejections", "row_hash")
    op.drop_column("dq_rejections", "run_id")

