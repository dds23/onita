"""create tables

Revision ID: 331aa943c231
Revises: 
Create Date: 2023-05-13 11:01:54.140382

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '331aa943c231'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'movies',
        sa.Column('tconst', sa.String, primary_key=True),
        sa.Column('titleType', sa.String, nullable = False),
        sa.Column('primaryTitle', sa.String, nullable = False),
        sa.Column('runtimeMinutes', sa.Integer, nullable = False),
        sa.Column('genres', sa.String, nullable = False)
    )

    op.create_table(
        'ratings',
        sa.Column('tconst', sa.String, primary_key=True),
        sa.Column('averageRating', sa.Float, nullable = False),
        sa.Column('numVotes', sa.Integer, nullable = False)
    )


def downgrade() -> None:
    raise Exception("Downgrade not supported")
