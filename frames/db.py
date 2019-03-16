import sqlalchemy as sa
from sqlalchemy_utils import database_exists, create_database
from databases import Database, DatabaseURL


metadata = sa.MetaData()
DB = None


Hashes = sa.Table(
    "hashes",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("tvdbid_season", sa.Text(length=100)),
    sa.Column("tvdbid", sa.Text(length=100)),
    sa.Column('hash', sa.Text(length=16))
)


# The combined hashes should be stored here.
Intro = sa.Table(
    "intro",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("tvdbid_season", sa.Integer),
    sa.Column("tvdbid", sa.Integer),
    sa.Column('intro_hexes', sa.Text())
)


# This table where the damn
Images = sa.Table(
    "images",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("hex", sa.Text(length=16)),
    sa.Column("tvdbid", sa.Integer),
    sa.Column("hex", sa.LargeBinary)
)


RefFrame = sa.Table(
    "refframe",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("hex", sa.Text(length=16)),
    sa.Column("tvdbid", sa.Integer),
    sa.Column("hex", sa.LargeBinary)
)





def init_db(url):
    global DB
    database_url = DatabaseURL(url)
    if database_url.dialect == "mysql":
        url = str(database_url.replace(driver="pymysql"))
    engine = sa.create_engine(url)

    if not database_exists(engine.url):
        create_database(engine.url)
    metadata.create_all(engine)
    DB = Database(url)
    return DB