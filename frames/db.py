from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy_utils import database_exists, create_database
from databases import Database, DatabaseURL


metadata = sa.MetaData()
DB = None
SESS = None


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
    sa.Column('intro_hashes', sa.Text())
)


# Raw images
Images = sa.Table(
    "images",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("hex", sa.Text(length=16)),
    sa.Column("tvdbid", sa.Integer),
    sa.Column("img", sa.LargeBinary)
)

# Referance frame
RefFrame = sa.Table(
    "refframe",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("hex", sa.Text(length=16)),
    sa.Column("tvdbid", sa.Integer),
    sa.Column("img", sa.LargeBinary)
)


def init_db(url):
    global DB, sess
    database_url = DatabaseURL(url)
    if database_url.dialect == "mysql":
        url = str(database_url.replace(driver="pymysql"))
    engine = sa.create_engine(url)

    if not database_exists(engine.url):
        create_database(engine.url)
    metadata.create_all(engine)
    DB = Database(url)
    session_factory = sa.orm.sessionmaker(bind=engine)
    SESS = sa.orm.scoped_session(session_factory)
    return DB



@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations.""" # for sync cli.
    session = sess()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()