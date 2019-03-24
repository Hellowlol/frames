from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database
from databases import Database, DatabaseURL


metadata = sa.MetaData()
DB = None
SESS = None
Base = declarative_base()


def showsql(q):
    """Helper to show the sql the db should execute."""
    print('\n')
    x = q.compile(dialect=mysql.dialect(),
                  compile_kwargs={"literal_binds": True})
    print(x)
    print('\n')
    return x


class Mixin():
    id = sa.Column(sa.Integer, primary_key=True)
    tvdbid = sa.Column(sa.Text(length=100))


class Reference_Frame(Mixin, Base):
    __tablename__ = 'reference_frame'
    hash = sa.Column(sa.String(length=16))
    type = sa.Column(sa.String(length=16))  # start or end


class Hashes(Mixin, Base):
    __tablename__ = 'hashes'
    season = sa.Column(sa.Integer)
    episode = sa.Column(sa.Integer)
    hash = sa.Column(sa.Text(length=16))
    offset = sa.Column(sa.Integer)


class Images(Mixin, Base):
    __tablename__ = 'images'
    hash = sa.Column(sa.Text(length=16))
    # https://stackoverflow.com/questions/31849494/serve-image-stored-in-sqlalchemy-largebinary-column
    img = sa.Column(sa.LargeBinary)


# Simply a table where we store the computed hashes
class Intro(Mixin, Base):
    __tablename__ = 'intro'
    season = sa.Column(sa.Integer)
    hashes = sa.Column(sa.JSON)


# Add shortcuts to some tables
# as we are using both core (async via databases) and the orm (sync via cli)
HASHES_T = Hashes.__table__
RFT = Reference_Frame.__table__
IMAGES_T = Images.__table__
INTRO_T = Intro.__table__


def init_db(url):
    """Set up the database."""
    global DB, SESS
    database_url = DatabaseURL(url)
    if database_url.dialect == "mysql":
        url = str(database_url.replace(driver="pymysql"))
    engine = sa.create_engine(url)

    if not database_exists(engine.url):
        create_database(engine.url)

    Base.metadata.create_all(engine)
    DB = Database(url)
    session_factory = sa.orm.sessionmaker(bind=engine)
    SESS = sa.orm.scoped_session(session_factory)
    return DB



@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""  # for sync cli.
    session = SESS()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()