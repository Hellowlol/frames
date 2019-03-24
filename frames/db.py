from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database
from databases import Database, DatabaseURL

#import logging
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


metadata = sa.MetaData()
DB = None
SESS = None
Base = declarative_base()

def showsql(q):
    print('\n')
    x =  q.compile(dialect=mysql.dialect(),
                   compile_kwargs={"literal_binds": True})
    print(x)
    print('\n')
    return x



class Reference_Frame(Base):
    __tablename__ = 'reference_frame'
    id = sa.Column(sa.Integer, primary_key=True)
    hex = sa.Column(sa.String(length=16))
    type = sa.Column(sa.String(length=16)) # start or end
    tvdbid = sa.Column(sa.String(16), nullable=True)



class Hashes(Base):
    __tablename__ = 'hashes'
    
    id = sa.Column(sa.Integer, primary_key=True)
    season = sa.Column(sa.Integer)
    episode = sa.Column(sa.Integer)
    tvdbid = sa.Column(sa.Text(length=100))
    hash = sa.Column(sa.Text(length=16))
    offset = sa.Column(sa.Integer)



HASHES_T = Hashes.__table__
RFT = Reference_Frame.__table__

"""
Hashes = sa.Table(
    "hashes",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("tvdbid_season", sa.Text(length=100)),
    sa.Column("tvdbid", sa.Text(length=100)),
    sa.Column('hash', sa.Text(length=16)),
    sa.Column('offset', sa.Integer)
)
"""

"""
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
"""

"""
SELECT * FROM hashes 
WHERE hashes.tvdbid = '79349' 
AND hashes.season = 1 
GROUP by hash, episode
Having count(episode) > 4.8
"""


def init_db(url):
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
    """Provide a transactional scope around a series of operations.""" # for sync cli.
    session = SESS()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()