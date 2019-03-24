import os

from loguru import logger as LOG

from frames.db import init_db
from frames.config import read_or_make


CONFIG = None
DB = None
DEFAULT_FOLDER = None
DB_PATH = None


def init_frames(folder=None, db_url=None):
    global CONFIG, DB, DEFAULT_FOLDER, DB_PATH

    DEFAULT_FOLDER = folder or os.environ.get('frames_default_folder') or os.path.expanduser('~/.config/frames')

    # Make sure folders exits.
    os.makedirs(DEFAULT_FOLDER, exist_ok=True)
    LOG.add(os.path.join(DEFAULT_FOLDER, 'log.log'), enqueue=True)
    config_file = os.path.join(DEFAULT_FOLDER, 'config.ini')
    CONFIG = read_or_make(config_file)
    DB_PATH = db_url or CONFIG['general']['db']
    # DB is the async database class
    DB = init_db(DB_PATH)
