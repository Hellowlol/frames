
from loguru import logger as LOG
from frames.db import init_db
from frames.config import read_or_make

CONFIG = None
DB = None
DEFAULT_FOLDER = None

def init_frames(folder=None):
    global CONFIG, DB

    DEFAULT_FOLDER = folder or os.environ.get('frames_default_folder') or os.path.expanduser('~/.config/frames')

    os.makedirs(DEFAULT_FOLDER, exist_ok=True)

    config_file = os.path.join(DEFAULT_FOLDER, 'config.ini')

    CONFIG = read_or_make(config_file)

    DB = init_db(CONFIG['general']['db'])
    