import configparser
import os

from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, '..', '..', 'config.ini')

config = configparser.ConfigParser()
config.read(os.path.abspath(CONFIG_PATH))

MIN_SIZE_POOL = config['database'].getint('min_size_pool')
MAX_SIZE_POOL = config['database'].getint('max_size_pool')
TIMEOUT = config['database'].getint('max_size_pool')

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

DB_CONNECT_STR = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"