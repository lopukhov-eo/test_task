import configparser
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

config_path = Path(__file__).resolve().parent.parent / "config.ini"

config = configparser.ConfigParser()
config.read(config_path)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_API_BASE_URL = config["GitHub"]["url_api"]

MAX_CONCURRENT_REQUESTS = int(config["GithubReposScrapper"]["max_concurrent_requests"])
REQUESTS_PER_SECOND = int(config["GithubReposScrapper"]["requests_per_second"])
REPOSITORIES_PER_PAGE = int(config["GithubReposScrapper"]["repositories_per_page"])
