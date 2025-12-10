import configparser
from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()

config = configparser.ConfigParser()
config.read('../config.ini')

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_API_BASE_URL = config['GitHub']['url_api']

MAX_CONCURRENT_REQUESTS = int(config['GithubReposScrapper']['max_concurrent_requests'])
REQUESTS_PER_SECOND = int(config['GithubReposScrapper']['requests_per_second'])
REPOSITORIES_PER_PAGE = int(config['GithubReposScrapper']['repositories_per_page'])


@dataclass(slots=True)
class RepositoryAuthorCommitsNum:
    author: str
    commits_num: int


@dataclass(slots=True)
class Repository:
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]