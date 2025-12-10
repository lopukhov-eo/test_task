import asyncio

from github_repos_scrapper import GithubReposScrapper
from src.config.logger import logger
from config import (
    GITHUB_TOKEN,
    MAX_CONCURRENT_REQUESTS,
    REQUESTS_PER_SECOND,
    REPOSITORIES_PER_PAGE
)

async def main():
    logger.info("Starting GitHub repos scrapper...")
    scrapper = GithubReposScrapper(
        access_token=GITHUB_TOKEN,
        max_concurrent_requests=MAX_CONCURRENT_REQUESTS,
        requests_per_second=REQUESTS_PER_SECOND,
    )

    repos = await scrapper.get_repositories(limit=REPOSITORIES_PER_PAGE)

    for repo in repos[:5]:
        logger.info(repo)

    await scrapper.close()

if __name__ == "__main__":
    asyncio.run(main())
