"""One scraper per `resultsType`, plus the search-discovery scraper."""

from ..input_model import ResultsType
from .base import BaseScraper, ScrapeContext, Stats
from .comments import CommentsScraper
from .details import DetailsScraper
from .mentions import MentionsScraper
from .posts import PostsScraper, ReelsScraper
from .search import SearchScraper, hits_to_targets
from .stories import StoriesScraper

#: `resultsType` -> the scraper that serves it
SCRAPERS: dict[ResultsType, type[BaseScraper]] = {
    ResultsType.POSTS: PostsScraper,
    ResultsType.REELS: ReelsScraper,
    ResultsType.COMMENTS: CommentsScraper,
    ResultsType.DETAILS: DetailsScraper,
    ResultsType.MENTIONS: MentionsScraper,
    ResultsType.STORIES: StoriesScraper,
}


def scraper_for(results_type: ResultsType, scrape: ScrapeContext) -> BaseScraper:
    """Instantiate the scraper registered for a `resultsType`."""
    return SCRAPERS[results_type](scrape)


__all__ = [
    "BaseScraper", "ScrapeContext", "Stats", "SCRAPERS", "scraper_for",
    "PostsScraper", "ReelsScraper", "CommentsScraper", "DetailsScraper",
    "MentionsScraper", "StoriesScraper", "SearchScraper", "hits_to_targets",
]
