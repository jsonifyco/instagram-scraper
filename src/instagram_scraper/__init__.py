"""Instagram scraper -- an `apify/instagram-scraper` work-alike built on getbro.ws.

Quick start::

    from instagram_scraper import build_input, run_scraper

    result = run_scraper(build_input({
        "directUrls": ["https://www.instagram.com/nasa/"],
        "resultsType": "posts",
        "resultsLimit": 30,
    }))
    print(len(result.items))
"""

from .errors import (
    FatalError,
    InputError,
    InstagramError,
    InstagramServerError,
    LoginRequiredError,
    NotFoundError,
    PrivateProfileError,
    ScraperError,
)
from .input_model import ResultsType, ScraperInput, SearchType, build_input, load_input_file
from .runner import RunResult, ScraperRun, run_scraper
from .storage import RunStorage
from .urls import Target, TargetType, parse_target, parse_targets

__version__ = "1.2.0"

__all__ = [
    "__version__",
    "build_input", "load_input_file", "run_scraper",
    "ScraperInput", "ScraperRun", "RunResult", "RunStorage",
    "ResultsType", "SearchType",
    "Target", "TargetType", "parse_target", "parse_targets",
    "ScraperError", "FatalError", "InputError", "InstagramError",
    "InstagramServerError", "LoginRequiredError", "NotFoundError",
    "PrivateProfileError",
]
