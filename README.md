# Instagram Scraper: Open-Source Python Alternative to Apify

A powerful, highly scalable Instagram scraper built entirely on the [getbro.ws](https://getbro.ws) stealth browser framework.

This scraper accepts the standard Apify ([`apify/instagram-scraper`](https://apify.com/apify/instagram-scraper)) input fields, writes records in the exact Apify output shape and uses the same `resultsType` vocabulary. What makes it unique is its execution model: every request runs inside a real Chrome hosted remotely in a *bro session*. Each session operates with its own residential IP.

There's no need for local browser installations, no Playwright and no complex system dependencies. **The only dependency is `bro-api-sdk`** (which brings `requests` and `pydantic`).

```bash
export BRO_API_KEY=sk_...
python -m instagram_scraper --url instagram.com/nasa --type details
```

---

## What You Can Scrape

The scraper supports extracting various data types by defining the `resultsType`. 

| Target (`resultsType`) | Description | Requires Account? |
| --- | --- | --- |
| `details` | Profile metadata (follower counts, bio, ID, business info). | No |
| `posts` | Images, videos and carousels from a profile or hashtag grid. | No |
| `reels` | Video reels from a user profile. | No |
| `comments` | Comments and replies on specific posts. | **Yes** |
| `mentions` | Posts where a specific profile is tagged. | **Yes** |
| `stories` | Active stories from a profile (expires in 24h). | **Yes** |

**Note:** 
* For searches beyond direct URLs, you can query by `hashtag`, `user` or `place`.
* Providing cookies is recommended to retrieve better results.

---

## Install

Requires Python 3.10+, the `bro-api-sdk` package and a getbro API key.

```bash
git clone <this repo> && cd Instagram-scraper
cp .env.example .env        # put your BRO_API_KEY in it
python -m pip install -e .  # installs bro-api-sdk and the `instagram-scraper` command
```

If you prefer not to install the package fully, you can use the requirements file:

```bash
python -m pip install -r requirements.txt
PYTHONPATH=src python -m instagram_scraper --help
```

---

## Quick Start CLI Examples

### Anonymous Runs (No Account Required)

```bash
# Get profile metadata for two accounts, saved as CSV
python -m instagram_scraper --url @nasa --url @esa --type details --format csv

# Fetch a hashtag grid and read each post through its embed page
python -m instagram_scraper --url instagram.com/explore/tags/space/ --type posts --limit 30 --enrich

# Run parallel scraping for profiles across 5 concurrent browsers using input JSON
python -m instagram_scraper --input examples/profile_details.json --concurrency 5

# Search profiles by keyword and retrieve their details
python -m instagram_scraper --input examples/search_profiles.json

# Scrape recent posts from a profile with relative date filtering
python -m instagram_scraper --input examples/profile_posts.json

# Scrape video reels from a user profile
python -m instagram_scraper --input examples/reels.json

# Scrape hashtag posts with enabled AI fallback and output directly as CSV
python -m instagram_scraper --input examples/hashtag_posts.json
```

### Logged-in Runs (Requires Account)

To fetch comments, mentions, stories and ranked searches, you must provide valid Instagram cookies (a JSON export of an `instagram.com` session or a bare `sessionid`).

```bash
# Scrape comments from one post
python -m instagram_scraper --input examples/post_comments.json --cookies-file .cookies.json

# Scrape posts where a profile is mentioned or tagged
python -m instagram_scraper --input examples/mentions.json --cookies-file .cookies.json

# Scrape every comment of a post completely, resumable via COMPLETE MODE
python -m instagram_scraper --input examples/comments_complete.json --cookies-file .cookies.json

# Resume the previous run if stopped
python -m instagram_scraper --resume storage/comments-complete --cookies-file .cookies.json
```

---

## Input & Output Basics

You can control the scraper completely via a JSON file compatible with Apify standard inputs.

**`input.json` Example:**
```json
{
  "directUrls": ["https://www.instagram.com/nasa"],
  "resultsType": "posts",
  "resultsLimit": 50,
  "onlyPostsNewerThan": "2026-01-01"
}
```
Run it via: `python -m instagram_scraper --input input.json`

**Where does the data go?**
* **Items**: `storage/datasets/<unique_id>/items.json` (or `.csv` / `.jsonl` depending on format).
* **Summary/Logs**: `storage/key_value_stores/<unique_id>/OUTPUT.json`.
* **Sanitized Input**: `storage/key_value_stores/<unique_id>/INPUT.json`.

---

## Proxies, IPs and Accounts

**Anonymous IP Swapping:** When doing anonymous reads (like profile info), Instagram will frequently block IPs with a login wall. The `getbro` environment handles this automatically by terminating the blocked session, provisioning a new browser with a new residential IP and seamlessly retrying the request. 

**Accounts & Cookies:** For tasks requiring a login, you must supply session cookies. You can pass them as a file (`--cookies-file my_cookies.json`) or directly in your environment/`.env` file by setting `IG_SESSIONID`. When logged in, IP swaps are disabled to prevent triggering Instagram's suspicious login checks.

Even for public data like profile details, providing an Instagram session cookie is highly recommended. Being logged in seamlessly bypasses Instagram's aggressive anonymous login walls. This guarantees you extract the most complete datasets possible.

---

## Documentation

For deeper configurations, architecture details and limitations, see documentation in the `docs/` folder:

* **[Collection Modes](docs/COLLECTION_MODES.md)** - Deep dive into standard data extraction, chronological iteration and resumable long-running scrapes (Complete Mode).
* **[Proxies, Sessions and Concurrency](docs/PROXIES_AND_SESSIONS.md)** - Learn how to control IP rotations, rate limits and task distribution across multiple parallel browsers.
* **[Input & Output Schemas](docs/INPUT_OUTPUT.md)** - Full reference for Apify-compatible input fields, search configurations and JSON output structures.
* **[CLI Reference](docs/CLI_REFERENCE.md)** - Every supported command-line flag and environment variable.
* **[Python API](docs/PYTHON_API.md)** - How to import and run the `instagram_scraper` module programmatically inside your own apps.
* **[Architecture](docs/ARCHITECTURE.md)** - Information on how requests are resolved, fallback options and how the queueing works.
* **[Limits, Costs & Measurements](docs/LIMITS_AND_MEASUREMENTS.md)** - Insights on capacity limits, budgeting API calls and performance benchmark stats.

## License

MIT
