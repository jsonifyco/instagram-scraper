## Python API


```python
from instagram_scraper import build_input, run_scraper

result = run_scraper(build_input({
    "directUrls": ["https://www.instagram.com/nasa/"],
    "resultsType": "details",
}))
print(len(result.items), "records, $", result.billing["total"])
print("fallbacks:", dict(result.stats.fallbacks))
```

The layers also work on their own:

```python
from instagram_scraper.bro import BroSession, make_client
from instagram_scraper.ig import IgContext, InstagramApi
from instagram_scraper.mappers import map_profile

client = make_client(api_key)
with BroSession(client, session_kwargs={"enable_proxy": True}) as session:
    ctx = IgContext(session=session, max_ip_rotations=3)  # swaps a walled-off session
    ctx.bootstrap()
    record = map_profile(InstagramApi(ctx).profile("nasa"))
    print(record["followersCount"])
```

---

