#!/usr/bin/env python
# coding: utf-8

# Prepare scraping

import os
from datetime import datetime
import pandas as pd
import requests

SOURCES_PATH = os.path.join("conf", "test-sources.csv")
STORAGE_PATH = os.path.join("output", "data-lake")
os.makedirs(STORAGE_PATH, exist_ok=True)


# Current date as string
now = datetime.now()
now_str = now.strftime("%Y-%m-%d")

def scrape_website(name, url):
    response = requests.get(url, allow_redirects=True)
    html_file_name = os.path.join(
        STORAGE_PATH,
        f"{now_str}-{name}.html",
    )
    with open(html_file_name, "wb") as f:
        f.write(response.content)
    log_info = dict(
        name=name,
        date=now_str,
        file_name=html_file_name,
        status=response.status_code,
        original_url=url,
        final_url=response.url,
        encoding=response.encoding,
    )
    return log_info

def scrape_wrapper(newspaper):
    url = newspaper["url"]
    name = newspaper["id"]
    try:
        log_info = scrape_website(name, url)
        print(f"[INFO] Scraped {name} ({url})")
    except:
        log_info = dict(
            name=name,
            url=url,
            status="error"
        )
        print(f"[ERROR] Failed to scrape: {name} ({url})")
    return pd.Series(log_info)

def main():
    web_sources = pd.read_csv(SOURCES_PATH)
    log_df = web_sources.apply(scrape_wrapper, axis=1)

    log_file_name = os.path.join(
        STORAGE_PATH,
        f"{now_str}.csv",
    )
    log_df.to_csv(log_file_name)

if __name__ == "__main__":
    main()