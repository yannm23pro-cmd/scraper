#!/usr/bin/env python3
"""
Domain Arbitrage SaaS — Scraper
Extracts expired/deleted domains from public sources and pushes them to Supabase.

Usage:
  python scraper.py           # Full run — pushes to Supabase
  python scraper.py --dry-run # Print rows without pushing
"""

import argparse
import json
import logging
import os
import random
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

# ExpiredDomains.net — public list of recently deleted .com domains
SOURCES = [
    "https://www.expireddomains.net/deleted-domains/?start=0",
    "https://www.expireddomains.net/deleted-domains/?start=25",
    "https://www.expireddomains.net/deleted-domains/?start=50",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
]

NICHE_KEYWORDS = {
    "Finance":    ["finance", "invest", "money", "bank", "credit", "forex", "trade", "loan", "wealth", "capital"],
    "Tech":       ["tech", "software", "app", "cloud", "data", "code", "dev", "ai", "saas", "cyber"],
    "Health":     ["health", "med", "care", "fit", "wellness", "pharma", "clinic", "diet", "gym", "bio"],
    "E-commerce": ["shop", "store", "buy", "deal", "market", "sell", "ecom", "boutique", "price", "cart"],
}


# ──────────────────────────────────────────────
# Data model
# ──────────────────────────────────────────────

@dataclass
class Domain:
    name: str
    extension: str
    niche: str = "Other"
    seo_score: int = 0
    age_years: int = 0
    backlinks: int = 0
    affiliate_url: str = ""
    discovered_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def random_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
    }


def detect_niche(domain_name: str) -> str:
    name_lower = domain_name.lower()
    for niche, keywords in NICHE_KEYWORDS.items():
        if any(kw in name_lower for kw in keywords):
            return niche
    return "Other"


def compute_seo_score(age_years: int, backlinks: int) -> int:
    """
    Simple heuristic SEO score (1-100):
      - Age contributes up to 50 pts (capped at 10 years)
      - Backlinks contribute up to 50 pts (log scale, capped at 10 000)
    """
    import math
    age_score = min(age_years / 10, 1.0) * 50
    bl_score = (math.log10(backlinks + 1) / math.log10(10001)) * 50
    score = int(round(age_score + bl_score))
    return max(1, min(score, 100))


def build_affiliate_url(domain: str, extension: str) -> str:
    full = f"{domain}{extension}"
    # GoDaddy affiliate search — replace with your own affiliate link pattern
    return f"https://www.godaddy.com/domainsearch/find?domainToCheck={full}"


def parse_int(text: str) -> int:
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else 0


# ──────────────────────────────────────────────
# Scraping
# ──────────────────────────────────────────────

def scrape_source(url: str) -> List[Domain]:
    log.info(f"Scraping: {url}")
    try:
        resp = requests.get(url, headers=random_headers(), timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning(f"Failed to fetch {url}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    domains: List[Domain] = []

    # ExpiredDomains.net table structure
    table = soup.find("table", class_="base1")
    if not table:
        log.warning("Table 'base1' not found — page structure may have changed.")
        return []

    rows = table.find_all("tr")[1:]  # skip header row
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 5:
            continue
        try:
            raw_domain = cols[0].get_text(strip=True)
            if not raw_domain:
                continue

            # Split name and extension (e.g. "example" + ".com")
            match = re.match(r"^(.+?)(\.[a-z]{2,10})$", raw_domain, re.IGNORECASE)
            if not match:
                continue
            name, extension = match.group(1), match.group(2).lower()

            # Try to grab age and backlinks from table columns (positions vary)
            age_text = cols[3].get_text(strip=True) if len(cols) > 3 else "0"
            bl_text  = cols[4].get_text(strip=True) if len(cols) > 4 else "0"

            age_years = parse_int(age_text)
            backlinks = parse_int(bl_text)

            niche     = detect_niche(name)
            seo_score = compute_seo_score(age_years, backlinks)

            domains.append(Domain(
                name=name,
                extension=extension,
                niche=niche,
                seo_score=seo_score,
                age_years=age_years,
                backlinks=backlinks,
                affiliate_url=build_affiliate_url(name, extension),
            ))
        except Exception as e:
            log.debug(f"Skipping row due to error: {e}")
            continue

    log.info(f"  → Found {len(domains)} domains on this page.")
    return domains


def scrape_all() -> List[Domain]:
    all_domains: List[Domain] = []
    for url in SOURCES:
        all_domains.extend(scrape_source(url))
        time.sleep(random.uniform(2.5, 5.0))  # Polite delay
    # Deduplicate by (name + extension)
    seen = set()
    unique = []
    for d in all_domains:
        key = (d.name.lower(), d.extension)
        if key not in seen:
            seen.add(key)
            unique.append(d)
    log.info(f"Total unique domains scraped: {len(unique)}")
    return unique


# ──────────────────────────────────────────────
# Supabase push
# ──────────────────────────────────────────────

def push_to_supabase(domains: List[Domain]) -> None:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise EnvironmentError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.")

    endpoint = f"{SUPABASE_URL}/rest/v1/domains"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",  # upsert on (name, extension) conflict
    }

    # Supabase REST accepts arrays — send in batches of 50
    BATCH = 50
    total = 0
    for i in range(0, len(domains), BATCH):
        batch = [asdict(d) for d in domains[i : i + BATCH]]
        resp = requests.post(endpoint, headers=headers, data=json.dumps(batch), timeout=20)
        if resp.status_code in (200, 201):
            total += len(batch)
            log.info(f"  ✓ Pushed batch of {len(batch)} domains.")
        else:
            log.error(f"  ✗ Batch push failed: {resp.status_code} — {resp.text[:300]}")

    log.info(f"Done. Total records pushed: {total}/{len(domains)}")


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Domain Arbitrage Scraper")
    parser.add_argument("--dry-run", action="store_true", help="Print rows without pushing to Supabase.")
    args = parser.parse_args()

    domains = scrape_all()

    if args.dry_run:
        log.info("DRY RUN — sample output:")
        for d in domains[:10]:
            print(json.dumps(asdict(d), indent=2, default=str))
        return

    push_to_supabase(domains)


if __name__ == "__main__":
    main()
