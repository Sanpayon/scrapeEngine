#!/usr/bin/env python3
"""
Citation fetcher module supporting OpenAlex and Semantic Scholar APIs.
Used to enrich paper records with citation counts.
"""

import requests
import time
import json
import logging
from typing import Optional

import aiohttp
import asyncio

MAX_RETRIES = 3

logger = logging.getLogger(__name__)


class CitationFetcher:
    def __init__(self, api_source: str = "openalex", api_key: Optional[str] = None, cache: dict = None):
        self.api_source = api_source.lower()
        self.api_key = api_key
        self.cache = cache or {}
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "CitationFetcher/1.0"
        })

    def get_citation(self, title: str, authors: str = "") -> Optional[int]:
        if title in self.cache:
            return self.cache[title]

        for attempt in range(MAX_RETRIES):
            try:
                if self.api_source == "openalex":
                    count = self._fetch_openalex(title)
                elif self.api_source == "semanticscholar":
                    count = self._fetch_semantic_scholar(title, authors)
                elif self.api_source == "crossref":
                    count = self._fetch_crossref(title)
                else:
                    raise ValueError(f"Unknown API source: {self.api_source}")

                if count is not None:
                    self.cache[title] = count
                return count
            except (requests.exceptions.RequestException, ValueError) as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
        return None

    def _fetch_openalex(self, title: str) -> Optional[int]:
        url = "https://api.openalex.org/works"
        
        params = {
            "filter": f"title.search:{title}",
            "select": "cited_by_count",
            "per_page": 1,
        }
        headers = {
            "Accept": "application/json",
            "User-Agent": "CitationFetcher/1.0 "
        }

        response = self.session.get(url, params=params, headers=headers, timeout=5)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        if results:
            return results[0].get("cited_by_count", 0)
        return 0

    def _fetch_semantic_scholar(self, title: str, authors: str = "") -> Optional[int]:
        url = "https://api.semanticscholar.org/graph/v1/paper/search"
        params = {"query": title, "limit": 1, "fields": "citationCount"}
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["x-api-key"] = self.api_key

        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        papers = data.get("data", [])
        if papers:
            return papers[0].get("citationCount", 0)
        return 0

    def _fetch_crossref(self, title: str) -> Optional[int]:
        url = "https://api.crossref.org/works"
        params = {"query.title": title, "rows": 1}
        headers = {
            "Accept": "application/json",
            "User-Agent": "CitationFetcher/1.0 "
        }

        response = self.session.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        items = data.get("message", {}).get("items", [])
        if items:
            return items[0].get("is-referenced-by-count", 0)
        return 0

    def fetch_batch(self, papers: list, show_progress: bool = True) -> list:
        total = len(papers)
        for i, paper in enumerate(papers):
            citation = self.get_citation(paper.get("paper_name", ""), paper.get("paper_authors", ""))
            paper["citation"] = citation if citation is not None else None

            if show_progress and (i + 1) % 50 == 0:
                logger.info(f"  Processed {i + 1}/{total} papers...")

        return papers


class AsyncCitationFetcher(CitationFetcher):
    async def _fetch_openalex_async(self, session, title, sem, api_key=None):
        async with sem:
            url = "https://api.openalex.org/works"
            headers = {
                "Accept": "application/json",
                "User-Agent": "CitationFetcher/1.0 "
            }
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"

            params = {
                "filter": f"title.search:{title}",
                "select": "cited_by_count",
                "per_page": 1,
            }
            try:
                async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        results = data.get("results", [])
                        return results[0].get("cited_by_count", 0) if results else 0
                    elif resp.status == 404:
                        return 0
                    else:
                        logger.warning(f"OpenAlex API error: {resp.status} for title={title[:50]}")
                        return None
            except Exception as e:
                logger.warning(f"OpenAlex request failed for {title[:50]}: {e}")
                return None

    async def fetch_batch_async(self, papers, concurrency=5, show_progress=True):
        connector = aiohttp.TCPConnector(
            limit=5,
            ttl_dns_cache=300,
            keepalive_timeout=30,
        )
        sem = asyncio.Semaphore(concurrency)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            for p in papers:
                title = p.get("paper_name", "")
                if not title:
                    tasks.append(asyncio.sleep(0))
                    continue
                tasks.append(self._fetch_openalex_async(session, title, sem, self.api_key))
            citations = await asyncio.gather(*tasks)

        for p, cit in zip(papers, citations):
            p["citation"] = cit if cit is not None else None

        if show_progress:
            total = len(papers)
            updated = sum(1 for p in papers if p.get("citation") is not None)
            logger.info(f"  Citation update done: {updated}/{total} papers updated")

        return papers


def fetch_citations_from_file(input_file: str, output_file: str,
                              api_source: str = "openalex",
                              api_key: Optional[str] = None):
    with open(input_file, "r", encoding="utf-8") as f:
        papers = json.load(f)

    logger.info(f"Loaded {len(papers)} papers from {input_file}")
    logger.info(f"Using API source: {api_source}")

    fetcher = CitationFetcher(api_source=api_source, api_key=api_key)
    enriched = fetcher.fetch_batch(papers)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved {len(enriched)} papers with citations to {output_file}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch citation counts for papers")
    parser.add_argument("input_file", help="Input JSON file with paper records")
    parser.add_argument("--output", "-o", default=None, help="Output JSON file (default: input_file_enriched.json)")
    parser.add_argument("--api-source", "-s", choices=["openalex", "semanticscholar", "crossref"],
                        default="openalex", help="API source to use (default: openalex)")
    parser.add_argument("--api-key", "-k", default=None, help="API key for Semantic Scholar (optional)")

    args = parser.parse_args()
    output_file = args.output or args.input_file.replace(".json", "_enriched.json")

    fetch_citations_from_file(args.input_file, output_file, args.api_source, args.api_key)
