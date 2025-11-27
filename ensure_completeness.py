"""
Script to retry failed URLs and merge them back into the state JSONL files.

This script:
1. Reads failed.jsonl and filters by state
2. Retries scraping each failed URL
3. Writes successful ones to retry_STATECODE.jsonl
4. Removes successful URLs from failed.jsonl
5. Merges retry file into main STATECODE.jsonl in correct URL order
"""

import argparse
import json
import re
from pathlib import Path
from typing import Optional

import cloudscraper
import tqdm
from bs4 import BeautifulSoup

from scraper_utils import FAILED_FAILPATH, JUR_NAME_MAP

_CODES_DIR = Path("codes")
_REGS_DIR = Path("regs")


def extract_url_parts(url: str) -> tuple[list[str], list[int]]:
    """
    Extract hierarchical parts from a Justia URL for sorting.
    
    Returns:
        tuple of (path_parts, numeric_parts) where:
        - path_parts: list of URL segments (e.g., ['title-14', 'chapter-43', 'section-4303'])
        - numeric_parts: list of extracted numbers (e.g., [14, 43, 4303])
    
    Example:
        /codes/delaware/title-14/chapter-43/section-4303/
        -> (['title-14', 'chapter-43', 'section-4303'], [14, 43, 4303])
    """
    # Remove domain and split path
    path = url.split("//")[-1].split("/", 1)[-1] if "//" in url else url
    parts = [p for p in path.split("/") if p and p not in ("codes", "delaware")]
    
    # Extract numbers from each part for numeric comparison
    numbers = []
    for part in parts:
        nums = re.findall(r"\d+", part)
        numbers.extend([int(n) for n in nums])
    
    return parts, numbers


def url_sort_key(url: str) -> tuple:
    """Generate a sort key for URL ordering."""
    parts, numbers = extract_url_parts(url)
    # Return tuple that sorts first by path structure, then by numbers
    return (len(parts), parts, numbers)


def scrape_single_url(
    url: str, state_name: str, scraper: cloudscraper.CloudScraper, is_reg: bool = False
) -> Optional[dict]:
    """
    Scrape a single URL and return the record if successful.
    
    Args:
        url: The URL to scrape
        state_name: State abbreviation (e.g., 'DE')
        scraper: CloudScraper instance
        is_reg: Whether this is a regulation (vs code)
    
    Returns:
        dict record if successful, None otherwise
    """
    try:
        response = scraper.get(url, timeout=30)
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Extract breadcrumb separator
        sep_element = soup.find("span", class_="breadcrumb-sep")
        if not sep_element:
            return None
        sep = sep_element.get_text(strip=True)
        
        # Extract path
        path_element = soup.find("nav", class_="breadcrumbs")
        if not path_element:
            return None
        path_str = path_element.get_text(strip=True)
        path_arr = path_str.split(sep)
        
        # Extract title
        title_element = soup.find("h1")
        if not title_element:
            return None
        title_str = title_element.get_text(f" {sep} ", strip=True)
        
        # Extract citation info
        has_univ_cite = False
        citation = None
        
        if is_reg:
            if wrapper := soup.find("div", class_="has-margin-bottom-20"):
                has_univ_cite = (
                    wrapper.find("b").get_text(strip=True) == "Universal Citation:"
                )
            if cite_tag := soup.find(href="/citations.html"):
                citation = cite_tag.get_text(strip=True)
        else:
            if wrapper := soup.find("div", class_="citation-wrapper"):
                has_univ_cite = (
                    wrapper.find("strong").get_text(strip=True) == "Universal Citation:"
                )
            if cite_tag := soup.find("div", class_="citation"):
                citation = cite_tag.find("span").get_text(strip=True)
        
        # Extract content
        content_element = soup.find(id="codes-content")
        if not content_element:
            return None
        content = content_element.get_text("\n", strip=True)
        
        return {
            "url": url,
            "state": state_name,
            "path": path_str,
            "title": title_str,
            "univ_cite": has_univ_cite,
            "citation": citation,
            "content": content,
            "lex_path": None,  # Will be inferred during merge
        }
    
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None


def retry_failed_urls(
    state_abb: str, regs: bool = False, max_retries: Optional[int] = None
) -> tuple[list[dict], list[str]]:
    """
    Retry all failed URLs for a given state.
    
    Args:
        state_abb: State abbreviation (e.g., 'DE')
        regs: Whether to process regulations (vs codes)
        max_retries: Maximum number of URLs to retry (None = all)
    
    Returns:
        tuple of (successful_records, still_failed_urls)
    """
    failed_path = Path(FAILED_FAILPATH)
    if not failed_path.exists():
        print(f"No failed URLs file found at {FAILED_FAILPATH}")
        return [], []
    
    # Read all failed URLs
    failed_urls = []
    with open(failed_path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                failed_urls.append(line)
    
    # Filter by state
    state_name = JUR_NAME_MAP.get(state_abb)
    if not state_name:
        print(f"Unknown state abbreviation: {state_abb}")
        return [], []
    
    state_slug = state_name.lower().replace(" ", "-")
    state_urls = [url for url in failed_urls if f"/{state_slug}/" in url.lower()]
    
    if not state_urls:
        print(f"No failed URLs found for {state_abb}")
        return [], []
    
    print(f"Found {len(state_urls)} failed URLs for {state_abb}")
    
    if max_retries:
        state_urls = state_urls[:max_retries]
    
    # Retry scraping
    scraper = cloudscraper.create_scraper()
    successful_records = []
    still_failed = []
    
    for url in tqdm.tqdm(state_urls, desc=f"Retrying {state_abb}"):
        record = scrape_single_url(url, state_abb, scraper, is_reg=regs)
        if record:
            successful_records.append(record)
        else:
            still_failed.append(url)
    
    print(
        f"Successfully scraped {len(successful_records)}/{len(state_urls)} URLs"
    )
    
    return successful_records, still_failed


def update_failed_file(urls_to_remove: list[str]) -> None:
    """
    Remove successfully scraped URLs from failed.jsonl.
    
    Args:
        urls_to_remove: List of URLs to remove from the failed file
    """
    failed_path = Path(FAILED_FAILPATH)
    if not failed_path.exists():
        return
    
    # Read all failed URLs
    with open(failed_path, "r") as f:
        failed_urls = [line.strip() for line in f if line.strip()]
    
    # Remove successful ones
    urls_to_remove_set = set(urls_to_remove)
    remaining = [url for url in failed_urls if url not in urls_to_remove_set]
    
    # Write back
    with open(failed_path, "w") as f:
        for url in remaining:
            f.write(f"{url}\n")
    
    print(f"Removed {len(urls_to_remove)} URLs from {FAILED_FAILPATH}")
    print(f"Remaining failed URLs: {len(remaining)}")


def merge_retry_into_main(
    state_abb: str, retry_records: list[dict], regs: bool = False
) -> None:
    """
    Merge retry records into the main state JSONL file in correct URL order.
    
    Args:
        state_abb: State abbreviation (e.g., 'DE')
        retry_records: List of records to merge in
        regs: Whether processing regulations (vs codes)
    """
    save_dir = _REGS_DIR if regs else _CODES_DIR
    main_path = save_dir / f"{state_abb}.jsonl"
    
    if not main_path.exists():
        print(f"Main file not found: {main_path}")
        return
    
    # Read existing records
    existing_records = []
    with open(main_path, "r") as f:
        for line in f:
            if line.strip():
                existing_records.append(json.loads(line))
    
    print(f"Loaded {len(existing_records)} existing records")
    
    # Combine and sort by URL
    all_records = existing_records + retry_records
    all_records.sort(key=lambda r: url_sort_key(r["url"]))
    
    # Remove duplicates (keep first occurrence)
    seen_urls = set()
    unique_records = []
    for record in all_records:
        if record["url"] not in seen_urls:
            seen_urls.add(record["url"])
            unique_records.append(record)
    
    print(
        f"Total unique records after merge: {len(unique_records)} "
        f"(added {len(unique_records) - len(existing_records)} new)"
    )
    
    # Write back
    with open(main_path, "w") as f:
        for record in unique_records:
            f.write(json.dumps(record))
            f.write("\n")
    
    print(f"Merged records into {main_path}")


def ensure_completeness(
    state_abb: str, regs: bool = False, skip_merge: bool = False
) -> None:
    """
    Main function to ensure completeness for a state.
    
    Args:
        state_abb: State abbreviation (e.g., 'DE')
        regs: Whether to process regulations (vs codes)
        skip_merge: If True, save to retry file but don't merge
    """
    print(f"Ensuring completeness for {state_abb}")
    print("=" * 60)
    
    # Retry failed URLs
    successful_records, still_failed = retry_failed_urls(state_abb, regs=regs)
    
    if not successful_records:
        print("No records were successfully scraped.")
        return
    
    # Save to retry file
    save_dir = _REGS_DIR if regs else _CODES_DIR
    retry_path = save_dir / f"retry_{state_abb}.jsonl"
    
    with open(retry_path, "w") as f:
        for record in successful_records:
            f.write(json.dumps(record))
            f.write("\n")
    
    print(f"Saved {len(successful_records)} records to {retry_path}")
    
    # Update failed.jsonl
    successful_urls = [r["url"] for r in successful_records]
    update_failed_file(successful_urls)
    
    # Merge into main file
    if not skip_merge:
        merge_retry_into_main(state_abb, successful_records, regs=regs)
    else:
        print(f"Skipping merge. Records saved to {retry_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="ensure_completeness.py",
        description="Retry failed URLs and merge them into state JSONL files.",
    )
    parser.add_argument(
        "state",
        type=str,
        help="The state code to process (e.g., DE, CA)",
        choices=list(JUR_NAME_MAP.keys()),
    )
    parser.add_argument(
        "-r",
        "--regs",
        action="store_true",
        help="Process regulations instead of codes.",
    )
    parser.add_argument(
        "--skip-merge",
        action="store_true",
        help="Save to retry file but don't merge into main file.",
    )
    
    args = parser.parse_args()
    ensure_completeness(args.state, regs=args.regs, skip_merge=args.skip_merge)

