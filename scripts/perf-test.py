#!/usr/bin/env python3
"""
Performance and ranking quality evaluation for phonetic text search.

Runs a set of test queries against the FoursquareOSP database and reports:
- Query latency (median, p95, p99)
- Top results for manual ranking quality review
"""

import sys
import time
import statistics
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from garganorn.database import FoursquareOSP

DB_PATH = Path(__file__).resolve().parent.parent / "db" / "fsq-osp.duckdb"

# San Francisco: 37.7749, -122.4194
# Oakland: 37.8044, -122.2712
# San Jose: 37.3382, -121.8863
# Sacramento: 38.5816, -121.4944

QUERIES = {
    # --- Exact name matches ---
    "exact_chain": [
        {"q": "Starbucks"},
        {"q": "Target"},
        {"q": "Safeway"},
        {"q": "Walgreens"},
    ],
    # --- Misspellings / phonetic variants ---
    "misspelled": [
        {"q": "Starbux"},
        {"q": "taco bel"},
        {"q": "cheez cake factory"},
        {"q": "wallgreens"},
        {"q": "Macdonalds"},
        {"q": "Chipolte"},
    ],
    # --- Multi-word ---
    "multi_word": [
        {"q": "Golden Gate Bridge"},
        {"q": "San Francisco Airport"},
        {"q": "Bay Area Rapid Transit"},
        {"q": "Palo Alto Caltrain"},
    ],
    # --- Local / unique names ---
    "local_unique": [
        {"q": "Chez Panisse"},
        {"q": "Tartine Bakery"},
        {"q": "Bi-Rite Creamery"},
        {"q": "Zachary's Chicago Pizza"},
    ],
    # --- Ambiguous / common names ---
    "ambiguous": [
        {"q": "First Baptist Church"},
        {"q": "Pizza Hut"},
        {"q": "Shell Gas Station"},
        {"q": "Community Center"},
    ],
    # --- Spatial + text (near downtown SF) ---
    "spatial_text": [
        {"q": "coffee", "latitude": 37.7749, "longitude": -122.4194},
        {"q": "pizza", "latitude": 37.7749, "longitude": -122.4194},
        {"q": "sushi", "latitude": 37.7749, "longitude": -122.4194},
        {"q": "pharmacy", "latitude": 37.7749, "longitude": -122.4194},
    ],
    # --- Spatial + misspelled text ---
    "spatial_misspelled": [
        {"q": "Starbux", "latitude": 37.7749, "longitude": -122.4194},
        {"q": "wallgreens", "latitude": 37.7749, "longitude": -122.4194},
        {"q": "Macdonalds", "latitude": 37.7749, "longitude": -122.4194},
    ],
    # --- Text-only, single short token ---
    "short_query": [
        {"q": "BART"},
        {"q": "CVS"},
        {"q": "UPS"},
    ],
}


def format_result(r, index):
    """Format a single result for display."""
    name = r.get("names", [{}])[0].get("text", "?")
    loc = r.get("locations", [{}])[0]
    lat = loc.get("latitude", "?")
    lon = loc.get("longitude", "?")
    dist = r.get("distance_m", None)
    attrs = r.get("attributes", {})
    locality = attrs.get("locality", "")
    region = attrs.get("region", "")

    location_str = f"{locality}, {region}" if locality else region or ""
    dist_str = f"  ({dist}m)" if dist and dist > 0 else ""

    return f"  {index:2d}. {name:<45s} {location_str:<25s}{dist_str}"


def run_query(db, params, warmup=False):
    """Run a single query and return (results, elapsed_ms)."""
    kwargs = {}
    if "latitude" in params:
        kwargs["latitude"] = params["latitude"]
        kwargs["longitude"] = params["longitude"]
    if "q" in params:
        kwargs["q"] = params["q"]
    kwargs["limit"] = 10

    start = time.perf_counter()
    results = db.nearest(**kwargs)
    elapsed = (time.perf_counter() - start) * 1000

    if warmup:
        return results, elapsed

    return results, elapsed


def main():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        sys.exit(1)

    db = FoursquareOSP(DB_PATH)
    db.connect()

    print(f"Database: {DB_PATH}")
    print(f"has_name_index: {db.has_name_index}")
    print(f"has_trigram_index: {db.has_trigram_index}")
    print()

    # Warmup: run one query to ensure extensions are loaded, caches warm
    db.nearest(q="warmup query", limit=1)

    all_timings = []
    category_timings = {}

    for category, queries in QUERIES.items():
        print(f"{'=' * 80}")
        print(f"Category: {category}")
        print(f"{'=' * 80}")

        cat_times = []

        for params in queries:
            label = params["q"]
            if "latitude" in params:
                label += f" @ ({params['latitude']}, {params['longitude']})"

            # Run 3 times, take median for timing
            timings = []
            results = None
            for i in range(3):
                results, elapsed = run_query(db, params)
                timings.append(elapsed)

            median_ms = statistics.median(timings)
            cat_times.append(median_ms)
            all_timings.append(median_ms)

            print(f"\n  Query: \"{label}\"")
            print(f"  Time:  {median_ms:.1f}ms (runs: {', '.join(f'{t:.1f}' for t in timings)})")
            print(f"  Results: {len(results)}")

            for i, r in enumerate(results[:5]):
                print(format_result(r, i + 1))

            if len(results) > 5:
                print(f"  ... and {len(results) - 5} more")

        avg = statistics.mean(cat_times)
        category_timings[category] = cat_times
        print(f"\n  Category avg: {avg:.1f}ms")
        print()

    # Summary
    print(f"{'=' * 80}")
    print("SUMMARY")
    print(f"{'=' * 80}")
    print(f"Total queries: {len(all_timings)}")
    print(f"Median:  {statistics.median(all_timings):.1f}ms")
    if len(all_timings) >= 2:
        print(f"Mean:    {statistics.mean(all_timings):.1f}ms")
        print(f"Stdev:   {statistics.stdev(all_timings):.1f}ms")
    sorted_timings = sorted(all_timings)
    p95_idx = int(len(sorted_timings) * 0.95)
    p99_idx = int(len(sorted_timings) * 0.99)
    print(f"P95:     {sorted_timings[min(p95_idx, len(sorted_timings)-1)]:.1f}ms")
    print(f"P99:     {sorted_timings[min(p99_idx, len(sorted_timings)-1)]:.1f}ms")
    print(f"Min:     {sorted_timings[0]:.1f}ms")
    print(f"Max:     {sorted_timings[-1]:.1f}ms")
    print()

    print("By category:")
    for category, times in category_timings.items():
        print(f"  {category:<25s}  avg {statistics.mean(times):6.1f}ms  "
              f"min {min(times):6.1f}ms  max {max(times):6.1f}ms")

    db.close()


if __name__ == "__main__":
    main()
