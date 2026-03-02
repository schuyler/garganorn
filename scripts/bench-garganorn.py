#!/usr/bin/env python3
"""Benchmark script for garganorn place search API."""

import argparse
import json
import statistics
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

XRPC_NSID = "community.lexicon.location.searchRecords"

QUERIES = [
    # Spatial only
    {"label": "spatial/tokyo",     "latitude": "35.6762",  "longitude": "139.6503"},
    {"label": "spatial/nyc",       "latitude": "40.7128",  "longitude": "-74.0060"},
    {"label": "spatial/saopaulo",  "latitude": "-23.5505", "longitude": "-46.6333"},
    {"label": "spatial/lagos",     "latitude": "6.5244",   "longitude": "3.3792"},
    {"label": "spatial/sydney",    "latitude": "-33.8688", "longitude": "151.2093"},
    # Text only
    {"label": "text/tokyo",        "q": "Tokyo"},
    {"label": "text/starbucks",    "q": "Starbucks"},
    {"label": "text/hospital",     "q": "hospital"},
    {"label": "text/cafe",         "q": "café"},
    # Combined
    {"label": "combined/starbucks+nyc", "q": "Starbucks", "latitude": "40.7128", "longitude": "-74.0060"},
    {"label": "combined/hospital+lagos", "q": "hospital", "latitude": "6.5244", "longitude": "3.3792"},
    {"label": "combined/cafe+tokyo", "q": "café", "latitude": "35.6762", "longitude": "139.6503"},
]


def build_url(base_url, collection, query):
    """Build the XRPC request URL for a query."""
    params = {"collection": collection}
    for key in ("latitude", "longitude", "q"):
        if key in query:
            params[key] = query[key]
    qs = urllib.parse.urlencode(params)
    return f"{base_url}/xrpc/{XRPC_NSID}?{qs}"


def run_query(url):
    """Execute a single query and return (wall_ms, server_ms, count)."""
    t0 = time.perf_counter()
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as e:
        return None, None, str(e)
    wall_ms = (time.perf_counter() - t0) * 1000
    server_ms = data.get("_query", {}).get("elapsed_ms")
    count = len(data.get("records", []))
    return wall_ms, server_ms, count


def percentile(data, p):
    """Compute the p-th percentile of a sorted list."""
    k = (len(data) - 1) * (p / 100)
    f = int(k)
    c = f + 1
    if c >= len(data):
        return data[f]
    return data[f] + (k - f) * (data[c] - data[f])


def compute_stats(values):
    """Return dict with min, median, p95, mean for a list of numbers."""
    s = sorted(values)
    return {
        "min": round(s[0], 1),
        "median": round(statistics.median(s), 1),
        "p95": round(percentile(s, 95), 1),
        "mean": round(statistics.mean(s), 1),
    }


def run_benchmark(args):
    results = []
    queries = [q for q in QUERIES if not args.filter or q["label"].startswith(args.filter)]
    total = len(queries)

    for qi, query in enumerate(queries, 1):
        label = query["label"]
        url = build_url(args.base_url, args.collection, query)
        wall_times = []
        server_times = []
        result_count = None
        errors = []

        if not args.json_output:
            print(f"[{qi}/{total}] {label} ", end="", flush=True)

        def do_one(_i):
            return run_query(url)

        total_iters = args.warmup + args.iterations
        all_results = []

        if args.concurrency > 1:
            with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                futures = [pool.submit(do_one, i) for i in range(total_iters)]
                for f in as_completed(futures):
                    all_results.append(f.result())
        else:
            for i in range(total_iters):
                all_results.append(do_one(i))

        # Discard warmup iterations (first N)
        measured = all_results[args.warmup:]

        for wall_ms, server_ms, count_or_err in measured:
            if wall_ms is None:
                errors.append(count_or_err)
                continue
            wall_times.append(wall_ms)
            if server_ms is not None:
                server_times.append(server_ms)
            result_count = count_or_err

        entry = {"label": label, "iterations": len(wall_times), "results": result_count}
        if wall_times:
            entry["wall_ms"] = compute_stats(wall_times)
        if server_times:
            entry["server_ms"] = compute_stats(server_times)
        if errors:
            entry["errors"] = len(errors)
            entry["last_error"] = errors[-1]

        results.append(entry)

        if not args.json_output:
            if wall_times:
                w = entry["wall_ms"]
                print(f"wall: {w['median']:.0f}ms (p95: {w['p95']:.0f}ms)", end="")
                if server_times:
                    s = entry["server_ms"]
                    print(f"  server: {s['median']:.0f}ms (p95: {s['p95']:.0f}ms)", end="")
                print(f"  n={result_count}")
            else:
                print(f"FAILED: {errors[-1] if errors else 'unknown'}")

    return results


def print_table(results):
    """Print a summary table to stdout."""
    label_w = max(len(r["label"]) for r in results)
    hdr = f"{'query':<{label_w}}  {'wall med':>8}  {'wall p95':>8}  {'svr med':>8}  {'svr p95':>8}  {'count':>5}"
    print()
    print(hdr)
    print("-" * len(hdr))
    for r in results:
        label = r["label"]
        if "wall_ms" in r:
            w = r["wall_ms"]
            s = r.get("server_ms", {})
            parts = [
                f"{label:<{label_w}}",
                f"{w['median']:>7.0f}ms",
                f"{w['p95']:>7.0f}ms",
                f"{s.get('median', 0):>7.0f}ms" if s else f"{'n/a':>8}",
                f"{s.get('p95', 0):>7.0f}ms" if s else f"{'n/a':>8}",
                f"{r['results']:>5}",
            ]
            print("  ".join(parts))
        else:
            print(f"{label:<{label_w}}  {'ERROR':>8}  {r.get('last_error', '')}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark garganorn place search API")
    parser.add_argument("--base-url", default="https://places.atgeo.org",
                        help="Base URL of the garganorn server (default: %(default)s)")
    parser.add_argument("-n", "--iterations", type=int, default=10,
                        help="Number of measured iterations per query (default: %(default)s)")
    parser.add_argument("-c", "--concurrency", type=int, default=1,
                        help="Concurrent requests per query (default: %(default)s)")
    parser.add_argument("--warmup", type=int, default=1,
                        help="Warmup iterations to discard (default: %(default)s)")
    parser.add_argument("--collection", default="community.lexicon.location.com.foursquare.places",
                        help="Collection to query (default: %(default)s)")
    parser.add_argument("--json", dest="json_output", action="store_true",
                        help="Output results as JSON")
    parser.add_argument("-f", "--filter", dest="filter",
                        help="Only run queries whose label starts with this prefix (e.g. spatial, text, combined)")
    args = parser.parse_args()

    if not args.json_output:
        print(f"Benchmarking {args.base_url} collection={args.collection}")
        print(f"  iterations={args.iterations} warmup={args.warmup} concurrency={args.concurrency}")
        print()

    results = run_benchmark(args)

    if args.json_output:
        json.dump(results, sys.stdout, indent=2)
        print()
    else:
        print_table(results)


if __name__ == "__main__":
    main()
