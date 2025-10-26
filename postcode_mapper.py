#!/usr/bin/env python3

import requests
import csv
import time
import sys
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://los.rightmove.co.uk/typeahead"

# Retry configuration
INITIAL_BACKOFF = 1  # seconds
MAX_BACKOFF = 60  # seconds
BACKOFF_MULTIPLIER = 2


def create_session():
    """Create optimized HTTP session with connection pooling and retry strategy."""
    session = requests.Session()

    # Configure retry strategy for transient errors
    retry_strategy = Retry(
        total=3,
        backoff_factor=INITIAL_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    # Use HTTPAdapter with optimized pool settings
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def fetch_postcode_id(postcode, session):
    """Fetch ID for a given postcode from Rightmove API.

    Returns the ID if a match is found, None if no matches exist.
    Retries handled automatically by session retry strategy.
    """
    params = {"query": postcode, "limit": 10, "exclude": "STREET"}

    try:
        response = session.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        matches = data.get("matches", [])
        if matches:
            return matches[0].get("id")
        return None

    except requests.exceptions.RequestException as e:
        print(f"  Failed to fetch {postcode}: {e}")
        return None


def flush_buffer_to_csv(buffer, csv_file):
    """Write buffered postcodes to CSV file."""
    if not buffer:
        return
    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f)
        for postcode, postcode_id in buffer:
            writer.writerow([postcode, postcode_id])


def write_sorted_csv(mapping, csv_file):
    """Write mapping dictionary to CSV file, sorted by postcode."""
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["postcode", "id"])
        for postcode, postcode_id in sorted(mapping.items()):
            writer.writerow([postcode, postcode_id])


def format_time(seconds):
    """Format seconds into human readable format."""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{int(hours)}h {int(minutes)}m"


def main():
    # Unbuffered output
    sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1)

    postcodes_file = Path("postcodes.txt")
    csv_file = Path("postcode-mapping.csv")

    # Create optimized HTTP session with connection pooling and retry strategy
    session = create_session()

    # Load existing mapping if it exists
    mapping = {}
    csv_exists = csv_file.exists()
    if csv_exists:
        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mapping[row["postcode"]] = row["id"]
        print(f"Loaded {len(mapping)} existing mappings from {csv_file}")
    else:
        # Write header for new CSV
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["postcode", "id"])

    # Read postcodes
    with open(postcodes_file, "r") as f:
        postcodes = [line.strip() for line in f if line.strip()]

    # Fetch IDs for each postcode
    total = len(postcodes)
    fetched = 0
    write_buffer = []
    start_time = time.time()
    for i, postcode in enumerate(postcodes, 1):
        if postcode not in mapping:
            fetched += 1
            postcode_id = fetch_postcode_id(postcode, session)
            if postcode_id:
                mapping[postcode] = postcode_id
                write_buffer.append((postcode, postcode_id))
            else:
                print(f"  No ID found for {postcode}")

            # Flush buffer and log progress every 100 new results
            if fetched % 100 == 0:
                flush_buffer_to_csv(write_buffer, csv_file)
                write_buffer = []

                elapsed = time.time() - start_time
                rate = fetched / elapsed if elapsed > 0 else 0
                remaining_to_fetch = total - i
                remaining_time = remaining_to_fetch / rate if rate > 0 else 0

                print(f"Progress: {i}/{total} postcodes | Fetched: {fetched} | Saved: {len(mapping)} | Elapsed: {format_time(elapsed)} | ETA: {format_time(remaining_time)}")

    # Flush any remaining buffered writes
    flush_buffer_to_csv(write_buffer, csv_file)

    # Final write to CSV (sorted)
    write_sorted_csv(mapping, csv_file)
    session.close()
    total_time = time.time() - start_time
    print(f"\nFinal mapping saved to {csv_file} ({len(mapping)} total postcodes) in {format_time(total_time)}")


if __name__ == "__main__":
    main()
