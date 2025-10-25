#!/usr/bin/env python3

import requests
import csv
from pathlib import Path

BASE_URL = "https://los.rightmove.co.uk/typeahead"


def fetch_postcode_id(postcode):
    """Fetch ID for a given postcode from Rightmove API.

    Returns the ID if a match is found, None if no matches exist.
    Raises RequestException if the API call fails.
    """
    params = {"query": postcode, "limit": 10, "exclude": "STREET"}

    response = requests.get(BASE_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    # Extract ID from the first match if available
    matches = data.get("matches", [])
    if matches and len(matches) > 0:
        return matches[0].get("id")
    return None


def main():
    postcodes_file = Path("postcodes.txt")
    csv_file = Path("postcode-mapping.csv")

    # Load existing mapping if it exists
    mapping = {}
    if csv_file.exists():
        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mapping[row["postcode"]] = row["id"]

    # Read postcodes
    with open(postcodes_file, "r") as f:
        postcodes = [line.strip() for line in f if line.strip()]

    # Fetch IDs for each postcode
    total = len(postcodes)
    for i, postcode in enumerate(postcodes, 1):
        if (i - 1) % 1000 == 0:
            print(f"\nProgress: {i}/{total} postcodes")

        if postcode not in mapping:
            postcode_id = fetch_postcode_id(postcode)
            if postcode_id:
                mapping[postcode] = postcode_id
            else:
                print(f"  No ID found for {postcode}")
        else:
            pass  # Already mapped

    # Write mapping to CSV
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["postcode", "id"])
        for postcode, postcode_id in sorted(mapping.items()):
            writer.writerow([postcode, postcode_id])

    print(f"\nMapping saved to {csv_file}")


if __name__ == "__main__":
    main()
