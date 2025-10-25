#!/usr/bin/env python3
import csv

csv_file = "ONSPD_AUG_2025/Data/ONSPD_AUG_2025_UK.csv"
output_file = "postcodes.txt"

postcodes = set()

with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        postcode = row.get('pcds', '').strip()
        doterm = row.get('doterm', '').strip()
        if postcode and not doterm:
            postcodes.add(postcode)

with open(output_file, 'w', encoding='utf-8') as f:
    for postcode in sorted(postcodes):
        f.write(postcode + '\n')

print(f"Extracted {len(postcodes)} unique postcodes to {output_file}")
