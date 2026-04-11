"""Prep metadata JSON for Snowflake upload.

Strips similar_item (HTML blob) and image URLs to reduce file size.
Compresses output with gzip.

Usage:
    python scripts/prep_metadata.py meta_Cell_Phones.json
    python scripts/prep_metadata.py meta_Electronics.json --output clean.json.gz
"""

import argparse
import gzip
import json
import os
import time


DROP_FIELDS = {'similar_item', 'imageURL', 'imageURLHighRes'}


def main():
    parser = argparse.ArgumentParser(description="Prep metadata for Snowflake upload")
    parser.add_argument("input_file", help="Path to raw metadata JSON file")
    parser.add_argument("--output", help="Output path (default: input_clean.json.gz)")
    args = parser.parse_args()

    if not os.path.exists(args.input_file):
        print(f"ERROR: File not found: {args.input_file}")
        return

    output_path = args.output or args.input_file.replace('.json', '_clean.json.gz')

    print(f"Processing: {args.input_file}")
    print(f"Output: {output_path}")
    print(f"Dropping fields: {DROP_FIELDS}")

    start = time.time()
    count = 0
    errors = 0

    with open(args.input_file, 'r', encoding='utf-8') as fin, \
         gzip.open(output_path, 'wt', encoding='utf-8', compresslevel=6) as fout:
        for line in fin:
            try:
                item = json.loads(line.strip())
                for field in DROP_FIELDS:
                    item.pop(field, None)
                fout.write(json.dumps(item, ensure_ascii=False) + '\n')
                count += 1
            except json.JSONDecodeError:
                errors += 1

            if count % 100000 == 0 and count > 0:
                print(f"  Processed {count:,} lines in {time.time() - start:.0f}s...")

    elapsed = time.time() - start
    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"\nDone. {count:,} products, {errors} errors, {elapsed:.0f}s")
    print(f"Output: {output_path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
