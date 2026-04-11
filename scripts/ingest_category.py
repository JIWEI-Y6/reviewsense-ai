"""Ingest a new product category into ReviewSenseAI.

Simple script: upload file to Snowflake stage → COPY INTO raw table.
Snowflake handles parallelism, resumability, and idempotency natively.

Usage:
    python scripts/ingest_category.py --category Cell_Phones --reviews Cell_Phones.json.gz
    python scripts/ingest_category.py --category Cell_Phones --metadata meta_Cell_Phones_clean.json.gz
    python scripts/ingest_category.py --category Cell_Phones --reviews r.json.gz --metadata m.json.gz
"""

import argparse
import json
import os
import sys
import time

import snowflake.connector
from dotenv import load_dotenv


def get_connection():
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
    )


def validate_jsonl(file_path: str, sample_lines: int = 10) -> bool:
    """Check first N lines are valid JSON."""
    import gzip
    opener = gzip.open if file_path.endswith('.gz') else open
    valid = 0
    invalid = 0
    with opener(file_path, 'rt', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= sample_lines:
                break
            try:
                json.loads(line.strip())
                valid += 1
            except json.JSONDecodeError:
                invalid += 1

    print(f"  Validated {valid + invalid} sample lines: {valid} valid, {invalid} invalid")
    return invalid == 0


def upload_and_load(conn, file_path: str, category: str, target: str):
    """PUT file to stage, COPY INTO target table."""
    cur = conn.cursor()
    stage_path = f"@RAW.INGESTION_STAGE/{category}"

    # Upload
    print(f"  Uploading {file_path} to {stage_path}...")
    start = time.time()
    cur.execute(
        f"PUT file://{os.path.abspath(file_path)} {stage_path}/ "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    upload_time = time.time() - start
    for row in cur:
        print(f"    {row[0]} -> {row[1]} ({row[6]})")
    print(f"  Upload complete in {upload_time:.1f}s")

    # Get the staged filename
    filename = os.path.basename(file_path)

    # COPY INTO
    print(f"  Loading into {target}...")
    start = time.time()

    if target == "RAW.REVIEWS_RAW_V2":
        cur.execute(f"""
            COPY INTO RAW.REVIEWS_RAW_V2 (V, SOURCE_CATEGORY, SOURCE_FILE, LOADED_AT)
            FROM (
                SELECT
                    $1::VARIANT,
                    '{category}',
                    METADATA$FILENAME,
                    CURRENT_TIMESTAMP()
                FROM {stage_path}/
            )
            FILE_FORMAT = RAW.INGESTION_JSON_FF
            PATTERN = '.*{filename}.*'
            ON_ERROR = CONTINUE
        """)
    elif target == "RAW.METADATA_RAW_V2":
        cur.execute(f"""
            COPY INTO RAW.METADATA_RAW_V2 (V, SOURCE_CATEGORY, SOURCE_FILE, LOADED_AT)
            FROM (
                SELECT
                    $1::VARIANT,
                    '{category}',
                    METADATA$FILENAME,
                    CURRENT_TIMESTAMP()
                FROM {stage_path}/
            )
            FILE_FORMAT = RAW.INGESTION_JSON_FF
            PATTERN = '.*{filename}.*'
            ON_ERROR = CONTINUE
        """)

    load_time = time.time() - start
    for row in cur:
        print(f"    Loaded: {row[3]} rows, {row[5]} errors ({row[1]})")

    # Verify
    cur.execute(f"""
        SELECT COUNT(*) FROM {target}
        WHERE SOURCE_CATEGORY = '{category}'
    """)
    count = cur.fetchone()[0]
    print(f"  Total rows for '{category}' in {target}: {count}")
    print(f"  Load complete in {load_time:.1f}s")

    return count


def main():
    parser = argparse.ArgumentParser(description="Ingest a product category into ReviewSenseAI")
    parser.add_argument("--category", required=True, help="Category name (e.g., Cell_Phones)")
    parser.add_argument("--reviews", help="Path to reviews JSONL file (or .json.gz)")
    parser.add_argument("--metadata", help="Path to metadata JSONL file (or .json.gz)")
    args = parser.parse_args()

    if not args.reviews and not args.metadata:
        print("Error: provide at least --reviews or --metadata (or both)")
        sys.exit(1)

    print(f"=== Ingesting category: {args.category} ===")

    # Validate files
    if args.reviews:
        print(f"\nValidating reviews file: {args.reviews}")
        if not os.path.exists(args.reviews):
            print(f"  ERROR: File not found: {args.reviews}")
            sys.exit(1)
        if not validate_jsonl(args.reviews):
            print("  WARNING: Some sample lines are invalid JSON. Proceeding with ON_ERROR=CONTINUE.")

    if args.metadata:
        print(f"\nValidating metadata file: {args.metadata}")
        if not os.path.exists(args.metadata):
            print(f"  ERROR: File not found: {args.metadata}")
            sys.exit(1)
        if not validate_jsonl(args.metadata):
            print("  WARNING: Some sample lines are invalid JSON. Proceeding with ON_ERROR=CONTINUE.")

    # Connect
    print("\nConnecting to Snowflake...")
    conn = get_connection()
    print("  Connected.")

    # Load reviews
    if args.reviews:
        print(f"\n--- Loading reviews ---")
        review_count = upload_and_load(conn, args.reviews, args.category, "RAW.REVIEWS_RAW_V2")

    # Load metadata
    if args.metadata:
        print(f"\n--- Loading metadata ---")
        meta_count = upload_and_load(conn, args.metadata, args.category, "RAW.METADATA_RAW_V2")

    conn.close()

    print(f"\n=== Ingestion complete for '{args.category}' ===")
    if args.reviews:
        print(f"  Reviews: {review_count} rows")
    if args.metadata:
        print(f"  Metadata: {meta_count} rows")
    print(f"\nNext steps:")
    print(f"  1. python run_dbt.py run     # Process through pipeline")
    print(f"  2. python run_dbt.py test    # Validate")


if __name__ == "__main__":
    main()
