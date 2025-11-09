import re
import pandas as pd
import boto3
import s3fs
from multiprocessing import Pool, cpu_count

# ---------- CONFIG ----------
S3_BUCKET = "your-bucket-name"
S3_PREFIX = "data/restaurants/"  # Folder containing date partitions
NUM_WORKERS = min(cpu_count(), 8) # Change this value as per min nodes needed

# ---------- LIST PARQUET FILES ----------
def list_s3_parquet_files(bucket, prefix):
    """Recursively lists all parquet files under a given S3 prefix"""
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    files = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                files.append(f"s3://{bucket}/{key}")
    return files

# ---------- EXTRACT DATE FROM KEY ----------
def extract_date_from_key(s3_path):
    """Extracts date=YYYY-MM-DD from the S3 key"""
    match = re.search(r"date=(\d{4}-\d{2}-\d{2})", s3_path)
    return match.group(1) if match else None

# ---------- PROCESS ONE FILE ----------
def process_parquet_file(s3_path):
    """Reads one parquet file, adds partition date, returns top restaurant per city"""
    try:
        fs = s3fs.S3FileSystem()
        df = pd.read_parquet(s3_path, filesystem=fs)

        # Add partition date column if found
        df["date"] = extract_date_from_key(s3_path)

        # Ensure required columns exist
        required_cols = {"restaurant_name", "city", "rating"}
        if not required_cols.issubset(df.columns):
            missing = required_cols - set(df.columns)
            print(f"Skipping {s3_path} (missing columns: {missing})")
            return pd.DataFrame()

        # Get top restaurant per city within that file
        top_df = (
            df.sort_values(["city", "rating"], ascending=[True, False])
              .groupby("city", as_index=False)
              .first()
        )

        top_df["source_file"] = s3_path
        return top_df

    except Exception as e:
        print(f"Error processing {s3_path}: {e}")
        return pd.DataFrame()

# ---------- MAIN PARALLEL EXECUTION ----------
if __name__ == "__main__":
    parquet_files = list_s3_parquet_files(S3_BUCKET, S3_PREFIX)
    print(f"Found {len(parquet_files)} parquet files under s3://{S3_BUCKET}/{S3_PREFIX}")

    with Pool(processes=NUM_WORKERS) as pool:
        results = pool.map(process_parquet_file, parquet_files)

    # Combine all partial results
    all_data = pd.concat(results, ignore_index=True)
    print(f"Combined data size: {len(all_data)} rows")

    # Get top restaurant per city (across all dates)
    final_top_restaurants = (
        all_data.sort_values(["city", "rating", "date"], ascending=[True, False, False])
                .groupby("city", as_index=False)
                .first()
    )

    print("\n🏆 Top Restaurants per City:")
    print(final_top_restaurants[["city", "restaurant_name", "rating", "date"]])

    final_top_restaurants.to_csv("top_restaurants_by_city.csv", index=False)
