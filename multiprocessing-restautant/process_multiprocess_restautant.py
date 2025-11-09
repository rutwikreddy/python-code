import pandas as pd
import boto3
import s3fs
from multiprocessing import Pool, cpu_count

# ---------- CONFIG ----------
S3_BUCKET = "your-bucket-name"
S3_PREFIX = "data/restaurants/"  # S3 folder with parquet files
NUM_WORKERS = min(cpu_count(), 8) # Change this value as per min nodes needed

# ---------- LIST PARQUET FILES ----------
def list_s3_parquet_files(bucket, prefix):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    files = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(f"s3://{bucket}/{obj['Key']}")
    return files

# ---------- PROCESS ONE FILE ----------
def process_parquet_file(s3_path):
    """Reads one parquet file and returns top restaurant per city within that file"""
    try:
        df = pd.read_parquet(s3_path, filesystem=s3fs.S3FileSystem())

        # Ensure required columns exist
        required_cols = {"restaurant_name", "city", "rating"}
        if not required_cols.issubset(df.columns):
            missing = required_cols - set(df.columns)
            print(f"Skipping {s3_path} (missing columns: {missing})")
            return pd.DataFrame()

        # Get top restaurant (max rating) per city in that file
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
    print(f"Found {len(parquet_files)} parquet files in s3://{S3_BUCKET}/{S3_PREFIX}")

    # Parallel processing
    with Pool(processes=NUM_WORKERS) as pool:
        results = pool.map(process_parquet_file, parquet_files)

    # Combine and get top restaurant per city across all files
    all_data = pd.concat(results, ignore_index=True)
    print(f"Combined data size: {len(all_data)} rows")

    final_top_restaurants = (
        all_data.sort_values(["city", "rating"], ascending=[True, False])
                .groupby("city", as_index=False)
                .first()
    )

    print("\n🏆 Top Restaurants per City:")
    print(final_top_restaurants)

    final_top_restaurants.to_csv("top_restaurants_by_city.csv", index=False)

