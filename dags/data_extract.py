import requests
import duckdb 
import os

def download_file():
    for year in range(2020, 2026):
        for month in range(1, 13):
            url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
            output_path = f'data/raw/yellow_tripdata_{year}-{month:02d}.parquet'
            if os.path.exists(output_path):
                continue
            response = requests.get(url, stream=True)
            response.raise_for_status() 
            os.makedirs(os.path.dirname(output_path), exist_ok=True)


            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

def load_file():
    con = duckdb.connect(database='data/raw/database.duckdb')
    con.execute(f"CREATE OR REPLACE TABLE yellow_taxi AS SELECT * FROM read_parquet('data/raw/yellow_tripdata_*.parquet')")
