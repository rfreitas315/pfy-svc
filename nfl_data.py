import nfl_data_py as nfl
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import duckdb
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nfl_data_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('nfl_data')

# Set display options for better dataframe viewing
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)

season_years = [2023, 2024]
for season_year in season_years:
    logger.info(f"Processing data for {season_year} season")
    pbp_data = nfl.import_pbp_data([season_year])
    logger.info(f"Successfully loaded {len(pbp_data)} plays from the {season_year} NFL season")
    
    print(pd.Series(pbp_data.columns).sort_values().values)

# qb_stats = pbp_data[pbp_data['pass'] == 1].groupby('passer_player_name').agg(
#     attempts=('play_id', 'count'),
#     completions=('complete_pass', 'sum'),
#     yards=('passing_yards', 'sum'),
#     touchdowns=('pass_touchdown', 'sum'),
#     interceptions=('interception', 'sum'),
#     epa=('epa', 'mean')
# ).reset_index()

logger.info("Connecting to DuckDB")
conn = duckdb.connect('nfl_data.ddb')

parquet_dir = 'nfl_pbp_parquet'
if not os.path.exists(parquet_dir):
    logger.info(f"Creating directory: {parquet_dir}")
    os.makedirs(parquet_dir)

logger.info("Creating DuckDB table from DataFrame")
conn.execute("CREATE OR REPLACE TABLE pbp_data AS SELECT * FROM pbp_data")

logger.info(f"Writing data to Parquet file: {parquet_dir}/pbp_data.parquet")
conn.execute(f"COPY pbp_data TO '{parquet_dir}/pbp_data.parquet' (FORMAT PARQUET)")

logger.info("Verifying Parquet table")
conn.execute(f"CREATE OR REPLACE TABLE pbp_parquet AS SELECT * FROM '{parquet_dir}/pbp_data.parquet'")
count_result = conn.execute("SELECT COUNT(*) FROM pbp_parquet").fetchall()
logger.info(f"Parquet table contains {count_result[0][0]} records")
print(count_result)

logger.info("Closing DuckDB connection")
conn.close()

logger.info("Processing complete")