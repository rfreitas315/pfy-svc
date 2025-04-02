import nfl_data_py as nfl
import pandas as pd
import seaborn as sns
import duckdb
import os

# Set display options for better dataframe viewing
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)

season_years = [2023, 2024]
for season_year in season_years:
    pbp_data = nfl.import_pbp_data([season_year])

print(pd.Series(pbp_data.columns).sort_values().values)


# qb_stats = pbp_data[pbp_data['pass'] == 1].groupby('passer_player_name').agg(
#     attempts=('play_id', 'count'),
#     completions=('complete_pass', 'sum'),
#     yards=('passing_yards', 'sum'),
#     touchdowns=('pass_touchdown', 'sum'),
#     interceptions=('interception', 'sum'),
#     epa=('epa', 'mean')
# ).reset_index()

conn = duckdb.connect('nfl_data.ddb')

parquet_dir = 'nfl_pbp_parquet'
if not os.path.exists(parquet_dir):
    os.makedirs(parquet_dir)

conn.execute("CREATE OR REPLACE TABLE pbp_data AS SELECT * FROM pbp_data")

conn.execute(f"COPY pbp_data TO '{parquet_dir}/pbp_data.parquet' (FORMAT PARQUET)")

conn.execute(f"CREATE OR REPLACE TABLE pbp_parquet AS SELECT * FROM '{parquet_dir}/pbp_data.parquet'")
print(conn.execute("SELECT COUNT(*) FROM pbp_parquet").fetchall())

# Close the connection
conn.close()