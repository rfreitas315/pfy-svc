import nfl_data_py as nfl
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import duckdb
import os

# Set display options for better dataframe viewing
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)

# Load play-by-play data for a specific season
# Change the year as needed (data available from 1999 onwards)
season_year = 2023
pbp_data = nfl.import_pbp_data([season_year])

print(f"Successfully loaded {len(pbp_data)} plays from the {season_year} NFL season")

# Display the first few rows to see what's available
print("\nSample of available play-by-play data:")
print(pbp_data.head())

# See the columns available in the dataset
print("\nColumns available in the dataset:")
print(pd.Series(pbp_data.columns).sort_values().values)

# Example 1: Basic filtering to find all touchdown passes
td_passes = pbp_data[(pbp_data['pass'] == 1) & (pbp_data['touchdown'] == 1)]
print(f"\nFound {len(td_passes)} touchdown passes in {season_year}")

# Example 2: Calculate average EPA (Expected Points Added) by team for passing plays
team_pass_epa = pbp_data[pbp_data['pass'] == 1].groupby('posteam')['epa'].mean().sort_values(ascending=False)
print("\nTop 5 teams by average EPA on passing plays:")
print(team_pass_epa.head())

# Example 3: Simple visualization of EPA by down for passing plays
plt.figure(figsize=(10, 6))
sns.boxplot(x='down', y='epa', data=pbp_data[pbp_data['pass'] == 1])
plt.title(f'EPA Distribution by Down for Passing Plays ({season_year})')
plt.savefig('pass_epa_by_down.png')
print("\nCreated visualization: pass_epa_by_down.png")

# Example 4: Get quarterback stats
print("\nCalculating QB stats...")
qb_stats = pbp_data[pbp_data['pass'] == 1].groupby('passer_player_name').agg(
    attempts=('play_id', 'count'),
    completions=('complete_pass', 'sum'),
    yards=('passing_yards', 'sum'),
    touchdowns=('pass_touchdown', 'sum'),
    interceptions=('interception', 'sum'),
    epa=('epa', 'mean')
).reset_index()

# Calculate completion percentage
qb_stats['completion_pct'] = (qb_stats['completions'] / qb_stats['attempts'] * 100).round(1)

# Filter for QBs with at least 100 attempts
qb_stats = qb_stats[qb_stats['attempts'] >= 100].sort_values('epa', ascending=False)
print("\nTop 5 QBs by EPA per play (min. 100 attempts):")
print(qb_stats[['passer_player_name', 'attempts', 'completion_pct', 'yards', 'touchdowns', 'interceptions', 'epa']].head())

# Example 5: Get roster data for player information
# print("\nLoading roster data...")
# roster = nfl.__import_rosters([season_year])
# print(f"Loaded roster information for {len(roster)} players")
# print(roster[['player_name', 'position', 'team', 'height', 'weight']].head())

# Create a DuckDB connection
conn = duckdb.connect('nfl_data.ddb')

# Create a directory to store the data if it doesn't exist
parquet_dir = 'nfl_pbp_parquet'
if not os.path.exists(parquet_dir):
    os.makedirs(parquet_dir)

# Convert the DataFrame to a DuckDB table
conn.execute("CREATE OR REPLACE TABLE pbp_data AS SELECT * FROM pbp_data")

# Write to Parquet format instead of Delta
conn.execute(f"COPY pbp_data TO '{parquet_dir}/pbp_data.parquet' (FORMAT PARQUET)")

print(f"\nCreated DuckDB parquet table at '{parquet_dir}/pbp_data.parquet'")

# Verify the parquet table was created
conn.execute(f"CREATE OR REPLACE TABLE pbp_parquet AS SELECT * FROM '{parquet_dir}/pbp_data.parquet'")
print("\nVerifying parquet table contents:")
print(conn.execute("SELECT COUNT(*) FROM pbp_parquet").fetchall())

# Close the connection
conn.close()