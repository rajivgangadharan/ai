import time
import dask.dataframe as dd
import pandas as pd

# Timing Pandas loading
start_time = time.time()
pd_large_df = pd.read_csv('/run/media/rajivg/WORK/Code/Datasets/10_million_rows.csv')
print(f"Pandas loading time: {time.time() - start_time} seconds")

# Timing Dask loading
start_time = time.time()
dd_large_df = dd.read_csv('/run/media/rajivg/WORK/Code/Datasets/10_million_rows.csv')
print(f"Dask loading time: {time.time() - start_time} seconds")

