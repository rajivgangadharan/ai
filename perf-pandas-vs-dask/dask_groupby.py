import dask.dataframe as dd
import time

# Load the dataset with Pandas
start_time = time.time()

ddf = dd.read_csv('/run/media/rajivg/WORK/Code/Datasets/10_million_rows_for_groupby.csv')
result = ddf.groupby('value1').sum().compute()  # Perform groupby operation

end_time = time.time()

print("Dask GroupBy Processing Time: {:.2f} seconds".format(end_time - start_time))
