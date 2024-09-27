import pandas as pd
import time

# Load the dataset with Pandas
start_time = time.time()

df = pd.read_csv('/run/media/rajivg/WORK/Code/Datasets/10_million_rows_for_groupby.csv')
result = df.groupby('value1').sum()  # Perform groupby operation

end_time = time.time()

print("Pandas GroupBy Processing Time: {:.2f} seconds".format(end_time - start_time))
