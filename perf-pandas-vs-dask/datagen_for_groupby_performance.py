import os
import pandas as pd
import numpy as np
import dotenv
from dotenv import load_dotenv


# Function to generate a 10 million row dataset
def generate_large_dataset(num_rows=10_000_000):
    n_unique_values = 1000
    n_rows = num_rows
    load_dotenv()
    # Set a random seed for reproducibility
    np.random.seed(0)
    
    # Generate random data
    data = {
        'value1': np.random.randint(0, n_unique_values, n_rows),
        'value2': np.random.rand(num_rows),
        'value3': np.random.rand(num_rows)
    }
    
    # Create a DataFrame
    df = pd.DataFrame(data)
    
    # Save the DataFrame to a CSV file
    file_name = os.getenv('DATASETS_PATH') +'/10_million_rows_for_groupby.csv' 
    print(f"file_name is {file_name}")
    df.to_csv(file_name, index=False)
    print(f"10 million rows dataset generated and saved as '10_million_rows_for_groupby.csv'.")

# Generate the dataset
generate_large_dataset()

