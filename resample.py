import pandas as pd
import numpy as np

usage_data = pd.read_csv('usage.csv')
print(usage_data.shape)

usage_data = usage_data.sample(frac=0.001, replace=True, random_state=1)
print("\nresized:", flush=True)
print(usage_data.shape, flush=True)

usage_data.to_csv('usage_downsized.csv');
