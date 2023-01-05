import pandas as pd
import os
import sys

script_path = os.path.realpath(sys.modules['__main__'].__file__)
script_dir = os.path.dirname(script_path)
data_path = os.path.join(script_dir, 'data/coin_id_100.csv')
output_path = os.path.join(script_dir, 'data/coin_id_100_sorted.csv')

df = pd.read_csv(data_path)

df.sort_values(by=['market_cap_rank'], inplace=True)

df.to_csv(output_path, index=False)