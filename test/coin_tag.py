import pandas as pd
import os
import sys

def coin_tag():
    script_path = os.path.realpath(sys.modules['__main__'].__file__)
    script_dir = os.path.dirname(script_path)
    data_path = os.path.join(script_dir, 'coin_id_100_sorted.csv')
        
    df = pd.read_csv(data_path)

    df['ticker'] = df['ticker'].str.upper()

    lower_tickers = df['ticker'].str.lower().to_numpy().tolist()
    
    coin_tickers = df['ticker'].to_numpy().tolist()
    
    coin_tickers += lower_tickers
    
    results = ""
    
    list_results = []
        
    for i, ticker in enumerate(coin_tickers):
        if (len(results) + len(ticker) + 5) >= 512:
            list_results.append(results[:-4])
            results = ""
        
        if i != len(coin_tickers) - 1:
            results += "#" + ticker + " OR "
        else:
            results += "#" + ticker
    
    list_results.append(results)
    
    return list_results