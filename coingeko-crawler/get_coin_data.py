from market_service import MarketService
import os
import sys
import time

from twython import TwythonStreamer

BATCH_SIZE = 100

def main():
    service = MarketService()
    list_coin_id = service.get_list_coin_id()

    script_path = os.path.realpath(sys.modules['__main__'].__file__)
    script_dir = os.path.dirname(script_path)
    data_path = os.path.join(script_dir, 'data/coin_id_100.csv')
    
    # with open(data_path, 'w') as f:
    #     f.write('coin_id,ticker,name,market_cap_rank\n')
    

    for i in range(0, len(list_coin_id), BATCH_SIZE):
        print('Processing batch {} ...'.format(i))

        batch_coin_ids = None

        if i + BATCH_SIZE > len(list_coin_id):
            batch_coin_ids = list_coin_id[i:]
        else:
            batch_coin_ids = list_coin_id[i: i+BATCH_SIZE]
        
        coin_data = service.get_market_info(batch_coin_ids)
        
        append_str = ""
        
        for coin_info in coin_data:
            if not (coin_info['market_cap_rank'] is None):
                if coin_info['market_cap_rank'] <= 100:            
                    append_str += coin_info['id'] + "," + coin_info['symbol'] + "," + coin_info['name'] + "," + str(coin_info['market_cap_rank']) + "\n"
        
        # with open(data_path, 'a') as f:
        #     f.write(append_str)                
    
    
if __name__ == '__main__':
    main()