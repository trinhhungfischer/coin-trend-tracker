from market_service import MarketService
import pymongo
import pandas as pd

client = pymongo.MongoClient('mongodb+srv://admin:admin@db.5m3qg.mongodb.net/test?authSource=admin&replicaSet=atlas-dzj3vd-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true', 
                             username='admin', password='admin')


service = MarketService()
list_coin_id = service.get_list_coin_id()

db = client["token"]
col = db["coincap"]

error = []

for i in range (0, len(list_coin_id), 200):
  try:
    list_market_info = service.get_market_info(list_coin_id[i:i+200])
    for coin in list_market_info:
      coin['_id'] = 'token/' + coin['id']
    
    col.insert_many(list_market_info)
  except:
    print(i)
    error.append(i)
    
list_market_info = service.get_market_info(list_coin_id[5200:5400])
for coin in list_market_info:
  coin['_id'] = 'token/' + coin['id']