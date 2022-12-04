import logging
import time

from pycoingecko import CoinGeckoAPI

logger = logging.getLogger('PriceEnricher')


class MarketService:
    def __init__(self, platform_id='binance-smart-chain', currency='usd'):
        self.coingecko = CoinGeckoAPI()
        self.coingecko.request_timeout = 15
        self.platform_id = platform_id
        self.currency = currency

    def get_price(self, address):
        address = address.lower()
        try:
            info = self.coingecko.get_token_price(self.platform_id, address, self.currency)
            if not info:
                if address == '0x':
                    coin_id = 'binancecoin'
                    info = self.coingecko.get_price(coin_id, self.currency)
                    address = coin_id
                else:
                    return None
            price = info[address][self.currency]
        except Exception as ex:
            logger.exception(ex)
            return None
        return price

    def get_token_info(self, address):
        try:
            address = address.lower()
            info = self.coingecko.get_token_price(self.platform_id, address, self.currency, include_market_cap=True, include_24hr_vol=True)
            if not info:
                if address == '0x':
                    coin_id = 'binancecoin'  # get coin id correspond address
                    info = self.coingecko.get_price(coin_id, self.currency, include_market_cap=True, include_24hr_vol=True)
                else:
                    return {}
            return {
                'price': info[address].get(self.currency),
                'market_cap': info[address].get(self.currency + '_market_cap'),
                'volume_24h': info[address].get(self.currency + '_24h_vol')
            }
        except Exception as e:
            logger.exception(e)
        return {}

    def get_list_coin_id(self):
        tokens = self.coingecko.get_coins_list()
        coin_ids = [t['id'] for t in tokens]
        return coin_ids

    def get_list_coin_id_by_address(self, addresses):
        results = {}
        tokens = self.coingecko.get_coins_list(include_platform=True)
        for token in tokens:
            if self.platform_id in token['platforms'] and token['platforms'][self.platform_id] != '':
                address = token['platforms'][self.platform_id].strip()
                if address in addresses:
                    results[address] = token["id"]
        if '0x' in addresses:
            if self.platform_id == 'binance-smart-chain':
                results['0x'] = 'binancecoin'
        return results

    def get_market_info(self, ids, batch_size=200):
        length = len(ids)
        tokens = []
        for i in range(0, length, batch_size):
            last = min(i + batch_size, length)
            tokens += self.coingecko.get_coins_markets(vs_currency=self.currency, ids=ids[i:last])
            time.sleep(1)
        return tokens

    def get_list_address_by_id(self, coin_id):
        coin_detail = self.coingecko.get_coin_by_id(coin_id)
        return coin_detail['platforms']