from market_service import MarketService

def main():
    service = MarketService()
    list_coin_id = service.get_list_coin_id()
    print(list_coin_id)

if __name__ == '__main__':
    main()