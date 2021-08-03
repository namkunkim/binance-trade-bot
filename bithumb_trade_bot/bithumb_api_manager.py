import math
import time
import traceback
from typing import Dict, Optional
import pybithumb
from cachetools import TTLCache, cached

from .bithumb_stream_manager import BithumbCache, BithumbOrder, BithumbStreamManager, OrderGuard
from .config import Config
from .database import Database
from .logger import Logger
from .models import Coin


class BithumbAPIManager:
    def __init__(self, config: Config, db: Database, logger: Logger):
        # initializing the client class calls `ping` API endpoint, verifying the connection

        """
        self.binance_client = Client(
            config.BINANCE_API_KEY,
            config.BINANCE_API_SECRET_KEY,
            tld=config.BINANCE_TLD,
        )
        """

        """
        빗썸에서는 connection api, security api는 private api를 사용할때 필요하다.
        코인들의 현재 시세등의 오픈된 정보의 획득은 public api - pybithumb을 사용하는데.   
        """
        self.client = pybithumb.Bithumb(config.BINANCE_API_KEY, config.BINANCE_API_SECRET_KEY)

        self.db = db
        self.logger = logger
        self.config = config

        self.cache = BithumbCache()
        self.stream_manager: Optional[BithumbStreamManager] = None
        self.setup_websockets()

    def setup_websockets(self):
        self.stream_manager = BithumbStreamManager(
            self.cache,
            self.config,
            self.client,
            self.logger,
        )

    @cached(cache=TTLCache(maxsize=1, ttl=43200))
    def get_trade_fees(self) -> Dict[str, float]:
        """return {ticker["symbol"]: ticker["taker"] for ticker in self.binance_client.get_trade_fee()["tradeFee"]}"""
        d = dict()
        for currency in self.db.get_coins():
            for target in ["KRW", "BTC"]:
                self.logger.info("get_trade_fees currency : " + currency.symbol)
                fee = self.client.get_trading_fee(currency.symbol, payment_currency=target)
                try:
                    fee_f = float(str(fee))
                    self.logger.info("get_trade_fees fee : " + str(fee))
                    d[currency.symbol + target] = fee_f
                    self.logger.info("get_trade_fees d : " + str(d))
                except:
                    self.logger.info(currency.symbol + target + " " + str(fee))
        return d

    def get_fee(self, origin_coin: Coin, target_coin: Coin, selling: bool):
        base_fee = self.get_trade_fees()[origin_coin + target_coin]
        self.logger.info("get_fee base_fee : " + str(base_fee))
        return base_fee

    def get_account(self):
        """
        Get account information
            {
                "makerCommission": 15,
                "takerCommission": 15,
                "buyerCommission": 0,
                "sellerCommission": 0,
                "canTrade": true,
                "canWithdraw": true,
                "canDeposit": true,
                "balances": [
                    {
                        "asset": "BTC",
                        "free": "4723846.89208129",
                        "locked": "0.00000000"
                    },
                    {
                        "asset": "LTC",
                        "free": "4763368.68006011", ##단위가 뭐지?
                        "locked": "0.00000000"
                    }
                ]
            }
            
            가상화폐 이름의 키 값 안에는 
            보유 중인 코인('free'), 
            거래 진행 중인 코인('used'), 
            전체 코인('total')
            https://wikidocs.net/31065            
            return self.binance_client.get_account()

        //get_account : balance 정보가 없어 get_balance()
        {
            "status" : "0000",
            "data" : {
                "opening_price" : "504000",
                "closing_price" : "505000",
                "min_price" : "504000",
                "max_price" : "516000",
                "units_traded" : "14.71960286",
                "acc_trade_value" : "16878100",
                "prev_closing_price" : "503000",
                "units_traded_24H" : "1471960286",
                "acc_trade_value_24H" : "16878100",
                "fluctate_24H" : "1000",
                "fluctate_rate_24H" : 0.19,
                "date" : "1417141032622"
            }
        }
        """
        account = dict()
        lst = []

        d = dict()
        b = self.client.get_balance("BTC")
        try:
            free = b[2] - b[3]
            if free > 0.0:
                d["free"] = float(int(free))
                d["asset"] = "KRW"
                lst.append(d)
        except Exception as e:
            self.logger.info("KRW" + " " + str(b) + " e : " + str(e))

        for currency in self.db.get_coins():
            d = dict()
            b = self.client.get_balance(currency.symbol)
            ##self.logger.info("get_account currency : " + currency.symbol + " balance : " + str(b))
            try:
                free = b[0] - b[1]
                ##self.logger.info("get_account free : " + str(free))
                if free > 0.0:
                    d["free"] = free
                    d["asset"] = currency.symbol##+"KRW"
                    """보유코인, 사용중코인, 보유원화, 사용중원화"""
                    lst.append(d)
            except Exception as e:
                self.logger.info(currency.symbol + " " + str(b) + " e : " + str(e))

        account["balances"] = lst
        return account

    def get_symbol_ticker(self):
        lst = []
        for currency in self.db.get_coins():
            for target in ["KRW", "BTC"]:
                d = dict()
                ##self.logger.info("get_symbol_ticker currency : " + currency.symbol)
                price = pybithumb.get_current_price(currency.symbol, payment_currency=target)
                try:
                    price_f = float(str(price))
                    ##self.logger.info("get_symbol_ticker price : " + str(price))
                    d["symbol"] = currency.symbol + target
                    d["price"] = str(price)
                    ##self.logger.info("get_symbol_ticker d : " + str(d))
                    lst.append(d)
                except:
                    self.logger.info(currency.symbol + target + " " + str(price))
        return lst

    def get_ticker_price(self, ticker_symbol: str):
        """
        Get ticker price of a specific coin
        """
        self.logger.info("get_ticker_price : " + ticker_symbol)
        price = self.cache.ticker_values.get(ticker_symbol, None)
        if price is None and ticker_symbol not in self.cache.non_existent_tickers:
            self.cache.ticker_values = {
                ticker["symbol"]: float(ticker["price"]) for ticker in self.get_symbol_ticker()
            }
            self.logger.info("get_ticker_price .cache.ticker_values : " +str(self.cache.ticker_values))
            """self.binance_client.get_symbol_ticker()"""
            self.logger.info(f"Fetched all ticker prices: {self.cache.ticker_values}")
            price = self.cache.ticker_values.get(ticker_symbol, None)
            if price is None:
                self.logger.info(f"Ticker does not exist: {ticker_symbol} - will not be fetched from now on")
                self.cache.non_existent_tickers.add(ticker_symbol)

        return price

    def get_currency_balance(self, currency_symbol: str, force=False) -> float:
        """
        Get balance of a specific coin
        """
        with self.cache.open_balances() as cache_balances:
            balance = cache_balances.get(currency_symbol, None)
            self.logger.info("get_currency_balance in : " + str(currency_symbol) + " " +str(balance))
            if force or balance is None:
                cache_balances.clear()
                cache_balances.update(
                    {
                        currency_balance["asset"]: float(currency_balance["free"])
                        for currency_balance in self.get_account()["balances"]
                    }
                )
                """self.binance_client.get_account()["balances"]"""
                self.logger.info(f"Fetched all balances: {cache_balances}")
                if currency_symbol not in cache_balances:
                    cache_balances[currency_symbol] = 0.0
                    return 0.0
                return cache_balances.get(currency_symbol, 0.0)

            return balance

    def retry(self, func, *args, **kwargs):
        time.sleep(1)
        attempts = 0
        while attempts < 20:
            try:
                return func(*args, **kwargs)
            except Exception:  # pylint: disable=broad-except
                self.logger.warning(f"Failed to Buy/Sell. Trying Again (attempt {attempts}/20)")
                if attempts == 0:
                    self.logger.warning(traceback.format_exc())
                attempts += 1
        return None

    # def get_symbol_filter(self, origin_symbol: str, target_symbol: str, filter_type: str):
    #     return next(
    #         _filter
    #         for _filter in self.binance_client.get_symbol_info(origin_symbol + target_symbol)["filters"]
    #         # """
    #         #     "filters": [
    #         #         {
    #         #             "filterType": "PRICE_FILTER",
    #         #             "minPrice": "0.00000100",
    #         #             "maxPrice": "100000.00000000",
    #         #             "tickSize": "0.00000100"
    #         #         }, {
    #         #             "filterType": "LOT_SIZE",
    #         #             "minQty": "0.00100000",
    #         #             "maxQty": "100000.00000000",
    #         #             "stepSize": "0.00100000"
    #         #         }, {
    #         #             "filterType": "MIN_NOTIONAL",
    #         #             "minNotional": "0.00100000"
    #         #         }
    #         #     ]
    #         # """
    #         if _filter["filterType"] == filter_type
    #     )

    def get_step_size(self, origin_symbol: str, target_symbol: str):
        """
        LOT_SIZE
        :stepSize는 구입할 코인의 최소 단위로, 구매할 코인의 가격에 대해 호가 가격 단위를 결정해야 한다.
        https://www.bithumb.com/customer_support/info_guide?seq=536&categorySeq=203
        """
        orderbook = pybithumb.get_orderbook(origin_symbol, target_symbol)
        if orderbook is None:
            return None
        asks = orderbook['asks']
        price = asks[0]['price']
        return str(self.get_sale_unit(price))

    # noinspection PyMethodMayBeStatic
    def get_sale_unit(self, price: int):
        if price < 100:
            return 10
        elif price < 1000:
            return 1
        elif price < 10000:
            return 0.1
        elif price < 100000:
            return 0.01
        else:
            return 0.001

    @cached(cache=TTLCache(maxsize=2000, ttl=43200))
    def get_alt_tick(self, origin_symbol: str, target_symbol: str):
        """step_size = self.get_symbol_filter(origin_symbol, target_symbol, "LOT_SIZE")["stepSize"]"""
        self.logger.info("get_alt_tick " + origin_symbol + " " + target_symbol)
        step_size = self.get_step_size(origin_symbol, target_symbol)
        if step_size is None:
            return 0
        self.logger.info("get_alt_tick " + str(step_size))
        if step_size.find("1") == 0:
            return 1 - step_size.find(".")
        return step_size.find("1") - 1

    @cached(cache=TTLCache(maxsize=2000, ttl=43200))
    def get_min_notional(self, origin_symbol: str, target_symbol: str):
        #https://github.com/jaggedsoft/php-binance-api/issues/205
        # """minQty is the minimum amount you can order (quantity)
        # minNotional is the minimum value of your order. (price * quantity)
        #
        # // Check minimum order size
        # if ( price * quantity < minNotional ) {
        #     quantity = minNotional / price;
        # }"""

        # return float(self.get_symbol_filter(origin_symbol, target_symbol, "MIN_NOTIONAL")["minNotional"])
        # 빗썸은 최소 비용이 1000원 이상이어야 한다.
        return 1000.0
    # def get_min_notional(self, origin_symbol: str, target_symbol: str):
    #     """
    #     MIN_NOTIONAL
    #     :minNotional 최소 구입 갯수 * 비용
    #     https://www.bithumb.com/customer_support/info_guide?seq=536&categorySeq=203
    #     """
    #     orderbook = pybithumb.get_orderbook(origin_symbol, target_symbol)
    #     asks = orderbook['asks']
    #     price = asks[0]['price']
    #     return self.get_sale_unit(price) * price

    def cancel_order(self, order: BithumbOrder):
        ret = self.client.cancel_order(order.get_order_desc(self.config.BRIDGE))
        return {
            "symbol": order.symbol,
            "orderId": order.id,
            "result": ret
        }

    def sell_market_order(self, order_currency: str, payment_currency: str, unit: int):
        return self.client.sell_market_order(order_currency, payment_currency, unit)

    def _wait_for_order(
        self, order_id, origin_symbol: str, target_symbol: str
    ) -> Optional[BithumbOrder]:  # pylint: disable=unsubscriptable-object
        while True:
            order_status: BithumbOrder = self.cache.orders.get(order_id, None)
            if order_status is not None:
                break
            self.logger.info(f"Waiting for order {order_id} to be created")
            time.sleep(1)

        self.logger.info(f"Order created: {order_status}")

        while order_status.status != "FILLED":

                order_status = self.cache.orders.get(order_id, None)
                self.logger.info(f"Waiting for order {order_id} to be filled")

                if self._should_cancel_order(order_status):
                    cancel_order = None
                    while cancel_order is None:
                        """cancel_order = self.binance_client.cancel_order(
                            symbol=origin_symbol + target_symbol, orderId=order_id
                        )"""
                        cancel_order = self.cancel_order(order_status)
                    self.logger.info("Order timeout, canceled...")

                    # sell partially
                    if order_status.status == "PARTIALLY_FILLED" and order_status.side == "BUY":
                        self.logger.info("Sell partially filled amount")

                        order_quantity = self._sell_quantity(origin_symbol, target_symbol)
                        partially_order = None
                        while partially_order is None:
                            partially_order = self.sell_market_order(origin_symbol, target_symbol, order_quantity)
                            """self.binance_client.order_market_sell(
                                symbol=origin_symbol + target_symbol, quantity=order_quantity
                            )"""

                    self.logger.info("Going back to scouting mode...")
                    return None

                if order_status.status == "CANCELED":
                    self.logger.info("Order is canceled, going back to scouting mode...")
                    return None
                time.sleep(1)

        self.logger.info(f"Order filled: {order_status}")
        return order_status

    def wait_for_order(
        self, order_id, origin_symbol: str, target_symbol: str, order_guard: OrderGuard
    ) -> Optional[BithumbOrder]:  # pylint: disable=unsubscriptable-object
        with order_guard:
            return self._wait_for_order(order_id, origin_symbol, target_symbol)

    def _should_cancel_order(self, order_status):
        minutes = (time.time() - order_status.time / 1000) / 60
        timeout = 0

        if order_status.side == "SELL":
            timeout = float(self.config.SELL_TIMEOUT)
        else:
            timeout = float(self.config.BUY_TIMEOUT)

        if timeout and minutes > timeout and order_status.status == "NEW":
            return True

        if timeout and minutes > timeout and order_status.status == "PARTIALLY_FILLED":
            if order_status.side == "SELL":
                return True

            if order_status.side == "BUY":
                current_price = self.get_ticker_price(order_status.symbol)
                if float(current_price) * (1 - 0.001) > float(order_status.price):
                    return True

        return False

    def buy_alt(self, origin_coin: Coin, target_coin: Coin) -> BithumbOrder:
        return self.retry(self._buy_alt, origin_coin, target_coin)

    def _buy_quantity(
        self, origin_symbol: str, target_symbol: str, target_balance: float = None, from_coin_price: float = None
    ):
        target_balance = target_balance or self.get_currency_balance(target_symbol)
        from_coin_price = from_coin_price or self.get_ticker_price(origin_symbol + target_symbol)

        origin_tick = self.get_alt_tick(origin_symbol, target_symbol)

        self.logger.info(f"_buy_quantity {target_balance}, {from_coin_price}, {origin_tick}")

        return math.floor(target_balance * 10 ** origin_tick / from_coin_price) / float(10 ** origin_tick)

    def buy_limit_order(self, order_currency: str, payment_currency: str, price: int, unit: int):
        self.logger.info(f"buy_limit_order {order_currency} {payment_currency} {str(price)} {str(unit)}")
        resp = self.client.buy_limit_order(order_currency, price, unit, payment_currency)
        self.logger.info("buy_limit_order : " + str(resp))
        if resp is None:
            return None
        elif isinstance(resp, dict):
            self.logger.info(resp)
            return None

        self.cache.order_desc[resp[2]] = resp
        order = self.client.get_order_completed(resp)
        self.logger.info("buy_limit_order detail : " + str(order))

        # self.logger.info(resp)
        # order_id = resp[2]
        # desc = [
        #     "bid",
        #     order_currency,
        #     order_id,
        #     payment_currency
        # ]
        # resp = self.client.get_order_completed(desc)
        # if resp is None:
        #     return None
        # """
        #  TODO : resp['order_status'] --> current_order_status
        # """
        # """
        # 바이낸스
        # https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#new-order--trade
        # """
        # """
        # 빗썸
        # https://apidocs.bithumb.com/docs/orders_detail
        # cumulative_quote_asset_transacted_quantity 의 의미는 뭘까? : 거래 시점에 코인 구매에 소비된 총 비용
        # 빗썸 기준에서는 defail order info에서 각 거래된 내용의 총 지불 (중계료 포함)비용을 전달 하면 될 것 같다.
        # """
        # cumulative_cost = 0
        # for contract in resp["contract"]:
        #     cumulative_cost = cumulative_cost + contract["total"]
        report = {
            "symbol": order_currency + payment_currency,
            "side": "BUY",
            "order_type": "LIMIT",
            "order_id": resp[2],
            "cumulative_quote_asset_transacted_quantity": 0.0,
            "current_order_status": "NEW",
            "order_price": price,
            "transaction_time": ""
        }

        self.logger.info("order : "+ str(report))

        return report

    def _buy_alt(self, origin_coin: Coin, target_coin: Coin):
        """
        Buy altcoin
        """
        trade_log = self.db.start_trade_log(origin_coin, target_coin, False)
        origin_symbol = origin_coin.symbol
        target_symbol = target_coin.symbol

        with self.cache.open_balances() as balances:
            balances.clear()

        origin_balance = self.get_currency_balance(origin_symbol)
        target_balance = self.get_currency_balance(target_symbol)
        from_coin_price = self.get_ticker_price(origin_symbol + target_symbol)

        self.logger.info("origin_balance : " + str(origin_balance) + " target_balance : "
                         + str(target_balance) + "from_coin_price " + str(from_coin_price))

        order_quantity = self._buy_quantity(origin_symbol, target_symbol, target_balance, from_coin_price)
        self.logger.info(f"BUY QTY[1] {order_quantity} of <{origin_symbol}>")
        order_quantity = order_quantity * 0.9
        self.logger.info(f"BUY QTY[2] {order_quantity} of <{origin_symbol}>")

        if order_quantity * from_coin_price < 500:
            self.logger.info("BUY QTY : 최소 주문금액은 500 KRW 입니다")
            return None

        # Try to buy until successful
        order = None
        order_guard = self.stream_manager.acquire_order_guard()
        while order is None:
            """order = self.binance_client.order_limit_buy(
                symbol=origin_symbol + target_symbol,
                quantity=order_quantity,
                price=from_coin_price,
            )"""
            order = self.buy_limit_order(origin_symbol, target_symbol, from_coin_price, order_quantity)
            self.logger.info(order)

        trade_log.set_ordered(origin_balance, target_balance, order_quantity)

        order_guard.set_order(origin_symbol, target_symbol, order["order_id"])
        order = self.wait_for_order(order["order_id"], origin_symbol, target_symbol, order_guard)

        if order is None:
            return None

        self.logger.info(f"Bought {origin_symbol}")
        self.cache.orders.pop(order["order_id"])
        self.cache.order_desc.pop(order["order_id"])

        trade_log.set_complete(order.cumulative_quote_qty)

        return order

    def sell_alt(self, origin_coin: Coin, target_coin: Coin) -> BithumbOrder:
        return self.retry(self._sell_alt, origin_coin, target_coin)

    def _sell_quantity(self, origin_symbol: str, target_symbol: str, origin_balance: float = None):
        origin_balance = origin_balance or self.get_currency_balance(origin_symbol)

        origin_tick = self. get_alt_tick(origin_symbol, target_symbol)
        return math.floor(origin_balance * 10 ** origin_tick) / float(10 ** origin_tick)

    def sell_limit_order(self, order_currency: str, payment_currency: str, price: int, unit: int):
        resp = self.client.sell_limit_order(order_currency, price, unit, payment_currency)
        if resp is None:
            return None

        self.cache.order_desc[resp[2]] = resp

        # resp = self.client.get_order_completed(resp)
        # if resp is None:
        #     return None
        # """
        #  TODO : resp['order_status'] --> current_order_status
        # """
        # """
        # 바이낸스
        # https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#new-order--trade
        # """
        # """
        # 빗썸
        # https://apidocs.bithumb.com/docs/orders_detail
        # cumulative_quote_asset_transacted_quantity 의 의미는 뭘까? : 거래 시점에 코인 구매에 소비된 총 비용
        # 빗썸 기준에서는 defail order info에서 각 거래된 내용의 총 지불 (중계료 포함)비용을 전달 하면 될 것 같다.
        # """
        # cumulative_cost = 0
        # for contract in resp["contract"]:
        #     cumulative_cost = cumulative_cost + contract["total"]
        report = {
            "symbol": order_currency + payment_currency,
            "side": "SELL",
            "order_type": "LIMIT",
            "order_id": resp[2],
            "cumulative_quote_asset_transacted_quantity": 0,
            "current_order_status": "NEW",
            "order_price": price,
            "transaction_time": 0,
        }
        return report

    def _sell_alt(self, origin_coin: Coin, target_coin: Coin):
        """
        Sell altcoin
        """
        trade_log = self.db.start_trade_log(origin_coin, target_coin, True)
        origin_symbol = origin_coin.symbol
        target_symbol = target_coin.symbol

        with self.cache.open_balances() as balances:
            balances.clear()

        origin_balance = self.get_currency_balance(origin_symbol)
        target_balance = self.get_currency_balance(target_symbol)
        from_coin_price = self.get_ticker_price(origin_symbol + target_symbol)

        order_quantity = self._sell_quantity(origin_symbol, target_symbol, origin_balance)
        self.logger.info(f"Selling {order_quantity} of {origin_symbol}")

        self.logger.info(f"Balance is {origin_balance}")
        order = None
        order_guard = self.stream_manager.acquire_order_guard()
        while order is None:
            order = self.sell_limit_order(origin_symbol, target_symbol, from_coin_price, order_quantity)
            # Should sell at calculated price to avoid lost coin
            """order = self.binance_client.order_limit_sell(
                symbol=origin_symbol + target_symbol, quantity=order_quantity, price=from_coin_price
            )"""

        self.logger.info("order")
        self.logger.info(order)

        trade_log.set_ordered(origin_balance, target_balance, order_quantity)

        order_guard.set_order(origin_symbol, target_symbol, (order["order_id"]))
        order = self.wait_for_order(order["order_id"], origin_symbol, target_symbol, order_guard)

        if order is None:
            return None

        new_balance = self.get_currency_balance(origin_symbol)
        while new_balance >= origin_balance:
            new_balance = self.get_currency_balance(origin_symbol, True)

        self.logger.info(f"Sold {origin_symbol}")
        self.cache.orders.pop(order["order_id"])
        self.cache.order_desc.pop(order["order_id"])
        trade_log.set_complete(order.cumulative_quote_qty)

        return order
