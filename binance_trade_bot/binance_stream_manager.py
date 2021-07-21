import sys
import threading
import time
from contextlib import contextmanager
from typing import Dict, Set, Tuple
import pybithumb
import os
##import binance.client
##from binance.exceptions import BinanceAPIException, BinanceRequestException
##from unicorn_binance_websocket_api import BinanceWebSocketApiManager
from multiprocessing import Process, Manager
from .config import Config
from .logger import Logger




from pybithumb import WebSocketManager


class BinanceOrder:  # pylint: disable=too-few-public-methods
    def __init__(self, report):
        self.event = report
        self.symbol = report["symbol"]
        self.side = report["side"]
        self.order_type = report["order_type"]
        self.id = report["order_id"]
        self.cumulative_quote_qty = float(report["cumulative_quote_asset_transacted_quantity"])
        self.status = report["current_order_status"]
        self.price = float(report["order_price"])
        self.time = report["transaction_time"]

    """    
    ('bid', 'BTC', '1530000309557335')    
    (type=order_desc[0],
       order_currency=order_desc[1],
       order_id=order_desc[2],
       payment_currency=order_desc[3])
    """

    def get_order_desc(self, bridge: str):
        order_type = "bid"
        if self.side == "SELL":
            order_type = "ask"
        return [order_type, self.symbol.replace(bridge, ""), self.id, bridge]

    def __repr__(self):
        return f"<BinanceOrder {self.event}>"


# pylint: disable=too-few-public-methods
class BinanceCache:
    ticker_values: Dict[str, float] = {}
    _balances: Dict[str, float] = {}
    _balances_mutex: threading.Lock = threading.Lock()
    non_existent_tickers: Set[str] = set()
    orders: Dict[str, BinanceOrder] = {}
    ##order_desc: Dict[str, list] = {}

    def __init__(self):
        self.order_desc = Manager().dict()

    @contextmanager
    def open_balances(self):
        with self._balances_mutex:
            yield self._balances


class OrderGuard:
    def __init__(self, pending_orders: Set[Tuple[str, int]], mutex: threading.Lock):
        self.pending_orders = pending_orders
        self.mutex = mutex
        # lock immediately because OrderGuard
        # should be entered and put tag that shouldn't be missed
        self.mutex.acquire()
        self.tag = None

    def set_order(self, origin_symbol: str, target_symbol: str, order_id: str):
        self.tag = (origin_symbol + target_symbol, order_id)

    def __enter__(self):
        try:
            if self.tag is None:
                raise Exception("OrderGuard wasn't properly set")
            self.pending_orders.add(self.tag)
        finally:
            self.mutex.release()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pending_orders.remove(self.tag)

def _print(msg : str):
    #print(msg)
    return None


def _resp_polling_process(in_config, in_processing, in_dict: Dict, out_queue):
    _print("_resp_polling_process " + str(os.getpid()))
    client = pybithumb.Bithumb(in_config.BINANCE_API_KEY, in_config.BINANCE_API_SECRET_KEY)
    _print(str(os.getpid()) + " _resp_polling_process client : " + str(client))
    while True:
        if not in_processing:
            exit()
            break
        orders = in_dict.copy()
        _print(str(os.getpid()) + " _resp_polling_process orders : " + str(orders))
        if orders.__len__ != 0:
            for order_id in orders:
                _print(str(os.getpid()) + " _resp_polling_process order : "  + str(orders[order_id]))
                resp = client.get_order_completed(orders[order_id])
                _print(str(os.getpid()) + " _resp_polling_process order_indetail : " + str(resp))
                output = {"type": "order", "id": order_id, "detail": resp}
                _print(str(os.getpid()) + " _resp_polling_process output : " + str(output))
                out_queue.put(output)
                time.sleep(1)

        for currency in in_config.SUPPORTED_COIN_LIST:
            # def get_balance(self, currency):
            #     """
            #     거래소 회원의 잔고 조회
            #     :param currency   : BTC/ETH/DASH/LTC/ETC/XRP/BCH/XMR/ZEC/QTUM/BTG/EOS/ICX/VEN/TRX/ELF/MITH/MCO/OMG/KNC
            #     :return           : (보유코인, 사용중코인, 보유원화, 사용중원화)
            #     """
            #     resp = None
            #     try:
            #         resp = self.api.balance(currency=currency)
            #         specifier = currency.lower()
            #         return (float(resp['data']["total_" + specifier]),
            #                 float(resp['data']["in_use_" + specifier]),
            #                 float(resp['data']["total_krw"]),
            #                 float(resp['data']["in_use_krw"]))
            #     except Exception:
            #         return resp
            _print(str(os.getpid()) + " _resp_polling_process currency : " + currency)
            resp = client.get_balance(currency)
            _print(str(os.getpid()) + " _resp_polling_process get_balance : " + str(resp))
            output = {"type": "balance", "currency": currency, "detail": resp}
            out_queue.put(output)
            time.sleep(1)
        time.sleep(10)


def _ticker_process(symbols: list, in_loop, que):
    _print("_ticker_process " + str(os.getpid()))
    wm = WebSocketManager(type="ticker", symbols=symbols, ticktype=["24H"])
    while True:
        if not in_loop:
            wm.terminate()
            exit()
        resp = wm.get()
        _print("_ticker_process : loop " + str(resp))
        que.put(resp)



def _transaction_process(symbols: list, que):
    _print("_transaction_process " + str(os.getpid()))
    wm = WebSocketManager(type="transaction", symbols=symbols)
    while True:
        resp = wm.get()
        _print("_transaction_process : loop " + str(resp))
        que.put(resp)


class BinanceStreamManager:
    def __init__(self, cache: BinanceCache, config: Config, client: pybithumb.Bithumb, logger: Logger):
        self.cache = cache
        self.logger = logger
        self.config = config
        self.client = client
        self.pending_orders: Set[Tuple[str, int]] = set()

        symbols = []
        for currency in self.config.SUPPORTED_COIN_LIST:
            symbols.append(f"{currency}_KRW")
            symbols.append(f"{currency}_BTC")

        self.pending_orders_mutex: threading.Lock = threading.Lock()
        self._processors = []
        self.queue = Manager().Queue()
        self._process_control = Manager().Value('b',True)
        self._processors.append(Process(target=_ticker_process, args=(symbols,self._process_control, self.queue)))
        ##not to use below process to get transaction.
        ##self._processors.append(Process(target=_transaction_process, args=(symbols, self.queue)))
        self._processors.append(Process(target=_resp_polling_process,
                                        args=
                                        (self.config, self._process_control, self.cache.order_desc, self.queue)))
        for process in self._processors:
            process.start()

        self._main_loop = threading.Thread(target=self._main_thread)
        self._main_loop.start()

    def acquire_order_guard(self):
        return OrderGuard(self.pending_orders, self.pending_orders_mutex)

    def _fetch_pending_orders(self):

        self.logger.info("_fetch_pending_orders")

        pending_orders: Set[Tuple[str, int]]
        with self.pending_orders_mutex:
            pending_orders = self.pending_orders.copy()
        for (symbol, order_id) in pending_orders:
            resp = None
            while True:
                desc = self.cache.order_desc[order_id]
                order = self.client.get_order_completed(desc)
                self.logger.info("_fetch_pending_orders : " + str(order))
                if order is not None:
                    resp = order
                    break
                time.sleep(1)
            fake_report = self._parse_order(order_id, resp)
            self.logger.info(f"Pending order {order_id} for symbol {symbol} fetched:\n{fake_report}", False)
            self.cache.orders[fake_report["order_id"]] = BinanceOrder(fake_report)

    def _invalidate_balances(self):
        with self.cache.open_balances() as balances:
            balances.clear()

    def _parse_order(self, order_id, detail):
        data = detail["data"]
        order_qty = float(data["order_qty"])
        filled_qty = 0.0
        cumulative_cost = 0

        for contract in data["contract"]:
            cumulative_cost = cumulative_cost + int(contract["total"])
            filled_qty = filled_qty + float(contract["units"])

        side = "BUY"
        if data["type"] == "ask":
            side = "SELL"

        order_status = "NEW"
        detail_status = data["order_status"]
        if detail_status == "Pending":
            if filled_qty == 0.0:
                order_status = "NEW"
            elif filled_qty < order_qty:
                order_status = "PARTIALLY_FILLED"
        elif detail_status == "Completed":
            order_status = "FILLED"
        elif detail_status == "Cancel":
            order_status = "CANCELED"

        report = {
            "symbol": data["order_currency"] + data["payment_currency"],
            "side": side,
            "order_type": "LIMIT",
            "order_id": order_id,
            "cumulative_quote_asset_transacted_quantity": cumulative_cost,
            "current_order_status": order_status,
            "order_price": data["order_price"],
            "transaction_time": int(data["order_date"]),
        }
        return report

    def _main_thread(self):
        self.logger.info("_main_thread")
        self._fetch_pending_orders()
        self._invalidate_balances()
        while True:
            if not self.is_alive():
                self.logger.info("_main_thread : loop - exit")
                self.close()
                sys.exit()
            msg = self.queue.get()
            ##self.logger.info("_main_thread : " + str(msg))
            if msg["type"] == "order":
                report = self._parse_order(msg["id"], msg["detail"])
                self.logger.info("_main_thread " + str(report))
                self.cache.orders[msg["id"]] = BinanceOrder(report)
            elif msg["type"] == "ticker":
                content = msg["content"]
                if content["tickType"] == "24H":
                    symbol = str(content["symbol"]).replace("_", "")
                    close_price = float(content["closePrice"])
                    self.cache.ticker_values[symbol] = close_price
            elif msg["type"] == "balance":
                currency = msg["currency"]
                with self.cache.open_balances() as balances:
                    if currency in balances:
                        total_qty = msg["detail"][0]
                        used_qty = msg["detail"][1]
                        free_qty = total_qty - used_qty
                        if free_qty == 0.0:
                            del balances[currency]
                        else:
                            balances[currency] = free_qty
        #
        #     self.logger.info("_process_stream_data : end")
        # https://github.com/binance/binance-spot-api-docs/blob/master/user-data-stream.md
        # event_type = stream_data["event_type"]
        # if event_type == "executionReport":  # !userData
        #     self.logger.info(f"execution report: {stream_data}")
        #     order = BinanceOrder(stream_data)
        #     self.cache.orders[order.id] = order

        # elif event_type == "balanceUpdate":  # !userData
        #     self.logger.info(f"Balance update: {stream_data}")
        #     with self.cache.open_balances() as balances:
        #         asset = stream_data["asset"]
        #         if asset in balances:
        #             del balances[stream_data["asset"]]

        # #         return (float(resp['data']["total_" + specifier]),
        # #                 float(resp['data']["in_use_" + specifier]),
        # #                 float(resp['data']["total_krw"]),
        # #                 float(resp['data']["in_use_krw"]))
        # #     except Exception:
        # #         return resp
        # self.logger.info(str(os.getpid()) + " _resp_polling_process currency : " + currency)
        # resp = client.get_balance(currency)
        # self.logger.info(str(os.getpid()) + " _resp_polling_process get_balance : " + str(resp))
        # output = {"type": "balance", "currency": currency, "detail": resp}


        # elif event_type in ("outboundAccountPosition", "outboundAccountInfo"):  # !userData
        #     self.logger.info(f"{event_type}: {stream_data}")
        #     with self.cache.open_balances() as balances:
        #         for bal in stream_data["balances"]:
        #             balances[bal["asset"]] = float(bal["free"])

        # elif event_type == "24hrMiniTicker":
        #     for event in stream_data["data"]:
        #         self.cache.ticker_values[event["symbol"]] = float(event["close_price"])
        # else:
        #     self.logger.error(f"Unknown event type found: {event_type}\n{stream_data}")

                # {
                #     "type": "ticker",
                #     "content": {
                #                    "symbol": "BTC_KRW", // 통화코드
                #     "tickType": "24H", // 변동
                # 기준시간 - 30
                # M, 1
                # H, 12
                # H, 24
                # H, MID
                # "date": "20200129", // 일자
                # "time": "121844", // 시간
                # "openPrice": "2302", // 시가
                # "closePrice": "2317", // 종가
                # "lowPrice": "2272", // 저가
                # "highPrice": "2344", // 고가
                # "value": "2831915078.07065789", // 누적거래금액
                # "volume": "1222314.51355788", // 누적거래량
                # "sellVolume": "760129.34079004", // 매도누적거래량
                # "buyVolume": "462185.17276784", // 매수누적거래량
                # "prevClosePrice": "2326", // 전일종가
                # "chgRate": "0.65", // 변동률
                # "chgAmt": "15", // 변동금액
                # "volumePower": "60.80" // 체결강도
                # }
                # }


    def close(self):
        ##self.bw_api_manager.stop_manager_with_all_streams()
        self.logger.info("close")
        self._process_control.set(False)
        for process in self._processors:
            process.terminate()
        self._main_loop.join()

    def is_alive(self):
        for process in self._processors:
            if not process.is_alive():
                return False
        return True

