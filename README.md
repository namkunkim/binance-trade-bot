# bithumb-trade-bot
bithumb trade bot은 https://github.com/edeng23/binance-trade-bot 를 기반으로 합니다.
binance가 websocket으로 다양한 api를 제공하고 있으나. bithumb의 api 정보는 그렇지 못한 편입니다.
바이낸스는 BNB와 USDT를 기본 거래 단위로 사용하고, 그것에 맞게 api가 구성된 반면,
bithumb는 원화 기준이라 생각했던 api 1:1 변경이 안됩니다.

대부분의 수정은 bithumb_api_manager.py와 bithumb_stream_manager.py에서 진행했습니다.

bithumb을 위한 작업은 이것으로 마무리 하고 업비트 API로 변경해보고자 합니다.
binance -> bithumb 변경을 위해 검토했던 내용은 아래를 참고하면 됩니다.
https://nacodingstroy.tistory.com/
