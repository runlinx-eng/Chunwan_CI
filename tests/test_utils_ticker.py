from src.utils import to_exchange_ticker


def test_to_exchange_ticker_normalizes_a_share_codes():
    assert to_exchange_ticker("000001") == "000001.SZ"
    assert to_exchange_ticker("600000") == "600000.SH"
    assert to_exchange_ticker("830001") == "830001.BJ"


def test_to_exchange_ticker_keeps_supported_forms():
    assert to_exchange_ticker("600000.SH") == "600000.SH"
    assert to_exchange_ticker("SH600000") == "600000.SH"
    assert to_exchange_ticker("A0001") == "A0001"
