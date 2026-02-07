import pandas as pd

from src.data_provider import AkshareProvider


def test_resolve_name_prefers_code_name_map_for_placeholder():
    provider = AkshareProvider()
    row = pd.Series({"name": "STOCK_000001"})
    assert provider._resolve_name(row, "000001", {"000001": "平安银行"}) == "平安银行"


def test_resolve_name_keeps_non_placeholder_name():
    provider = AkshareProvider()
    row = pd.Series({"名称": "万科A"})
    assert provider._resolve_name(row, "000002", {"000002": "备用名"}) == "万科A"
