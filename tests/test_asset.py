from quantrion.asset.base import BacktestableAsset, TradableAsset


def test_asset_cached_per_subclass():
    class A(BacktestableAsset):
        pass

    class B(TradableAsset):
        pass

    a = A("AAPL")
    a2 = A("AAPL")
    b = B("AAPL")
    assert a is a2
    assert a is not b
