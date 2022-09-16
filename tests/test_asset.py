from quantrion.asset.base import Asset, TradableAsset


def test_asset_cached_per_subclass():
    class A(Asset):
        pass

    class B(TradableAsset):
        pass

    a = A("AAPL")
    a2 = A("AAPL")
    b = B("AAPL")
    assert a is a2
    assert a is not b
