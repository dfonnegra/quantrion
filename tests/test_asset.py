from quantrion.asset.base import USStock


def test_asset_cached_per_subclass():
    class A(USStock):
        pass

    class B(USStock):
        pass

    a = A("AAPL")
    a2 = A("AAPL")
    b = B("AAPL")
    assert a is a2
    assert a is not b
