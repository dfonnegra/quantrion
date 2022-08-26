from quantrion.ticker.ticker import Ticker


def test_ticker_cached_per_subclass():
    class A(Ticker):
        pass

    class B(Ticker):
        pass

    a = A("AAPL")
    a2 = A("AAPL")
    b = B("AAPL")
    assert a is a2
    assert a is not b
