def test_checklist():
    """
    Sanity check that the environment is wired correctly.
    """
    import connectors.ccxt_rest as ccxt_rest
    import orderbook.exchange_orderbook_manager as obm
    import detector.arb_detector as det
    import reporter.csv_reporter as rep

    assert hasattr(ccxt_rest, "CCXTRest")
    assert hasattr(obm, "ExchangeOrderbookManager")
    assert hasattr(det, "ArbDetector")
    assert hasattr(rep, "CSVReporter")
