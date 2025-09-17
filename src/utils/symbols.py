EXCHANGE_SYMBOL_MAP = {
    "independentreserve": {"BTC/AUD": "XbtAud"},
    "kraken": {"BTC/AUD": "XXBTZTAUD"},
    # add others
}

def get_exchange_symbol(exchange_id, unified_symbol):
    return EXCHANGE_SYMBOL_MAP[exchange_id][unified_symbol]
