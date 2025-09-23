# Normalizes symbols and maps any exchange-specific quirks to standard "BASE/QUOTE" form.

AUD = "AUD"

CANONICAL = {
    # add special cases if an exchange uses different tickers
    # e.g., "XBT/AUD" -> "BTC/AUD"
    "XBT": "BTC",
}

def canonical_base(base: str) -> str:
    return CANONICAL.get(base.upper(), base.upper())

def to_canonical(pair: str) -> str:
    try:
        base, quote = pair.split("/")
    except ValueError:
        return pair.upper()
    return f"{canonical_base(base)}/{quote.upper()}"

def is_aud_pair(pair: str) -> bool:
    return pair.upper().endswith("/AUD")
