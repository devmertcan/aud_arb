from typing import List, Tuple, Any, Dict

def parse_ir_event(event: Dict) -> List[Tuple[str, Any]]:
    """
    Map Independent Reserve events to normalized actions.
    - NewOrder        -> add(order_id, side, price, volume)
    - OrderChanged    -> change(order_id, volume)
    - OrderCanceled   -> cancel(order_id)
    """
    actions: List[Tuple[str, Any]] = []
    evt = event.get("Event")
    d = event.get("Data", {})

    if evt == "NewOrder":
        side = "bid" if d["OrderType"] == "LimitBid" else "ask"
        # IR provides price object with 'aud', 'usd', 'nzd', 'sgd' – we use AUD
        price = float(d["Price"]["aud"])
        volume = float(d["Volume"])
        actions.append(("add", {"order_id": d["OrderGuid"], "side": side, "price": price, "volume": volume}))

    elif evt == "OrderChanged":
        actions.append(("change", {"order_id": d["OrderGuid"], "volume": float(d["Volume"])}))

    elif evt == "OrderCanceled":
        actions.append(("cancel", {"order_id": d["OrderGuid"]}))

    # Ignore Heartbeat / Subscriptions at parser level.
    return actions
