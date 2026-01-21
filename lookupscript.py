from ib_insync import IB, Stock
import time

ib = IB()
ib.connect("127.0.0.1", 4002, clientId=999)

cds = ib.reqContractDetails(Stock("SBAC", "SMART", "USD"))

print("\n=== SBAC CONTRACTS + PRICE SNAPSHOT ===\n")

for cd in cds:
    c = cd.contract

    # request SNAPSHOT market data
    t = ib.reqMktData(c, "", snapshot=True, regulatorySnapshot=False)
    ib.sleep(0.8)   # give IB time to populate fields

    # derive a usable price
    bid = t.bid if t.bid and t.bid > 0 else None
    ask = t.ask if t.ask and t.ask > 0 else None
    last = t.last if t.last and t.last > 0 else None
    close = t.close if t.close and t.close > 0 else None

    if bid and ask:
        price = (bid + ask) / 2
        px_src = "mid"
    elif last:
        price = last
        px_src = "last"
    elif close:
        price = close
        px_src = "close"
    else:
        price = None
        px_src = "none"

    print(
        f"conId={c.conId} "
        f"secType={c.secType} "
        f"symbol={c.symbol} "
        f"localSymbol={getattr(c,'localSymbol',None)} "
        f"primaryExch={getattr(c,'primaryExchange',None)} "
        f"exch={c.exchange} "
        f"currency={c.currency} "
        f"price={price} ({px_src})"
    )

    ib.cancelMktData(c)  # clean up snapshot subscription

ib.disconnect()
