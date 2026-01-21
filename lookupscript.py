from ib_insync import IB, Stock

ib = IB()
ib.connect("127.0.0.1", 4002, clientId=999)

cds = ib.reqContractDetails(Stock("SBAC", "SMART", "USD"))
for cd in cds:
    c = cd.contract
    print(
        "conId", c.conId,
        "symbol", c.symbol,
        "primaryExch", getattr(c, "primaryExchange", None),
        "exch", c.exchange,
        "currency", c.currency,
        "priceMagnifier", getattr(c, "priceMagnifier", None),
        "minTick", getattr(cd, "minTick", None),
    )

ib.disconnect()
