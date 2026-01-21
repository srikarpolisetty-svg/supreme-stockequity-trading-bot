from ib_insync import IB, Stock

ib = IB()
ib.connect("127.0.0.1", 4002, clientId=999)

cds = ib.reqContractDetails(Stock("SBAC", "SMART", "USD"))
for cd in cds:
    c = cd.contract
    print(
        f"conId={c.conId}, secType={c.secType}, symbol={c.symbol}, "
        f"primaryExchange={getattr(c,'primaryExchange',None)}, exch={c.exchange}, "
        f"currency={c.currency}"
    )

ib.disconnect()
