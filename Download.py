from Lib import loadTradesDict, startDownload, makeTime

pair = "PEPEUSDT"
trades = loadTradesDict(pair)
smallestId = None if len(trades) == 0 else min(trades.keys())
if smallestId != None:
    print(f"Smallest id: {smallestId} @ {makeTime(trades[smallestId]['time'])}")

startDownload(pair, smallestId)