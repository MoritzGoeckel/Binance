import pandas as pd
import urllib.request, json 
import matplotlib
from sortedcontainers import SortedDict
from datetime import datetime
from os import listdir, makedirs
from os.path import isfile, join, exists

data_path = "data"

def downloadTrades(pair, fromId):
    url = ""
    if fromId == None:
        url = f'https://api4.binance.com/api/v3/historicalTrades?symbol={pair}&limit=1000'
    else:
        url = f'https://api4.binance.com/api/v3/historicalTrades?symbol={pair}&limit=1000&fromId={fromId}'
    with urllib.request.urlopen(url) as connection:
        return json.load(connection)

def save(pair, buffer):
    if len(buffer) == 0:
        return

    keys = sorted(buffer.keys())
    
    firstId = keys[0]
    lastId = keys[len(keys)-1]

    if not exists(join(data_path, pair)):
        makedirs(join(data_path, pair))
    
    with open(join(data_path, pair, f'{firstId}-{lastId}.txt'), 'w') as file:
        file.writelines("\n".join([json.dumps(buffer[key]) for key in keys]))

def makeTime(millis):
    return datetime.fromtimestamp(millis/1000).strftime("%d.%m.%Y, %H:%M:%S")

def getDataFiles(pair):
    if not exists(join(data_path, pair)):
        return []
    return [join(data_path, pair, file) for file in listdir(join(data_path, pair)) if isfile(join(data_path, pair, file)) and file != 'exceptions.txt']

def loadTradesDict(pair):
    buffer = {}
    lines = 0
    for filename in getDataFiles(pair):
        with open(filename) as file:
            for line in file:
                lines = lines + 1
                entry = json.loads(line.rstrip())
                buffer[entry['id']] = entry
            
    print(f'Loaded {len(buffer)} trades from {lines} lines')
    return buffer

def loadTradesList(pair):
    trades = loadTradesDict(pair)
    data = []
    keys = sorted(trades.keys())
    for key in keys:
        trade = trades[key]
        data.append([trade['id'], trade['price'], trade['qty'], trade['time'], trade['isBuyerMaker'], trade['isBestMatch']])
    return data

def loadTradesPd(pair):
    trades = loadTradesList(pair)
    df = pd.DataFrame(trades, columns=('id', 'price', 'qty', 'time', 'isBuyerMaker', 'isBestMatch'))
    df['id'] = df['id'].astype(int)
    df['price'] = df['price'].astype(float)
    df['qty'] = df['qty'].astype(float)
    df['isBuyerMaker'] = df['isBuyerMaker'].astype(bool)
    df['isBestMatch'] = df['isBestMatch'].astype(bool)
    return df

def startDownload(pair, smallestId):
    buffer = {}   
    while True:
        try:
            data = downloadTrades(pair, smallestId - 1001 if smallestId != None else None)
        
            for entry in data:
                id = entry['id']
                buffer[id] = entry
                smallestId = min(id, smallestId if smallestId != None else id)
        
            print(f'Buffer size: {len(buffer)} Last id: {smallestId} Time: {makeTime(buffer[smallestId]['time'])}')
        except Exception as e:
            print(e)
            with open(f'exceptions.txt', 'a') as file:
                file.writelines(f'{str(e)}\n')
            
        if len(buffer) > 1000 * 100:
            save(pair, buffer)
            buffer = {}