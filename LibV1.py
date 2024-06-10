import pandas as pd
import urllib.request, json 
import matplotlib
from sortedcontainers import SortedDict
import datetime
import os
import os.path
import zipfile
from enum import Enum

class Interval(Enum):
    monthly = 0
    daily = 1

def formatDateFileName(date, interval):
    if interval == Interval.monthly:
        return date.strftime("%Y-%m")
    else:
        return date.isoformat()

def advanceDate(sourceDate, interval):
    if interval == Interval.monthly:
        year = sourceDate.year + sourceDate.month // 12
        month = sourceDate.month % 12 + 1
        return datetime.date(year, month, 1)
    else:
        return date + datetime.timedelta(days=1)

def downloadTrades(pair, date, interval):
    fileName = f'{pair}-trades-{formatDateFileName(date, interval)}.zip'
    targetPath = os.path.join("data", "v1", interval.name, pair)
    targetFile = os.path.join(targetPath, fileName)
    if not os.path.exists(targetPath):
        os.makedirs(targetPath)
    if os.path.exists(targetFile):
        return False
    url = f'https://data.binance.vision/data/spot/{interval.name}/trades/{pair}/{fileName}'
    print(f"Download trades {interval.name}: {url}")
    urllib.request.urlretrieve(url, targetFile)
    return True

def parseBool(text):
    return text == "True"

def parseLine(line):
    line = line.rstrip()
    elems = line.split(',')
    # id price qty (base_qty) time is_buyer_maker is_best_match
    return [int(elems[0]), float(elems[1]), float(elems[2]), int(elems[4]), parseBool(elems[5]), parseBool(elems[6])]

def readTrades(pair, date, interval):
    name = f'{pair}-trades-{formatDateFileName(date, interval)}'
    zipName = os.path.join("data", "v1", interval.name, pair, f'{name}.zip')
    lines = []
    with zipfile.ZipFile(zipName, 'r') as archive:
        with archive.open(f'{name}.csv') as file:
            lines = file.readlines()
    return [parseLine(line.decode('utf8')) for line in lines]

def loadTrades(pair, fromDate, toDate, interval):
    currentDate = fromDate
    trades = []
    while currentDate < toDate:
        downloadTrades(pair, currentDate, interval)
        trades = trades + readTrades(pair, currentDate, interval)
        currentDate = advanceDate(currentDate, interval)
    print(f"Loaded {len(trades)} trades")
    
    df = pd.DataFrame(trades, columns=('id', 'price', 'qty', 'time', 'is_buyer_maker', 'is_best_match'))
    df['id'] = df['id'].astype(int)
    df['price'] = df['price'].astype(float)
    df['qty'] = df['qty'].astype(float)
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df['is_buyer_maker'] = df['is_buyer_maker'].astype(bool)
    df['is_best_match'] = df['is_best_match'].astype(bool)
    df.set_index('time')
    return df

def getInfo():
    with urllib.request.urlopen(f'https://api.binance.com/api/v3/exchangeInfo') as connection:
        return json.load(connection)

def getSymbols(pred=None):
    symbols = []
    for symbol in getInfo()['symbols']:
        symbols.append(symbol['symbol'])
    if pred is not None:
        symbols = [ sym for sym in symbols if pred(sym)]
    symbols.sort()
    return symbols