import json
import pylab as pl
import datetime as dt
from operator import itemgetter

def ReadData():
    global bitcoin_prices, gold_prices, dates1, dates2
    tuples = []
    dates = []
    prices = []
    with open('bitcoin_historic.json') as bitcoin_data:
        bitcoin = json.load(bitcoin_data)

    for date in bitcoin["bpi"]:
        dates.append((dt.datetime.strptime(date,'%Y-%m-%d').date()))
        prices.append(bitcoin["bpi"][date])

    tuples = zip(dates, prices)
    tuples.sort(key=itemgetter(0))
    for t in tuples:
        print t[0], "--> ",t[1]
        dates1.append(t[0])
        bitcoin_prices.append(t[1])
    with open('gold_historic.json') as gold_data:
        gold = json.load(gold_data)
       
    for info in gold["dataset"]["data"]:
        dates2.append(dt.datetime.strptime(info[0],'%Y-%m-%d').date())
        gold_prices.append(info[6])
        pass
    
bitcoin_prices = []
gold_prices = []
dates1 = []
dates2 = []

def Plot():
        pl.figure(figsize=(15,10), dpi=80, facecolor='white', edgecolor='k')
        pl.plot(dates1,bitcoin_prices,linewidth=2,color='blue',label='Bitcoin prices')
        pl.plot(dates2,gold_prices,linewidth=2,color='green',label='Gold prices')
        pl.legend()
        pl.xlabel('Date (Year)')
        pl.ylabel('Price (Euro)')
        pl.grid()
        pl.savefig('comparation.svg')
        pl.show()

if __name__ == "__main__":
    ReadData()
    Plot()
    print "Plot correctly generated"