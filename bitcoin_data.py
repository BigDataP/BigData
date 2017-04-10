import json,urllib
from pprint import pprint
from datetime import date

url = "http://api.coindesk.com/v1/bpi/historical/close.json?start=2010-07-17&end="+str(date.today())
response = urllib.urlopen(url)
data = json.loads(response.read())

with open('bitcoin_historic.json', 'w') as outfile:
    json.dump(data, outfile)

for date in data["bpi"]:
    print date ,"-->", data["bpi"][date]
