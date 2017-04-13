import json,urllib
from pprint import pprint

key = "GTYWrxHmbfy6aH-RWtnQ"
url = "https://www.quandl.com/api/v3/datasets/LBMA/GOLD.json?api_key="+key+"&start_date=2010-07-17"
response = urllib.urlopen(url)
data = json.loads(response.read())

with open('gold_historic.json', 'w') as outfile:
    json.dump(data, outfile)

for info in data["dataset"]["data"]:
    print info[0] ,"-->", info[6]
