import json
import ssl
import urllib.parse
import urllib.request
import certifi
import requests
import pandas as pd
from datetime import datetime
params = urllib.parse.urlencode(
    {
        "start": "1",
        "limit": "15",
        "convert": "USD",
    }
)
request = urllib.request.Request(
    f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?{params}",
    headers={
        "Accept": "application/json",
        "X-CMC_PRO_API_KEY": "c1d79d5d81aa4890af1b30c61395ed51",
    },
)
context = ssl.create_default_context(cafile=certifi.where())
with urllib.request.urlopen(request, context=context) as response:
    data = json.load(response)

# print(data)

pd.set_option('display.max_columns', None)
df = pd.json_normalize(data['data'])

df1=df[[
    'id','name','quote.USD.price','quote.USD.market_cap','total_supply','max_supply','cmc_rank', 'quote.USD.percent_change_24h',
        'quote.USD.last_updated']]

df1['time_stamp'] = pd.to_datetime('now')
print(df1)
