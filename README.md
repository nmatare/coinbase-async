# coinbase-async

[![Inline docs](http://inch-ci.org/github/dwyl/hapi-auth-jwt2.svg?branch=master)](http://inch-ci.org/github/dwyl/hapi-auth-jwt2)
[![HitCount](http://hits.dwyl.io/nmatare/coinbase-async.svg)](http://hits.dwyl.io/nmatare/coinbase-async)

Asynchronous python3 [coinbase](https://www.coinbase.com/) client with [Google 
Big Query](https://cloud.google.com/bigquery/) support (unofficial)

See the official API documentation [here](https://docs.prime.coinbase.com/) and
[here](https://cloud.google.com/bigquery/docs/)

## Features

  * Full, near real-time tracking of all trades and quotes placed onto the Coinbase
    exchange
  * Asynchronous WebSocket feed and asynchronous GET/POST requests
  * (Optional) Google BigQuery storage 

## Requirements
  
  * python>=3.6.5
  * aiohttp>=3.3.0
  * gcloud-aio-auth>=1.0.0
  * gcloud-aio-bigquery>=1.0.0
  * google-cloud-bigquery>=1.2.0
  * urllib3>=1.22
  * async-timeout>=3.0.0
  * ujson>=1.35

## Enhancements (Optional)

  * ciso8601>=1.0.8

## Installation

Install from github:

    pip3 install git+https://github.com/nmatare/coinbase-async.git#egg=measurements

You can download the source file [here](https://github.com/nmatare/coinbase-async/archive/master.zip), installing it manually:

    python3 setup.py install

## Naive Example

```python
import asyncio
from coinbase.connect import MessageHandler

async def main(product_ids, service_file=None, **kwargs): 
    async with MessageHandler(
        product_ids=product_ids, service_file=service_file, **kwargs
    ) as exchange:
        print("Connecting to Coinbase WebSocket Server")
        if exchange.datasets:
            print("Streaming data into Google BigQuery")
        while True:
            message = await exchange.process_message()
            if message == "":
                pass
            else:
                print(message)

if __name__ == "__main__":
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main(
      product_ids=["LTC-USD"],
      subscribe_message={
              "type": "subscribe",
              "product_ids": ["LTC-USD"],
              "channels": ["full"]
      }
  ))

# Connecting to Coinbase WebSocket Server
# Received orderbook snapshot for LTC_USD on 2018-06-09 05:53:04.143313
# Subscribed to Coinbase ['full'] WebSocket channels for products: ['LTC_USD']
# LTC_USD traded @ 120.46 on 2018-06-09 05:53:20.284000+00:00  
# LTC_USD traded @ 120.46 on 2018-06-09 05:53:20.284000+00:00  
# LTC_USD traded @ 120.47 on 2018-06-09 05:53:20.284000+00:00  
# LTC_USD traded @ 120.49 on 2018-06-09 05:53:20.284000+00:00
# ....

```

## Database (BigQuery) Example

```python
import asyncio
from coinbase.connect import MessageHandler
from coinbase.bigquery import CreateBigQuery

async def main(product_ids, service_file=None, **kwargs): 
    async with MessageHandler(
        product_ids=product_ids, service_file=service_file, **kwargs
    ) as exchange:
        print("Connecting to Coinbase WebSocket Server")
        if exchange.datasets:
            print("Streaming data into Google BigQuery")
        while True:
            message = await exchange.process_message()
            if message == "":
                pass
            else:
                print(message)

if __name__ == "__main__":
  db = CreateBigQuery(
        product_ids=["LTC-USD"],
        service_file="~/service_file.json",
        brand_new=True # sends GET requests to create datasets/tables
  )
  # db.delete_datasets("LTC_USD")

  loop = asyncio.get_event_loop()
  loop.run_until_complete(main(
      product_ids=["LTC-USD"],
      service_file="~/service_file.json"
  ))

# Connecting to Coinbase WebSocket Server
# Streaming data into Google BigQuery 
# Received orderbook snapshot for LTC_USD on 2018-06-09 05:53:04.143313
# Subscribed to Coinbase ['full', 'level2'] WebSocket channels for products: ['LTC_USD']
# ....
# Sent batch of 500 orderbook for product_id 'LTC_USD' to BigQuery
# Sent batch of 500 quotes for product_id 'LTC_USD' to BigQuery
# LTC_USD traded @ 605.0 on 2018-06-09 06:03:33.251000+00:00
# ....

```

## Command Line Example 

```bash
cd coinbase-async
python3 main.py \ 
  --product_ids "LTC-USD" \
  --service_file "~/service_file.json" \
  --logfile "ltc_usd.log"

```

## TODO
  
  * Test scripts and UAT
  * ~~User-based authentication to Coinbase~~
  * Only tested on channels 'full' and 'l2update'; add support for ticker,
    matches, user, and other channels
  * Asynchronously create datasets/tables

## Citations

  * The class coinbase.connect.requests.Request is based off the work done by
  [Daniel Paquin and Kornel Csernai](https://github.com/csko/gdax-python-api)

## License

This project is licensed under the Apache License Version 2.0 - see 
[LICENSE.md](https://github.com/nmatare/coinbase-async/blob/master/README.md) 
file for details

