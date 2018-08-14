#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
#  Copyright 2016-2018 Nathan Matare 
#  
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# "Author: Nathan Matare <nathan.matare@chicagobooth.edu>"

import asyncio
from coinbase.connect import MessageHandler

async def main(product_ids, service_file=None, **kwargs): # pragma: no cover
	async with MessageHandler(
		product_ids=product_ids, service_file=service_file, **kwargs
	) as exchange:
			logger.info("Connecting to Coinbase WebSocket Server")
			if exchange.datasets:
				logger.info("Streaming data into Google BigQuery")
			while True:
				message = await exchange.process_message()
				if message == "":
						pass
				else:
						logger.info(message)

if __name__ == "__main__":
	import argparse
	import os
	import sys
	import logging

	parser = argparse.ArgumentParser()
	parser.add_argument('-b', '--product_ids',
											required=True,
											type=str,
											default="BTC-USD",
											nargs='+',
											help="Coinbase product ids: "
													 "https://docs.gdax.com/#products")

	parser.add_argument('-s', '--service_file',
											required=False,
											type=str,
											default=None,
											help="If using Google BigQuery, The path/filename of"
													 " a JSON service account keyfile")

	parser.add_argument('-l', '--logfile',
											required=False,
											type=str,
											default="", 
											help="The path/file to output the logfile")

	args = parser.parse_args()
	logger = logging.getLogger(__name__)
	formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s', 
			datefmt='%a, %d %b %Y %H:%M:%S')
	logger.setLevel(logging.INFO)
	console = logging.StreamHandler()
	console.setLevel(logging.DEBUG)
	console.setFormatter(formatter)
	logger.addHandler(console)

	if args.logfile is not "":
		try:
				os.remove(args.logfile)
		except OSError:
				pass            
		log_to_file = logging.FileHandler(args.logfile)
		log_to_file.setLevel(logging.INFO)
		log_to_file.setFormatter(formatter)
		logger.addHandler(log_to_file)

	sys.tracebacklimit = 0
	loop = asyncio.get_event_loop()
	while True:
		try:
			loop.run_until_complete(main(
					product_ids=args.product_ids, 
					service_file=args.service_file
			))
		except (KeyboardInterrupt, SystemExit, Exception) as e:
			loop.run_until_complete(asyncio.sleep(5.0))
			logger.exception("Connection failed: " + str(e))
		finally:
			asyncio.gather(*asyncio.Task.all_tasks()).cancel()
			logger.info("Restarting connection") 