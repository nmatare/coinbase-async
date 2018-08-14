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

""" Reads and records message from the Coinbase WebSocket API  """

import asyncio
import aiohttp

from datetime import datetime   
from itertools import zip_longest
from uuid import UUID
from decimal import Decimal

try:
		from ciso8601 import parse_datetime
except ImportError:
		from dateutil.parser import parse as parse_datetime

from .websocket import WebSocket
from .requests import Request
from ..bigquery.interact import AsyncBigQuery

_ORDERBOOK_SNAPSHOT_ON = 1000000    # 1M events must pass before snapshot
_INSERT_STREAMING_BATCH_SIZE = 500  # Recommended by Google

class MessageHandler(WebSocket, Request, AsyncBigQuery):
	"""
	Reads message traffic from the Coinbase exchange

	Args:
			product_ids (list):
					A list of available products as specified by Coinbase: 
					https://docs.prime.coinbase.com/#products

	Raises:
			MessageHandlerError:
					Raised if errors are detected outside of normal operation

	"""
	def __init__(self, product_ids, **kwargs):
		if not isinstance(product_ids, list): 
				product_ids = [product_ids]
		self.product_ids = product_ids
		self.sequences = [product_id.replace(
				"-", "_") for product_id in self.product_ids]
		self.sequences = dict(zip(self.sequences, [0]*len(self.sequences)))
		super().__init__(product_ids=product_ids, **kwargs)
		self.datasets = (False if kwargs.pop("service_file") is None 
				else self.datasets)
		self._init_message_cache()


	def _init_message_cache(self):
		"""
		Initialize the message cache for the requested tables and product_ids.
		When the message cache exceeds the batch_size, the cache will be sent to
		the database and subsequently cleared

		"""
		if self.datasets:
				self.message_cache = {}
				for product_id in self.product_ids:
						self.message_cache.update({product_id : 
								dict(zip(self.tables.values(), [[] for _ in range(
										len(self.tables))]))})
							
	def _validate_message(self, message):
		"""
		Validate the received message

		Args:
				message (dict):
						A message from the Coinbase websocket API
		
		:rtype: dictionary
		:returns: validated message

		https://docs.oracle.com/cd/E19957-01/806-3568/ncg_goldberg.html

		"""
		message['type'] = message.get('type', "unknown")
		if message.get('time'):
				message['time'] = parse_datetime(message['time'])
		else:
				if message['type'] == 'snapshot':
						# Note: snapshot messages don't have timestamp, so we must
						# record the time the message is received by the client. This,
						# however, will fail to reflect the true time of the orderbook 
						# snapshot and will result in a temporarily out-of-sync 
						# orderbook
						message['time'] = datetime.utcnow()
				else:
						message['time'] = None

		if message.get('product_id'):
				message['product_id'] = message['product_id'].replace("-", "_")

		if message.get('changes'):

				if message['changes'][0][0] == "buy":
						message['changes'][0][0] = 1

				if message['changes'][0][0] == "sell":
						message['changes'][0][0] = -1

				message['changes'][0][1] = Decimal(
						message['changes'][0][1]) # depth

				message['changes'][0][2] = Decimal(
						message['changes'][0][2]) # level

				return message # exit early

		if message.get('price'):
				message['price'] = Decimal(message['price'])

		if message.get('funds'):
				message['funds'] = Decimal(message['funds'])

		if message.get('size'):
				message['size'] = Decimal(message['size'])

		if message.get('remaining_size'):
				message['remaining_size'] = Decimal(message['remaining_size'])

		if message.get('sequence'):
				message['sequence'] = int(message['sequence'])

		if message.get('trade_id'):
				message['trade_id'] = int(message['trade_id'])
		
		if message.get('side'):
				if message['side'] == "buy":
						message['side'] = 1

				if message['side'] == "sell":
						message['side'] = -1

		return message

	async def record_messages(self, product_id, table_ref, messages):
			"""
			Record a message(s) from the Coinbase websocket API 

			Args:
					product_id (str):
							A string representing the desired product_id 
					table (str):
							The table where the message(s) shall be stored into
					messages (dict):
							Message(s) from the Coinbase websocket API

			""" 
			table = self.datasets[product_id]['tables'][table_ref]

			def chunker(iterable, n, fillvalue=None):
					args = [iter(iterable)] * n
					return zip_longest(*args, fillvalue=fillvalue)

			def convert_to_row(record, side, timestamp):
					return dict(time=timestamp, side=side, 
											level=Decimal(record[0]), depth=Decimal(record[1]))

			errors = []
			if table_ref in [self.TABLE_TRADES, self.TABLE_QUOTES]:
					errors = await self.insert_rows(table, messages)
			else:
					snapshots = [message for message in 
							messages if message['type'] == 'snapshot']

					if len(snapshots) > 1:
							raise MessageHandlerError(
									"Message cache overflow; check the snapshot frequency or"
									"network connectivity")
					elif len(snapshots) > 0:

							timestamp = snapshots[0]['time']

							asks = [convert_to_row(record=record, side=1,
									timestamp=timestamp) for record in snapshots[0]['asks']]

							bids = [convert_to_row(record=record, side=-1,
									timestamp=timestamp) for record in snapshots[0]['bids']]

							chunks = chunker(
									iterable=asks + bids, 
									n=_INSERT_STREAMING_BATCH_SIZE
							)

							while True:
									try:
											chunk = [x for x in next(chunks) if x is not None]
											error = await self.insert_rows(table, chunk)
											errors.extend(error)
									except StopIteration:
											break
					else:
							l2updates = [message for message in 
									messages if message['type'] == 'l2update']

							messages_as_rows = ([convert_to_row(
											record=message['changes'][0][1:], 
											side=message['changes'][0][0],
											timestamp=message['time']) for message in l2updates])
							errors = await self.insert_rows(table, messages_as_rows)

			if errors != []:
					raise MessageHandlerError(
							"Error inserting Coinbase message(s) into BigQuery table")

	def _retrieve_message_cache(self, message):
		"""
		Retrieve the message cache

		Args:
				message (dict):
						A message from the Coinbase websocket API

		:rtype: list, string, string
		:returns: list of message cache, the table name, and 'product_id' string

		""" 
		if message['type'] == "subscriptions":
				return [None], None, None

		message_product_id = message['product_id']
		if message['type'] == 'match':
				cache, table_ref = (self.message_cache[message_product_id
						][self.TABLE_TRADES], self.TABLE_TRADES)

		elif message['type'] in self.MESSAGE_TYPES:
				cache, table_ref = (self.message_cache[message_product_id
						][self.TABLE_QUOTES], self.TABLE_QUOTES)

		elif message['type'] in ['snapshot', 'l2update']:
				cache, table_ref = (self.message_cache[message_product_id
						][self.TABLE_ORDERBOOK], self.TABLE_ORDERBOOK)
		else:
				raise TypeError 

		return cache, table_ref, message_product_id

	async def process_message(self, batch_size=_INSERT_STREAMING_BATCH_SIZE):
		"""
		Public method to process the incoming message from the 
		GDAX WebSocket API

		Args:
				batch_size (string):
						The number of messages to collect before sending to the remote
						database; alternatively, the size of the message cache

		:rtype: str
		:returns: "" (Nothing) or text output of message status

		""" 
		post = ""
		text, message = await self._process_message()

		if self.datasets:
				cache, table_ref, pid = self._retrieve_message_cache(
						message=message)
				if batch_size < len(cache):
						await self.record_messages(
								table_ref=table_ref, product_id=pid, messages=cache)
						
						post = (f"Sent batch of {batch_size} {table_ref} "
										f"for product_id: {pid} to BigQuery")
						self.message_cache[pid][table_ref].clear() # clear cache

		return ''.join(filter(None, [text, post]))#.replace("\n", "")

	async def _process_message(self):
		"""
		Private (workhorse) method to process the incoming message from the
		GDAX WebSocket API

		:rtype: str, dict
		:returns: text output of message status and the validated message

		""" 
		message = self._validate_message(await self._recieve_message())
		
		if message.get('type') is None:
				raise MessageHandlerError(f"Unknown message recieved: "
						f"{message.get('message')}, {message.get('reason')}")

		if message['type'] == "error":
				raise MessageHandlerError(
						f"{message.get('message')}, {message.get('reason')}")

		if message['type'] == "subscriptions":
				text = (f"Subscribed to {self.WSS_FEED} "
								f"{self._subscribe['channels']} WebSocket channels "
								f"for products: {self.product_ids}")
				return text, message

		if message['type'] == "snapshot":
				if self.datasets: 
						(self.message_cache[message['product_id']
								][self.TABLE_ORDERBOOK].append(message))

				text = (f"Received orderbook snapshot for "
								f"{message['product_id']} on {message['time']}")

				return text, message

		if message['type'] == "l2update":
				if self.datasets: 
						(self.message_cache[message['product_id']
								][self.TABLE_ORDERBOOK].append(message))

				return "", message

		if message['sequence'] % _ORDERBOOK_SNAPSHOT_ON == 0:
				message_orderbook = await self.get_product_order_book(
						product_id=message['product_id'])

				if self.datasets: 
						(self.message_cache[message['product_id']
								][self.TABLE_ORDERBOOK].append(message_orderbook.update(
										type='snapshot'))
						)
				
				text = (f"Received snapshot for {message['product_id']}"
								f"on {message['time']}") 
		else:
				text = ""

		if message['type'] in self.MESSAGE_TYPES:

				current_seq = self.sequences[message['product_id']] 
				if current_seq != 0 and message['sequence'] != current_seq:
						raise MessageHandlerError(
								f"Message {message['sequence']} is out-of-sequence")

				self.sequences[message['product_id']] = message['sequence']+1   
				if message['type'] == "match":
						if self.datasets: 
								(self.message_cache[message['product_id']
										][self.TABLE_TRADES].append(message))
						
						message_price = round(Decimal(message.get('price', 0)), 3)
						text = (f"{message['product_id']} traded @ {message_price}"
										f" on {message['time']} {text}")
				else:
						if self.datasets: 
								(self.message_cache[message['product_id']
										][self.TABLE_QUOTES].append(message))

				return text, message

		else: 
				raise MessageHandlerError(
								f"Unknown message recieved "
								f"{message.get('message')} { message.get('reason')}")

class MessageHandlerError(Exception):
	pass
