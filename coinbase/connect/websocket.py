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

""" Establishes a connection to a Coinbase WebSocket API  """

import asyncio
import aiohttp
from aiohttp.http import WSMsgType
import ujson as json

_UNSUPPORTED_CHANNELS = ["ticker", "user", "matches", "heartbeat"]

class WebSocket(object):
	"""
	Connects to the COINBASE WebSocket API

	Args:
			product_ids (list):
					A list of available products as specified by COINBASE: 
					https://docs.prime.coinbase.com/#products

			subscribe_message (dict):
					(Optional) The subscription message to send the WebSocket channel: 
					https://docs.prime.coinbase.com/#subscribe

	Raises:
			WebSocketError:
					Raised if errors are detected outside of normal operation

	Parameters:
			WSS_FEED ("wss://ws-feed.prime.coinbase.com"):
					The websocket feed provides real-time market data updates 
					for orders and trades

			WSS_TIMEOUT (15):
					Send ping message every WSS_TIMEOUT seconds and wait pong 
					response, if pong response is not received then close connection. 
					The timer is reset on any data reception
					https://aiohttp.readthedocs.io/en/v3.0.1/client_reference.html

			MESSAGE_TYPES: 
					(["received", "open", "closed", "done", "match", "change", 
						"activate"])
					The supported COINBASE message types
					https://docs.prime.coinbase.com/#overview

	"""

	WSS_FEED = "wss://ws-feed.prime.coinbase.com"
	WSS_TIMEOUT = 15 
	MESSAGE_TYPES = ["received", "open", "closed", "done", 
			"match", "change", "activate"]
	
	def __init__(self, product_ids, subscribe_message=None, **kwargs):
		if not isinstance(product_ids, list): 
				product_ids = [product_ids]
		self.product_ids = product_ids

		self._subscribe = subscribe_message
		if subscribe_message is None:
				self._subscribe = {
						"type": "subscribe",
						"product_ids": self.product_ids,
						"channels": ["full", "level2"]
				}
		if self._subscribe["channels"] in _UNSUPPORTED_CHANNELS:
				raise NotImplementedError("ticker, user, matches, and heartbeat,"
				 "channels are not yet supported")
		super().__init__(product_ids=product_ids, **kwargs)


	async def __init_connection__(self): 
		"""
		Subscribes to COINBASE websocket feed

		:returns: None

		"""        
		self.session = aiohttp.ClientSession()
		self.connection = self.session.ws_connect(
				url=self.WSS_FEED, heartbeat=self.WSS_TIMEOUT)

		self.websocket = await self.connection.__aenter__()
		await self.websocket.send_json(self._subscribe)

	async def __aenter__(self):
		await self.__init_connection__()
		return self

	async def __aexit__(self, exc_type, exc, traceback):
		await self.session.__aexit__(exc_type, exc, traceback)

	async def _recieve_message(self):
		"""
		Receive a message from the websocket connection

		:returns: message as JSON

		"""
		message = await self.websocket.receive(
				timeout=self.WSS_TIMEOUT)
		if message.type == WSMsgType.CLOSED: 
				raise MessageHandlerError("Websocket was unexpectedly",
						"and prematurely closed")
		elif message.type != WSMsgType.TEXT:
				raise TypeError(
						f"Received message {message.type}: "
						f"{message.data} is not str")
		else:
				json_data = json.loads(message.data)
		return json_data

class WebSocketError(Exception): pass