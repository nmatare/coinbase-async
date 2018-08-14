#!/usr/bin/python3
# -*- coding: utf-8 -*-
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
# "Authors: Daniel Paquin, Kornél Csernai

""" Request methods to interact with the Coinbase WebSocket API  """

import copy
from decimal import Decimal, InvalidOperation
import ujson as json

import time
import asyncio
import aiohttp
import async_timeout

import base64
import decimal
import hashlib
import hmac

class Request(object):
	"""
	Interact with the Coinbase REST API

	Args:
		api_key (str):
			(Optional) The api key as a string

		api_secret (str):
			(Optional) The secret randomly generated and provided by Coinbase

		passphrase (str):
			(Optional) The passphrase you specified when creating the API key

	Parameters:
		REST_EP ("https://api.prime.coinbase.com"):
			The REST API has endpoints for account and order management as 
			well as public market data
			https://docs.gdax.com/#api

		REST_TIMEOUT (15):
			Timeout in seconds for GET/POST/DELETE methods to REST_EP
			https://github.com/aio-libs/async-timeout

	Authors:
		This class is based upon the work done by Daniel Paquin and 
		Kornél Csernai. Kudos and thank you for their contribution. 
		https://github.com/csko/gdax-python-api
	"""

	REST_EP = "https://api.prime.coinbase.com"
	REST_TIMEOUT = 15 

	def __init__(self, api_key=None, api_secret=None, passphrase=None, **kwargs):
		if api_key is not None:
			self.authenticated = True
			self.api_key = api_key
			self.api_secret = api_secret
			self.passphrase = passphrase
		else:
			self.authenticated = False
		super().__init__(**kwargs)

	def _get_signature(self, path, method, body, timestamp, api_secret):
		"""Generate a signature for a request.

		Reference implementation at https://docs.gdax.com/#signing-a-message.

		"""
		message = timestamp + method + path + body
		message = message.encode('ascii')
		hmac_key = base64.b64decode(api_secret)
		assert len(hmac_key) == 64
		signature = hmac.new(hmac_key, message, hashlib.sha256)
		signature_b64 = base64.b64encode(signature.digest())
		return signature_b64.decode('ascii')

	def _auth_headers(self, path, method, body=''):
		timestamp = str(time.time())
		return {
			'Content-Type': 'application/json',
			'CB-ACCESS-SIGN': self._get_signature(
				path, method, body, timestamp, self.api_secret
			),
			'CB-ACCESS-TIMESTAMP': timestamp,
			'CB-ACCESS-KEY': self.api_key,
			'CB-ACCESS-PASSPHRASE': self.passphrase,
		}

	def _convert_return_fields(self, fields, decimal_fields, convert_all):
		if decimal_fields is None and not convert_all:
			return fields
		if isinstance(fields, list):
			return [self._convert_return_fields(field, decimal_fields,
												convert_all)
					for field in fields]
		elif isinstance(fields, dict):
			new_fields = {}
			for k, v in fields.items():
				if isinstance(v, dict):
					new_fields[k] = self._convert_return_fields(
						v, decimal_fields, convert_all)
				elif ((decimal_fields is not None and k in decimal_fields)
					  or convert_all):
					if isinstance(v, list):
						new_fields[k] = self._convert_return_fields(
							v, decimal_fields, convert_all)
					else:
						new_fields[k] = Decimal(v)
				else:
					new_fields[k] = v
			return new_fields
		else:
			if convert_all and not isinstance(fields, int):
				try:
					return Decimal(fields)
				except InvalidOperation:
					return fields
			else:
				return fields

	async def _get(self, path, params=None, decimal_return_fields=None,
				   convert_all=False, pagination=False):
		if params is None:
			params_copy = {}
		else:
			params_copy = copy.deepcopy(params)

		results = []
		while True:
			with async_timeout.timeout(self.REST_TIMEOUT):
				path_with_params = path
				if params_copy:
					path_with_params += '?'
					path_with_params += '&'.join(
						f'{k}={v}' for k, v in params_copy.items())

				if self.authenticated:
					headers = self._auth_headers(path_with_params,
												 method='GET')
				else:
					headers = None
				# Modified given recommended implementation:
				# "While we encourage ClientSession usage,
				# we also provide simple coroutines for making HTTP requests."
				async with aiohttp.ClientSession() as session:
					async with session.get(
						self.REST_EP + path_with_params,
						headers=headers
					) as response:
						res = await response.json()
						if pagination:
							results += res
							resp_headers = response.headers
							if "cb-after" in resp_headers:
								params_copy['after'] = resp_headers['cb-after']
							else:
								return self._convert_return_fields(
									results, decimal_return_fields, convert_all)
						else:
							return self._convert_return_fields(
								res, decimal_return_fields, convert_all)

	async def _post(self, path, data=None, decimal_return_fields=None,
					convert_all=False):
		json_data = json.dumps(data)
		headers = self._auth_headers(path, method='POST', body=json_data)
		path_url = self.REST_EP + path
		with async_timeout.timeout(self.REST_TIMEOUT):
			async with aiohttp.ClientSession() as session:
				async with session.post(path_url,
											 headers=headers,
											 data=json_data) as response:
					res = await response.json()
					return self._convert_return_fields(
						res, decimal_return_fields, convert_all)

	async def _delete(self, path, data={}, decimal_return_fields=None,
					  convert_all=False):
		json_data = json.dumps(data)
		headers = self._auth_headers(path, method='DELETE', body=json_data)
		path_url = self.REST_EP + path

		with async_timeout.timeout(self.REST_TIMEOUT):
			async with aiohttp.ClientSession() as session:
				async with session.delete(path_url,
											   headers=headers,
											   data=json_data) as response:
					return await response.json()

	async def get_products(self):
		return await self._get(
			'/products',
			decimal_return_fields={'base_min_size', 'base_max_size',
								   'quote_increment'})

	async def get_product_ticker(self, product_id=None):
		return await self._get(
			'/products/{}/ticker'.format(product_id),
			decimal_return_fields={'price', 'size', 'bid', 'ask', 'volume'})

	async def get_product_trades(self, product_id=None):
		return await self._get(
			'/products/{}/trades'.format(product_id),
			decimal_return_fields={'price', 'size'})

	async def get_product_order_book(self, product_id=None, level=3):
		params = {'level': level}
		return await self._get(
			'/products/{}/book'.format(product_id),
			params=params, decimal_return_fields={'bids', 'asks'},
			convert_all=True)

	async def get_product_historic_rates(self, product_id=None, start='',
										 end='', granularity=''):
		payload = {}
		payload["start"] = start
		payload["end"] = end
		payload["granularity"] = granularity
		res = await self._get(
			'/products/{}/candles'.format(product_id),
			params=payload)
		for row in res:
			for i, col in enumerate(row[1:]):
				row[i + 1] = Decimal(str(col))
		return res

	async def get_product_24hr_stats(self, product_id=None):
		return await self._get(
			'/products/{}/stats'.format(product_id),
			convert_all=True)

	async def get_currencies(self):
		return await self._get('/currencies',
							   decimal_return_fields={'min_size'})

	async def get_time(self):
		return await self._get('/time')

	async def get_account(self, account_id=''):
		assert self.authenticated
		return await self._get(f'/accounts/{account_id}')

	async def get_account_history(self, account_id):
		assert self.authenticated
		return await self._get(f'/accounts/{account_id}/ledger',
							   pagination=True)

	async def get_account_holds(self, account_id):
		assert self.authenticated
		return await self._get(f'/accounts/{account_id}/holds',
							   pagination=True)

	async def buy(self, product_id=None, price=None, size=None, funds=None,
				  **kwargs):
		assert self.authenticated
		payload = {}
		payload['side'] = 'buy'
		payload['product_id'] = product_id

		if price is not None:
			payload['price'] = str(price)
		if size is not None:
			payload['size'] = str(size)
		if funds is not None:
			payload['funds'] = str(funds)

		payload.update(kwargs)

		return await self._post(
			'/orders', data=payload,
			decimal_return_fields={'price', 'size', 'fill_fees', 'filled_size',
								   'executed_value'})

	async def sell(self, product_id=None, price=None, size=None, funds=None,
				   **kwargs):
		assert self.authenticated
		payload = {}
		payload['side'] = 'sell'
		payload['product_id'] = product_id

		if price is not None:
			payload['price'] = str(price)
		if size is not None:
			payload['size'] = str(size)
		if funds is not None:
			payload['funds'] = str(funds)

		payload.update(kwargs)

		return await self._post(
			'/orders', data=payload,
			decimal_return_fields={'price', 'size', 'fill_fees', 'filled_size',
								   'executed_value', 'funds',
								   'specified_funds'})

	async def cancel_order(self, order_id):
		assert self.authenticated
		return await self._delete(f'/orders/{order_id}')

	async def cancel_all(self, product_id=''):
		assert self.authenticated
		payload = {'product_id': product_id}
		return await self._delete('/orders/', data=payload)

	async def get_order(self, order_id):
		assert self.authenticated
		return await self._get(
			f'/orders/{order_id}',
			decimal_return_fields={'price', 'size', 'fill_fees', 'filled_size',
								   'executed_value', 'funds',
								   'specified_funds'})

	async def get_orders(self):
		assert self.authenticated
		return await self._get(
			'/orders', pagination=True,
			decimal_return_fields={'price', 'size', 'fill_fees', 'filled_size',
								   'executed_value', 'funds',
								   'specified_funds'})

	async def get_fills(self, order_id='', product_id=''):
		assert self.authenticated
		params = {}
		if order_id:
			params['order_id'] = order_id
		if product_id:
			params['product_id'] = product_id
		return await self._get(
			'/fills', params=params, pagination=True,
			decimal_return_fields={'price', 'size', 'fee'})

	async def get_fundings(self, status):
		assert self.authenticated
		params = {}
		if status:
			params['status'] = status
		return await self._get(
			'/funding', params=params, pagination=True,
			decimal_return_fields={'amount', 'repaid_amount',
								   'default_amount'})

	async def repay_funding(self, amount, currency):
		assert self.authenticated
		payload = {
			"amount": str(amount),
			"currency": currency,  # example: USD
		}
		return await self._post('/funding/repay', data=payload)

	async def margin_transfer(self, margin_profile_id, transfer_type,
							  currency, amount):
		assert self.authenticated
		payload = {
			"margin_profile_id": margin_profile_id,
			"type": transfer_type,
			"currency": currency,  # example: USD
			"amount": str(amount),
		}
		return await self._post('/profiles/margin-transfer', data=payload,
								decimal_return_fields={'amount'})

	async def get_position(self):
		assert self.authenticated
		return await self._get(
			'/position',
			decimal_return_fields={'max_funding_value', 'funding_value',
								   'amount', 'balance', 'hold',
								   'funded_amount', 'default_amount', 'price',
								   'sell', 'size', 'funds', 'complement',
								   'max_size'})

	async def close_position(self, repay_only=False):
		assert self.authenticated
		payload = {
			"repay_only": repay_only
		}
		return await self._post('/position/close', data=payload)

	async def deposit(self, amount, currency, payment_method_id):
		assert self.authenticated
		payload = {
			"amount": str(amount),
			"currency": currency,
			"payment_method_id": payment_method_id,
		}
		return await self._post('/deposits/payment-method', data=payload,
								decimal_return_fields={'amount'})

	async def coinbase_deposit(self, amount, currency, coinbase_account_id):
		assert self.authenticated
		payload = {
			"amount": str(amount),
			"currency": currency,
			"coinbase_account_id": coinbase_account_id,
		}
		return await self._post('/deposits/coinbase-account', data=payload,
								decimal_return_fields={'amount'})

	async def withdraw(self, amount, currency, payment_method_id):
		assert self.authenticated
		payload = {
			"amount": str(amount),
			"currency": currency,
			"payment_method_id": payment_method_id,
		}
		return await self._post('/withdrawals/payment-method', data=payload,
								decimal_return_fields={'amount'})

	async def coinbase_withdraw(self, amount, currency, coinbase_account_id):
		assert self.authenticated
		payload = {
			"amount": str(amount),
			"currency": currency,
			"coinbase_account_id": coinbase_account_id,
		}
		return await self._post('/withdrawals/coinbase', data=payload,
								decimal_return_fields={'amount'})

	async def crypto_withdraw(self, amount, currency, crypto_address):
		assert self.authenticated
		payload = {
			"amount": str(amount),
			"currency": currency,
			"crypto_address": crypto_address
		}
		return await self._post('/withdrawals/crypto', data=payload,
								decimal_return_fields={'amount'})

	async def get_payment_methods(self):
		assert self.authenticated
		return await self._get('/payment-methods')

	async def get_coinbase_accounts(self):
		assert self.authenticated
		return await self._get('/coinbase-accounts',
							   decimal_return_fields={'balance'})

	async def create_report(self, report_type, start_date, end_date,
							product_id=None, account_id=None,
							report_format=None, email=None):
		assert self.authenticated
		payload = {
			"type": report_type,
			"start_date": start_date,
			"end_date": end_date,
		}
		if report_type == 'fills':
			assert product_id is not None, \
				'product_id is required if report_type is fills'
		elif report_type == 'account':
			assert account_id is not None, \
				'account_id is required if report_type is account'
		else:
			assert False, \
				f'report_type must be one of fills or account, {report_type}' \
				' given'
		if product_id is not None:
			payload['product_id'] = product_id
		if account_id is not None:
			payload['account_id'] = account_id
		if report_format is not None:
			payload['format'] = report_format
		if email is not None:
			payload['email'] = email
		return await self._post('/reports', data=payload)

	async def get_report(self, report_id):
		assert self.authenticated
		return await self._get(f'/reports/{report_id}')

	async def get_trailing_volume(self):
		assert self.authenticated
		return await self._get('/users/self/trailing-volume',
							   decimal_return_fields={'exchange_volume',
													  'volume'})
