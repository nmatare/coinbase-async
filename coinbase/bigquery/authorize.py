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

from urllib.parse import urlencode
from datetime import timedelta

import asyncio
import aiohttp

from google.auth import _helpers
from google.auth.jwt import encode
from google.auth._service_account_info import from_filename

_SCOPES = ('https://www.googleapis.com/auth/bigquery', 
					 'https://www.googleapis.com/auth/cloud-platform')

_URLENCODED_CONTENT_TYPE = 'application/x-www-form-urlencoded'
_JWT_GRANT_TYPE = 'urn:ietf:params:oauth:grant-type:jwt-bearer'
_REFRESH_GRANT_TYPE = 'refresh_token'
_TOKEN_EXPIRATION = 3600

class AsyncAuthGoogleCloud(object): 
	"""
	Token based __Asynchronous__ authentication to the Google Cloud 

	Args:
			service_file (str):
					The path/filename of a JSON service account keyfile

	Raises:
			AsyncAuthGoogleCloudError:
					Raised if cannot retrieve authentication token

	"""
	def __init__(self, service_file=None, **kwargs):
		if service_file is not None:
				self._credential = from_filename(service_file) 
				self._project_id = self._credential[0]['project_id']
				self._token_uri = self._credential[0]['token_uri']
				self._signer_email = self._credential[0]['client_email']
				self._private_key = self._credential[0]['private_key']
				self._signer = self._credential[1]
				self._scopes = _helpers.scopes_to_string(_SCOPES)
				self._token_expiration = _helpers.utcnow()
		super().__init__(service_file=service_file, **kwargs)


	def _make_jwt_for_audience(self):
		"""
		Make a JSON Web Token given a service file

		:rtype: string
		:returns: signed authentication     

		"""     
		now = _helpers.utcnow()
		lifetime = timedelta(seconds=_TOKEN_EXPIRATION)
		self._token_expiration = now + lifetime

		payload = {
				'aud': self._token_uri,
				'iss': self._signer_email,
				'iat': _helpers.datetime_to_secs(now),
				'exp': _helpers.datetime_to_secs(self._token_expiration),
				'scope' : self._scopes 
		}

		return encode(self._signer, payload) # from google.auth.jwt.encode

	async def _acquire_token(self):
		"""
		__Asynchronous__ aquisition of a temporary access token  

		"""     
		headers = {
				'content-type': _URLENCODED_CONTENT_TYPE
		}

		body = {
				'assertion' : self._make_jwt_for_audience(),
				'grant_type': _JWT_GRANT_TYPE
		}
		body = urlencode(body)

		async with aiohttp.ClientSession() as session:
				response = await session.post(
						url=self._token_uri, headers=headers, data=body)
				if response.status != 200:
						raise AsyncAuthGoogleCloudError("Unable to aquire token")
				self.token = await response.json()

class AsyncAuthGoogleCloudError(Exception):
	pass
