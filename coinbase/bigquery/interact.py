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
import aiohttp

import ujson as json
from uuid import uuid4
from google.auth import _helpers

from google.cloud.bigquery.table import _row_from_mapping
from google.cloud.bigquery.table import _TABLE_HAS_NO_SCHEMA
from google.cloud.bigquery.table import TableReference
from google.cloud.bigquery.table import Table
from google.cloud.bigquery._helpers import _SCALAR_VALUE_TO_JSON_ROW

from .authorize import AsyncAuthGoogleCloud
from .create import CreateBigQuery

_API_BASE = "https://www.googleapis.com/bigquery/v2"

class AsyncBigQuery(AsyncAuthGoogleCloud, CreateBigQuery):
  """
  Reads message traffic from the Coinbase exchange

  Args:
    product_ids (list):
      A list of available products as specified by Coinbase: 
      https://docs.prime.coinbase.com/#products

    service_file (str):
      The path/filename of a JSON service account keyfile

  Raises:
    AsyncBigQueryError:
      Raised if rows cannot be properly inserted into dataset.table

  """

  def __init__(self, product_ids, service_file, **kwargs):
    super().__init__(
        product_ids=product_ids, service_file=service_file, **kwargs)

  async def insert_rows(self, table, rows, selected_fields=None, **kwargs):
    """__Asynchronous__ insertion of rows into a table via the streaming API

    Credit:
    http://google-cloud-python.readthedocs.io/en/latest/_modules/google/cloud
    /bigquery/client.html#Client.insert_rows
    https://cloud.google.com/bigquery/docs/reference/rest/v2/
    tabledata/insertAll

    :type table: One of:
                 :class:`~google.cloud.bigquery.table.Table`
                 :class:`~google.cloud.bigquery.table.TableReference`
    :param table: the destination table for the row data, or a reference
                  to it.

    :type rows: One of:
                list of tuples
                list of dictionaries
    :param rows: Row data to be inserted. If a list of tuples is given,
                 each tuple should contain data for each schema field on
                 the current table and in the same order as the schema
                 fields.  If a list of dictionaries is given, the keys must
                 include all required fields in the schema.  Keys which do
                 not correspond to a field in the schema are ignored.

    :type selected_fields:
        list of :class:`~google.cloud.bigquery.schema.SchemaField`
    :param selected_fields:
        The fields to return. Required if ``table`` is a
        :class:`~google.cloud.bigquery.table.TableReference`.

    :type kwargs: dict
    :param kwargs:
        Keyword arguments to
        :meth:`~google.cloud.bigquery.client.Client.insert_rows_json`

    :rtype: list of mappings
    :returns: One mapping per row with insert errors:  the "index" key
              identifies the row, and the "errors" key contains a list
              of the mappings describing one or more problems with the
              row.
    :raises: ValueError if table's schema is not set
    """
    if selected_fields is not None:
        schema = selected_fields
    elif isinstance(table, TableReference):
        raise ValueError('need selected_fields with TableReference')
    elif isinstance(table, Table):
        if len(table.schema) == 0:
            raise ValueError(_TABLE_HAS_NO_SCHEMA)
        schema = table.schema
    else:
        raise TypeError('table should be Table or TableReference')

    json_rows = []
    for index, row in enumerate(rows):
        if isinstance(row, dict):
            row = _row_from_mapping(row, schema)
        json_row = {}

        for field, value in zip(schema, row):
            converter = _SCALAR_VALUE_TO_JSON_ROW.get(field.field_type)
            if converter is not None:  # STRING doesn't need converting
                value = converter(value)
            json_row[field.name] = value

        json_rows.append(json_row)

    return await self.insert_rows_json(table, json_rows, **kwargs)

  async def insert_rows_json(self, table, json_rows, row_ids=None,
                             skip_invalid_rows=None, ignore_unknown_values=None,
                             template_suffix=None, retry=None):
    """__Asynchronous__ insertion of rows into a table via the streaming API

    Credit:
    http://google-cloud-python.readthedocs.io/en/latest/_modules/google/cloud
    /bigquery/client.html#Client.insert_rows

    :type table: One of:
                 :class:`~google.cloud.bigquery.table.Table`
                 :class:`~google.cloud.bigquery.table.TableReference`
    :param table: the destination table for the row data, or a reference
                  to it.

    :type json_rows: list of dictionaries
    :param json_rows: Row data to be inserted. Keys must match the table
                      schema fields and values must be JSON-compatible
                      representations.

    :type row_ids: list of string
    :param row_ids: (Optional)  Unique ids, one per row being inserted.
                    If omitted, unique IDs are created.

    :type skip_invalid_rows: bool
    :param skip_invalid_rows: (Optional)  Insert all valid rows of a
                              request, even if invalid rows exist.
                              The default value is False, which causes
                              the entire request to fail if any invalid
                              rows exist.

    :type ignore_unknown_values: bool
    :param ignore_unknown_values: (Optional) Accept rows that contain
                                  values that do not match the schema.
                                  The unknown values are ignored. Default
                                  is False, which treats unknown values as
                                  errors.

    :type template_suffix: str
    :param template_suffix:
        (Optional) treat ``name`` as a template table and provide a suffix.
        BigQuery will create the table ``<name> + <template_suffix>`` based
        on the schema of the template table. See
        https://cloud.google.com/bigquery/streaming-data-into-bigquery
        #template-tables

    :type retry: :class:`google.api_core.retry.Retry`
    :param retry: (Optional) How to retry the RPC.

    :rtype: list of mappings
    :returns: One mapping per row with insert errors:  the "index" key
              identifies the row, and the "errors" key contains a list
              of the mappings describing one or more problems with the
              row.
    """
    rows_info = []
    data = {'rows': rows_info}

    for index, row in enumerate(json_rows):
        info = {'json': row}
        if row_ids is not None:
            info['insertId'] = row_ids[index]
        else:
            info['insertId'] = str(uuid4())
        rows_info.append(info)

    if skip_invalid_rows is not None:
        data['skipInvalidRows'] = skip_invalid_rows

    if ignore_unknown_values is not None:
        data['ignoreUnknownValues'] = ignore_unknown_values

    if template_suffix is not None:
        data['templateSuffix'] = template_suffix

    headers = {
        'content-type': 'application/json'
    }

    path = "{}{}/insertAll".format(_API_BASE, table.path)
    body = json.dumps(data).encode('utf-8')

    if _helpers.utcnow() > self._token_expiration:
        await self._acquire_token()

    headers['authorization'] = 'Bearer {}'.format(
        _helpers.from_bytes(self.token['access_token']))

    async with aiohttp.ClientSession() as session:
        response = await session.post(url=path, headers=headers, data=body)
        if response.status != 200:
            raise AsyncBigQueryError("Unable to insert row(s)")
        content = await response.json()

    errors = []
    for error in content.get('insertErrors', ()):
        errors.append({'index': int(
            error['index']), 'errors': error['errors']})

    return errors

class AsyncBigQueryError(Exception):
  pass
