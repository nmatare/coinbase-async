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

from google.cloud.bigquery import Client
from google.cloud.bigquery import Dataset
from google.cloud.bigquery import Table
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.dbapi import IntegrityError, ProgrammingError
from google.api_core.exceptions import NotFound

_TABLE_PARTITION_TIME = "DAY"
_DATASET_GEOLOCATION = "US"

_SCHEMA_TRADES = [
  SchemaField('time', 'TIMESTAMP', mode='REQUIRED'),
  SchemaField('sequence', 'INTEGER', mode='REQUIRED'),
  SchemaField('trade_id', 'INTEGER', mode='REQUIRED'),
  SchemaField('price', 'FLOAT', mode='REQUIRED'),
  SchemaField('size', 'FLOAT', mode='REQUIRED'),
  SchemaField('side', 'INTEGER', mode='REQUIRED'),
  SchemaField('maker_order_id', 'STRING', mode='REQUIRED'),
  SchemaField('taker_order_id', 'STRING', mode='REQUIRED'),
  SchemaField('type', 'STRING', mode='REQUIRED')
]

_SCHEMA_QUOTES = [
  SchemaField('time', 'TIMESTAMP', mode='NULLABLE'), # REQUIRED
  SchemaField('type', 'STRING', mode='NULLABLE'),
  SchemaField('sequence', 'INTEGER', mode='NULLABLE'),
  SchemaField('order_id', 'STRING', mode='NULLABLE'),
  SchemaField('side', 'INTEGER', mode='NULLABLE'),

  SchemaField('size', 'FLOAT', mode='NULLABLE'),
  SchemaField('price', 'FLOAT', mode='NULLABLE'),
  SchemaField('order_type', 'STRING', mode='NULLABLE'),
  SchemaField('remaining_size', 'FLOAT', mode='NULLABLE'),
  SchemaField('client_oid', 'STRING', mode='NULLABLE'),
  SchemaField('reason', 'STRING', mode='NULLABLE'),
  SchemaField('funds', 'FLOAT', mode='NULLABLE')
]

_SCHEMA_ORDERBOOK = [
  SchemaField('time', 'TIMESTAMP', mode='REQUIRED'),
  SchemaField('level', 'FLOAT', mode='REQUIRED'),
  SchemaField('depth', 'FLOAT', mode='REQUIRED'),
  SchemaField('side',  'INTEGER', mode='REQUIRED') 
]

class CreateBigQuery(object):
  """
  Creates and/or connects to Google's BigQuery API

  Args:
    product_ids (list):
      A list of available products as specified by Coinbase: 
      https://docs.coinbase.prime.com/#products

    service_file (str):
      The path/filename of a JSON service account keyfile

    brand_new (bool):
      (Optional) True/False: whether to create brand new 
      datasets or connect to existing datasets, respectively

  Raises:
    google.cloud.bigquery.dbapi.IntegrityError:
      Raised if datasets or schemas to do not match specified schemas

    google.cloud.bigquery.dbapi.ProgrammingError
      Raised if dataset cannot be found in project_id given 
      by the service_file 

  Parameters:
    TABLE_TRADES ("trades"):
      The name of the table which will hold trade data

    TABLE_QUOTES ("quotes"):
      The name of the table which will hold quote data

    TABLE_ORDERBOOK ("orderbook"):
      The name of the table which will hold orderbook (level, depth) data

  """

  TABLE_TRADES = "trades"
  TABLE_QUOTES = "quotes"
  TABLE_ORDERBOOK = "orderbook"

  def __init__(self, product_ids, service_file=None, brand_new=False, **kwargs):
    self.tables = {
      'trades' : self.TABLE_TRADES, 
      'quotes' : self.TABLE_QUOTES, 
      'orderbook' : self.TABLE_ORDERBOOK
    }

    self.schemas = {
      self.TABLE_TRADES : _SCHEMA_TRADES, 
      self.TABLE_QUOTES : _SCHEMA_QUOTES, 
      self.TABLE_ORDERBOOK : _SCHEMA_ORDERBOOK
    }

    if not isinstance(product_ids, list):
      product_ids = [product_ids]
    self.product_ids = [product_id.replace("-", 
      "_") for product_id in product_ids]

    self.datasets = {}
    for product_id in self.product_ids:
      self.datasets.update({product_id : dict(zip(('datasets', 'tables'), 
        (None, dict(zip(self.tables.values(), [None]*3)))))})

    if service_file is not None:
      self.client = Client.from_service_account_json(service_file)
      for product_id in self.product_ids:
        if brand_new:
          self._create_brand_new_datasets(product_id=product_id)
        else:
          self._connect_to_existing_datasets(product_id=product_id)
    super().__init__(**kwargs)


  def _create_brand_new_datasets(self, product_id):
    """
    Creates a new Google BigQuery dataset

    Args:
      product_id (str):
        A string representing the desired product_id 

    """   
    dataset = Dataset(self.client.dataset(dataset_id=product_id))
    dataset.location = _DATASET_GEOLOCATION
    
    self.datasets[product_id]['datasets'] = (
      self.client.create_dataset(dataset))
    self._create_brand_new_tables(product_id=product_id)
    return

  def _connect_to_existing_datasets(self, product_id):
    """
    Connects to an existing Google BigQuery dataset

    Args:
      product_id (str):
        A string representing the desired product_id 

    """
    if not product_id in self.datasets.keys():
      raise ProgrammingError(f'Could not find table for dataset: {product_id}')    
    else:
      self.datasets[product_id]['datasets'] = (
        self.client.dataset(dataset_id=product_id))
      self._connect_to_existing_tables(product_id=product_id)
    return

  def delete_datasets(self, product_ids):
    """
    Deletes specified Google BigQuery dataset(s)

    product_ids (list):
      A list of available products as specified by Coinbase: 
      https://docs.prime.coinbase.com/#products

    """
    if not isinstance(product_ids, list):
      product_ids = [product_ids]

    for product_id in product_ids:
        dataset = self.client.dataset(dataset_id=product_id)
        try:
            self.client.delete_dataset(dataset, delete_contents=True)
        except NotFound:
            pass
    return

  def _create_brand_new_tables(self, product_id):
    """
    Creates new tables in the specified BigQuery dataset

    Args:
      product_id (str):
        A string representing the desired product_id 

    """
    for table_name in self.tables:
      table_ref = self.datasets[product_id]['datasets'].table(table_name)
      table = Table(table_ref, schema=self.schemas[table_name])
      table.partitioning_type = _TABLE_PARTITION_TIME

      self.client.create_table(table) # API request
      self.datasets[product_id]['tables'][table_name] = (
        self.client.get_table(table_ref))
    return 

  def _connect_to_existing_tables(self, product_id):
    """
    Connects to existing tables in the BigQuery dataset

    Args:
      product_id (str):
        A string representing the desired product_id 

    """
    for table_name in self.tables:
      table_ref = self.datasets[product_id]['datasets'].table(table_name)
      table = self.datasets[product_id]['tables'][table_name] = (
        self.client.get_table(table_ref))
      
      if table.schema != self.schemas[table_name]:
        raise IntegrityError(
          f"The schema for dataset: {product_id} " 
          f"table: {table_name} is incorrect. " 
          f"Try re-creating the dataset and/or table")

      elif table.time_partitioning.type_ != 'DAY':
        raise IntegrityError(
          f"The dataset: {product_id} " 
          f"table: {table_name} is not time-partitioned. " 
          f"Try re-creating the dataset and/or table")     

      else:
        pass

    return

  def _create_brand_new_tables(self, product_id):
    """
    Creates new tables in the specified BigQuery dataset

    Args:
      product_id (str):
        A string representing the desired product_id 

    """
    for table_name in self.tables:
      table_ref = self.datasets[product_id]['datasets'].table(table_name)
      table = Table(table_ref, schema=self.schemas[table_name])
      table.partitioning_type = _TABLE_PARTITION_TIME

      self.client.create_table(table) # API request
      self.datasets[product_id]['tables'][table_name] = (
        self.client.get_table(table_ref))
    return

