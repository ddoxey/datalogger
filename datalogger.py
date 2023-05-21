#!/usr/bin/env python3
"""
    Datalogger stores data to sqlite database and
    can export to Excel file.
"""
import os
import re
import sys
import glob
import sqlite3
import xlsxwriter
from pprint import pprint
from functools import lru_cache
from collections import OrderedDict


class DataCollector:

    def __init__(self, name, functor, schema):
        if not re.match(r'^\w+$', name):
            raise Exception(f'Invalid collector name: {name}')
        if not hasattr(functor, '__call__'):
            raise Exception('Invalid collector function')
        for key in schema:
            if not re.match(r'^\w+$', key):
                raise Exception(f'Invalid collector field name: {key}')
        self.name = name
        self.functor = functor
        self.schema = OrderedDict(schema)


    def fields(self):
        return list(self.schema.keys())


    def run(self):
        data = self.functor()
        if isinstance(data, dict):
            fields = schema.keys()
            record = {field : data[field]
                      for field in fields
                      if field in data}
            if len(record):
                return record
        return None


class DataLogger:

    def __init__(self, name):
        self.db_name = name
        self.collectors = []
        self.conn = None
        self.db_filename = None
        self.inserts = 0


    def __del__(self):
        if self.db_filename is not None and self.inserts == 0:
            os.remove(self.db_filename)


    @staticmethod
    def next_db(name):
        return f'/tmp/{name}_{os.getpid()}.db'


    @lru_cache(maxsize=1)
    def last_db(self, name):
        files = [
            filename
            for filename in glob.glob(f'/tmp/{name}_*.db')
            if filename != self.db_filename]
        if len(files) == 0:
            return None
        files.sort(key = os.path.getmtime)
        return files[-1]


    def initialize(self, db = None):
        if db is None:
            self.db_filename = self.next_db(self.db_name)
        self.conn = sqlite3.connect(self.db_filename)


    def register(self, *args):
        self.initialize()
        for collector in args:
            if isinstance(collector, DataCollector):
                self.create_table(collector.name, collector.schema)
                self.collectors.append(collector)


    def export(self, output_fh):
        db = self.last_db(self.db_name)
        if db is None:
            print(f'No prior db: {self.db_name}', file=sys.stderr)
            return False
        workbook = xlsxwriter.Workbook(output_fh.name)
        bold = workbook.add_format({'bold': True})
        self.conn = sqlite3.connect(db)
        cursor = self.conn.cursor()
        for collector in self.collectors:
            row = 0
            fields = collector.fields()
            worksheet = workbook.add_worksheet(collector.name)
            for col, field in enumerate(fields):
                worksheet.write(row, col, field, bold)
            row += 1
            select_all = (f'SELECT * FROM {collector.name} WHERE 1')
            cursor.execute(select_all)
            all_data = cursor.fetchall()
            for record in all_data:
                for col, field in enumerate(fields):
                    worksheet.write(row, col, record[col])
                row += 1
        workbook.close()
        return True


    def create_table(self, name, schema):
        datatype_for = {
            str: 'TEXT',
            int: 'INT',
            float: 'REAL',
            bytes: 'BLOB',
        }
        create = f'CREATE TABLE IF NOT EXISTS {name}'
        fields = ['timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP']
        for field, pytype in schema.items():
            fields.append(f'{field} {datatype_for[pytype]}')
        sql = f'{create} ({", ".join(fields)})'
        cursor = self.conn.cursor()
        cursor.execute(f'{create} ({", ".join(fields)})')
        self.conn.commit()


    def insert(self, table, record):
        values = record.values()
        fields = ','.join(record.keys())
        places = ','.join('?' * len(values))
        sql = f'INSERT INTO {table} ({fields}) VALUES ({places});'
        cursor = self.conn.cursor()
        cursor.execute(sql, list(values))
        self.conn.commit()
        self.inserts += 1


    def update(self):
        for collector in self.collectors:
            print(f'collecting: {collector.name}')
            data = collector.run()
            if data is not None:
                for record in data:
                    self.insert(collector.name, record)
