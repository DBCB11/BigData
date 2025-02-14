import logging
import pandas as pd 
import requests
from logging.handlers import RotatingFileHandler
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os
from datetime import date, timedelta
import time
from pandas import json_normalize
import schedule

class StockProducer:
    def __init__(self):
        log_handler = RotatingFileHandler(
            f"{os.path.abspath(os.getcwd())}/kafka/producer/logs/producer.log",
            maxBytes=104857600, backupCount=10)
        logging.basicConfig(
            format='%(asctime)s,%(msecs)d <%(name)s>[%(levelname)s]: %(message)s',
            datefmt='%H:%M:%S',
            level=logging.DEBUG,
            handlers=[log_handler])
        self.logger = logging.getLogger('producer')

        self.producer = KafkaProducer(
            bootstrap_servers=['kafka-1:9092',
                               'kafka-2:9092', 'kafka-3:9092'],
            client_id='producer')

    def message_handler(self, symbol, message):
        #  Message from stock api
        try:
            if not message.empty: 
                stock_info = f"{symbol},{message.Open.iloc[0]},{message.High.iloc[0]},{message.Low.iloc[0]},{message.Close.iloc[0]},{message.Volume.iloc[0]},{message.Tradingdate.iloc[0]}"
                print(symbol + "," + stock_info)
                self.producer.send('stockData', bytes(
                    stock_info, encoding='utf-8'))
                self.producer.flush()
        except KafkaError as e:
            self.logger.error(f"An Kafka error happened: {e}")
        except Exception as e:
            self.logger.error(
                f"An error happened while pushing message to Kafka: {e}")
    def stock_historical_data(self, symbol, start_date, end_date):
        fd = int(time.mktime(time.strptime(start_date, "%Y-%m-%d")))
        td = int(time.mktime(time.strptime(end_date, "%Y-%m-%d")))
        response = requests.get('https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={}&type=stock&resolution=D&from={}&to={}'.format(symbol, fd, td))
        # Check HTTP response status
        if response.status_code == 429:  # rate limit
            print("API rate limit exceeded, waiting before retrying...")
            time.sleep(60) # wait 60s 
            return None
        
        if response.status_code != 200: 
            print(f"Request failed with status code {response.status_code}")
            return None
        
        try:
            data = response.json()
            
            if 'data' not in data:
                print("No data found for symbol:", symbol)
                return None

            df = json_normalize(data['data'])
            df['stockCode'] = symbol
            df.columns = df.columns.str.title()  
            return df

        except ValueError:
            print("Failed to decode JSON response")
            return None

    def crawl_from_tcbs(self, symbol_list):
        try:
            self.logger.info("Start running stock producer...")
            for idx, symbol in enumerate(symbol_list):
                start_date = (date.today() - timedelta(days=4)
                              ).strftime("%Y-%m-%d")
                end_date = (date.today()).strftime("%Y-%m-%d")
                
                data = self.stock_historical_data(symbol, start_date, end_date)
                self.message_handler(symbol, data)
            # while True:
            #     pass
        except Exception as e:
            self.logger.error(f"An error happened while streaming: {e}")

    def run(self):
        
        with open(os.path.abspath(os.getcwd()) + "/kafka/producer/symbol_list.csv") as f:
            symbol_list = f.read().split('\n')
        # schedule.every().day.at("15:10").do(self.crawl_from_tcbs, c)
        self.crawl_from_tcbs(symbol_list)
        while True:
            schedule.run_pending()