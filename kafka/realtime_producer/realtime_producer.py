import logging
from logging.handlers import RotatingFileHandler
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os
from datetime import date, timedelta, datetime
import requests
import pandas
from pandas import json_normalize
import time



class RealtimeStockProducer:
    def __init__(self):
        log_handler = RotatingFileHandler(
            f"{os.path.abspath(os.getcwd())}/kafka/realtime_producer/logs/producer.log",
            maxBytes=104857600, backupCount=10)
        logging.basicConfig(
            format='%(asctime)s,%(msecs)d <%(name)s>[%(levelname)s]: %(message)s',
            datefmt='%H:%M:%S',
            level=logging.DEBUG,
            handlers=[log_handler])
        self.logger = logging.getLogger('realtimeProducer')

        self.producer = KafkaProducer(
            bootstrap_servers=['kafka-1:9092',
                               'kafka-2:9092', 'kafka-3:9092'],
            client_id='producer')

    def stock_intraday_data(self, symbol, page_num, page_size):
        retry_count = 0
        max_retries = 5
        retry_delay = 60  
        
        while retry_count < max_retries:
            try:
                d = datetime.now()
                if d.weekday() > 4:  # Nếu là cuối tuần
                    url = f'https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{symbol}/his/paging?page={page_num}&size={page_size}&headIndex=-1'
                else:  # Nếu là ngày trong tuần
                    url = f'https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{symbol}/his/paging?page={page_num}&size={page_size}'
                
                response = requests.get(url)
                
                # Kiểm tra mã lỗi HTTP
                if response.status_code == 429:  # Rate limit
                    self.logger.warning(f"API rate limit exceeded for {symbol}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_count += 1
                    continue
                
                if response.status_code != 200:
                    self.logger.error(f"Failed to fetch data for {symbol}. HTTP status code: {response.status_code}")
                    retry_count += 1
                    time.sleep(retry_delay)
                    continue

                data = response.json()
                df = json_normalize(data['data'])
                return df

            except Exception as e:
                self.logger.error(f"Error fetching data for {symbol}: {e}")
                retry_count += 1
                time.sleep(retry_delay)

        self.logger.error(f"Max retries reached for {symbol}. Skipping...")
        return None

    def message_handler(self, symbol, message):
        #  Message from stock api
        try:
            if not message.empty:
                stock_realtime_info = f"{symbol},{message.p.iloc[0]},{message.v.iloc[0]},{message.cp.iloc[0]},{message.rcp.iloc[0]},{message.ba.iloc[0]},{message.sa.iloc[0]},{message.hl.iloc[0]},{message.pcp.iloc[0]},{message.t.iloc[0]}"
                print(stock_realtime_info)
                self.producer.send('realtimeStockData', bytes(
                    stock_realtime_info, encoding='utf-8'))
                self.producer.flush()
        except KafkaError as e:
            self.logger.error(f"An Kafka error happened: {e}")
        except Exception as e:
            self.logger.error(
                f"An error happened while pushing message to Kafka: {e}")

    def crawl_from_binance(self, symbol_list):
        try:
            self.logger.info("Start running realtime stock producer...")
            for idx, symbol in enumerate(symbol_list):
                data = self.stock_intraday_data(symbol=symbol,
                                           page_num=0,
                                           page_size=1)
                self.message_handler(symbol, data)
            while True:
                pass
        except Exception as e:
            self.logger.error(f"An error happened while streaming: {e}")

    def run(self):
        with open(os.path.abspath(os.getcwd()) + "/kafka/producer/symbol_list.csv") as f:
            symbol_list = f.read().split('\n')
        self.crawl_from_binance(symbol_list)
