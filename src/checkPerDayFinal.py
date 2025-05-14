from data_acquisition.akshare_client import AKShareClient
from database.db_manager import DBManager
from database.models import StockDaily
from utils.logger import setup_logger,log
from datetime import datetime, timedelta,date
import pandas as pd
from database.tdengine_writer import TDEngineWriter
import time
import numpy as np
from decimal import Decimal, ROUND_HALF_UP
import akshare as ak

def checkData():
    setup_logger()
    
    akshare = AKShareClient()
    db_manager = DBManager()

    # 获取数据时间范围
    start_date = date(2025, 4, 10).strftime('%Y%m%d')
    end_date = date(2025, 4, 11).strftime('%Y%m%d')

    # 股票列表 (示例)
    # stock_list = [('002336','china.shenzhen')]
    # stock_list = db_manager.get_stock_id_list()
    stock_list = ['871694', '871553', '839273', '838971', '838171', '830779', '688582', '688510', '688486', '688368', '688307', '688200', '688195', '688123', '688100', '688093', '605499', '605368', '605218', '605077', '605028', '603949', '603866', '603809', '603697', '603682', '603677', '603223', '603199', '603162', '603031', '601988', '601886', '601882', '601658', '601328', '600976', '600965', '600711', '600605', '600502', '600211', '600113', '301389', '301362', '301280', '301277', '301187', '301127', '301113', '301069', '301066', '301016', '301004', '300999', '300997', '300969', '300960', '300926', '300918', '300896', '300842', '300824', '300801', '300783', '300782', '300750', '300693', '300562', '300476', '300458', '300441', '300406', '300403', '300401', '300373', '300304', '300146', '300102', '300059', '300047', '300018', '003022', '003006', '003000', '002972', '002967', '002920', '002911', '002879', '002876', '002870', '002852', '002807', '002774', '002749', '002692', '002661', '002658', '002600', '002516', '002469', '002467', '002455', '002404', '002393', '002320', '002318', '002315', '002282', '002236', '002233', '002185', '002145', '002054', '002014', '001376', '001368', '001358', '001299', '000989', '000895', '000823', '000538', '000063', '000062', '000048']
    # stock_list = ['300801', '300783', '300782', '300750', '300693', '300562', '300476', '300458', '300441', '300406', '300403', '300401', '300373', '300304', '300146', '300102', '300059', '300047', '300018', '003022', '003006', '003000', '002972', '002967', '002920', '002911', '002879', '002876', '002870', '002852', '002807', '002774', '002749', '002692', '002661', '002658', '002600', '002516', '002469', '002467', '002455', '002404', '002393', '002320', '002318', '002315', '002282', '002236', '002233', '002185', '002145', '002054', '002014', '001376', '001368', '001358', '001299', '000989', '000895', '000823', '000538', '000063', '000062', '000048']
    list = []
    for stock in stock_list:
        
        # 睡眠6秒钟
        time.sleep(6)
        stock_id = stock
        # akshare: daily_day,weekly_week,monthly_month,
        period_ak = 'weekly'
        period_local = 'week'
        log.info(f"正在处理股票: {stock},{period_local}")

        
        # 获取AKShare原始数据 symbol, 'daily', start_date, end_date, adjust
        raw_data = akshare.get_stock_data(
            symbol=stock_id.lower(),
            period = period_ak,
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"
        )
        
        # log.info(raw_data)


        if raw_data is not None:
            stock_per_day_db = db_manager.get_stock_per_day(stock_id,period_local,end_date)
            # log.info(stock_per_day_db)
            raw_open = raw_data.open
            db_open = stock_per_day_db.start_price
            # db_open = db_open.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
            float_num = np.float64(raw_open)
            decimal_num = Decimal(str(float_num))  # 先转字符串避免精度丢失
            # decimal_num = decimal_num.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
            # print(decimal_num == db_open)

            if (decimal_num != db_open):
                log.info(f"error is {stock_id} raw_open={decimal_num} db_open={db_open}")
                list.append(stock_id)
    
    log.info(list)
        

checkData()