from data_acquisition.akshare_client import AKShareClient
from database.db_manager import DBManager
from database.models import StockDaily
from utils.logger import setup_logger,log
from datetime import datetime, timedelta,date
import pandas as pd
from database.tdengine_writer import TDEngineWriter
import time

import akshare as ak

# 获取所有的数据，修改start 来调整获取的数据启始时间
def dig_data():
    setup_logger()
    
    akshare = AKShareClient()
    db_manager = DBManager()

    # 获取数据时间范围
    # 620的日，621的日，月，季度
    start = date(2025, 9, 4).strftime('%Y%m%d')
    end_date = date(2025, 9, 5).strftime('%Y%m%d')
    
    # 股票列表 (示例)
    # stock_list = [('002336','china.shenzhen')]
    stock_list = db_manager.get_stock_id_list()
    
    # akshare: daily_day,weekly_week,monthly_month
    input_str = "weekly_week"
    items = input_str.split(',')

    for item in items:
        
        parts = item.split('_')

        for stock in stock_list:
            
            time.sleep(2)
            stock_id = stock.stock_id
            basic = akshare.get_stock_basic(stock_id)
            db_manager.update_basic_info(basic)

            # 睡眠6秒钟
            time.sleep(2)
            period_ak = parts[0]
            period_local = parts[1]
            
            
            log.info(f"正在处理股票: {stock},{period_local},{period_ak}")

            # basic_info = db_manager.get_stock_basic_info(stock_id)
            # 获取AKShare原始数据 symbol, 'daily', start_date, end_date, adjust
            # start = basic_info.launch_date.strftime('%Y%m%d')

            # 获取AKShare原始数据 symbol, 'daily', start_date, end_date, adjust
            raw_data = akshare.get_stock_data(
                symbol=stock_id.lower(),
                period = period_ak,
                start_date=start,
                end_date=end_date,
                adjust="qfq"
            )
            
            # log.info(raw_data)

            if raw_data is not None:
                """同步数据到 mysql"""
                db_ready_data = akshare.convert_to_db_format(raw_data,period_local)
                
                db_manager.save_daily_data(db_ready_data)

                """同步数据到TDEngine"""
                # 确保表存在
                table_name_td = f"{period_local}_{stock_id}"
                TDEngineWriter.create_dynamic_table("nb_stock",stock_id,stock.location,period_local,table_name_td,"stock_trade_history")
                
                # 批量写入数据
                TDEngineWriter.write_daily_data_batch(
                    data=db_ready_data,
                    company_id=stock_id,
                    table_name = table_name_td
                )
        # log.info('一期数据获取结束开始休息！')
        # log.info("休息300秒")
        # time.sleep(300)

def updateCycleValue():
    setup_logger()
    ak = AKShareClient()
    db_manager = DBManager()


    stock_list = db_manager.get_stock_id_list()
    for stock in stock_list:
        time.sleep(3)
        stock_id = stock.stock_id
        basic = ak.get_stock_basic(stock_id)
        db_manager.update_basic_info(basic)

def updatePosition():
    setup_logger()
    ak = AKShareClient()
    db_manager = DBManager()
    # 获取持仓

    # 获取最新价格
    df = ak.get_realtime_data('000001')
    print(df)

def getHoldSituation():
    setup_logger()
    ak = AKShareClient()
    stock_ggcg_em_df = ak.getHold("600986")
    print(stock_ggcg_em_df)

# 更新已经退市的股票信息为 1
def updateRetiredStocks():
    setup_logger()
    ak = AKShareClient()
    db_manager = DBManager()

    stock_list = db_manager.get_stock_id_list()
    stock_id_list = [stock.stock_id for stock in stock_list if stock.stock_id is not None]
    print(stock_id_list)
    ak = AKShareClient()
    #stockL = ['000023']
    retired_list = ak.check_new_stocks(stock_id_list)
    #db_manager.update_basic_info
    print(retired_list)
    
    

if __name__ == "__main__":
    dig_data()
    # updateRetiredStocks()
