from data_acquisition.akshare_client import AKShareClient
from database.db_manager import DBManager
from database.models import StockDaily
from utils.logger import setup_logger,log
from datetime import datetime, timedelta,date
import pandas as pd
from database.tdengine_writer import TDEngineWriter
import time


def main():
    setup_logger()
    
    akshare = AKShareClient()
    db_manager = DBManager()
    
    # 获取数据时间范围
    start_date = date(2025, 4, 12).strftime('%Y%m%d')
    end_date = date(2025, 5, 15).strftime('%Y%m%d')

    # 股票列表 (示例)
    # stock_list = [('002336','china.shenzhen')]
    stock_list = db_manager.get_stock_id_list()
    
    
    for stock in stock_list:
        stock_id = stock.stock_id
        # akshare: daily_day,weekly_week,monthly_month,
        period_ak = 'daily'
        period_local = 'day'
        log.info(f"正在处理股票: {stock},{period_local}")

        
        # 获取AKShare原始数据 symbol, 'daily', start_date, end_date, adjust
        raw_data = akshare.get_stock_data(
            symbol=stock_id.lower(),
            period = period_ak,
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"
        )
        
        if raw_data is not None:
            """同步数据到 mysql"""
            db_ready_data = akshare.convert_to_db_format(raw_data,period_local)
            
            db_manager.save_daily_data(db_ready_data)

            """同步数据到TDEngine"""
            # 确保表存在
            table_name_td = f"{period_local}_{stock_id}"
            TDEngineWriter.create_dynamic_table("nb_stock",stock_id,stock.location,'day',table_name_td,"stock_trade_history")
            
            # 批量写入数据
            TDEngineWriter.write_daily_data_batch(
                data=db_ready_data,
                company_id=stock_id,
                table_name = table_name_td
            )
        # 睡眠6秒钟
        time.sleep(6)


if __name__ == "__main__":
    main()