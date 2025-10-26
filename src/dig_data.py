from datasource.akshare_client import AKShareClient
from database.db_manager import DBManager
from datasource.tushare_client import TushareService
from utils.logger import setup_logger,log
from datetime import date
from database.tdengine_writer import TDEngineWriter
import time
from database.tdengine_reader import TDEngineReader as tdReader
import logging
import akshare as ak

logger = logging.getLogger(__name__)
# 获取所有的数据，修改start 来调整获取的数据启始时间
def dig_data():
    setup_logger()
    
    akshare = AKShareClient()
    db_manager = DBManager()

    # 获取数据时间范围
    # 620的日，621的日，月，季度
    start = date(2025, 9, 25).strftime('%Y%m%d')
    end_date = date(2025, 9, 26).strftime('%Y%m%d')
    
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
                TDEngineWriter.create_dynamic_table("nb_stock",stock_id,stock.location,period_local,table_name_td,"stock_trade_history",False)
                
                # 批量写入数据
                TDEngineWriter.write_daily_data_batch(
                    data=db_ready_data,
                    company_id=stock_id,
                    table_name = table_name_td
                )

def dig_data_tushare():
    setup_logger()
    akshare = AKShareClient()
    tushare = TushareService()
    db_manager = DBManager()

    start_date = date(2025, 9, 29).strftime('%Y%m%d')
    end_date = date(2025, 9, 30).strftime('%Y%m%d')
    
    stock_list = db_manager.get_stock_id_list()
    print(stock_list)
    
    # tushare: daily_day,weekly_week,monthly_month
    input_str = "weekly_week"
    items = input_str.split(',')

    for item in items:
        
        parts = item.split('_')

        for stock in stock_list:
            
            time.sleep(2)
            stock_id = stock.stock_id
            location = stock.location
            # basic = akshare.get_stock_basic(stock_id)
            # db_manager.update_basic_info(basic)

            time.sleep(1)
            # weekly
            period_tu = parts[0]
            # week
            period_local = parts[1]
            
            log.info(f"正在处理股票: {stock},{period_local},{period_tu}")

            newStockId = TushareService.convert_stock_id(stock_id=stock_id,location=location)

            raw_data = tushare.get_adj_stock_data(
                symbol=newStockId,
                period = period_tu,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            
            if raw_data is not None:
                """同步数据到 mysql"""
                db_ready_data = db_manager.convert_to_db_format_tushare(stock_id,raw_data,period_local)
                
                db_manager.save_daily_data(db_ready_data)

                """同步数据到TDEngine"""
                # 确保表存在
                table_name_td = f"{period_local}_{stock_id}"
                TDEngineWriter.create_dynamic_table("nb_stock",stock_id,stock.location,period_local,table_name_td,"stock_trade_history",False)
                
                # 批量写入数据
                TDEngineWriter.write_daily_data_batch(
                    data=db_ready_data,
                    company_id=stock_id,
                    table_name = table_name_td
                )

def dig_new_per_data():
    setup_logger()
    akshare = AKShareClient()
    tushare = TushareService()
    db_manager = DBManager()

    start_date = date(2025, 9, 25).strftime('%Y%m%d')
    end_date = date(2025, 9, 26).strftime('%Y%m%d')
    
    stock_list = db_manager.get_stock_id_list_point()
    print(stock_list)
    
    # tushare: daily_day,weekly_week,monthly_month
    input_str = "weekly_week"
    items = input_str.split(',')

    for item in items:
        
        parts = item.split('_')

        for stock in stock_list:
            
            time.sleep(2)
            stock_id = stock.stock_id
            location = stock.location
            basic = akshare.get_stock_basic(stock_id)
            db_manager.update_basic_info(basic)

            time.sleep(1)
            # weekly
            period_tu = parts[0]
            # week
            period_local = parts[1]
            
            log.info(f"正在处理股票: {stock},{period_local},{period_tu}")

            newStockId = TushareService.convert_stock_id(stock_id=stock_id,location=location)

            raw_data = tushare.get_adj_stock_data(
                symbol=newStockId,
                period = period_tu,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            
            if raw_data is not None:
                """同步数据到 mysql"""
                db_ready_data = db_manager.convert_to_db_format_tushare(stock_id,raw_data,period_local)
                
                db_manager.save_daily_data(db_ready_data)

                """同步数据到TDEngine"""
                # 确保表存在
                table_name_td = f"{period_local}_{stock_id}"
                TDEngineWriter.create_dynamic_table("nb_stock",stock_id,stock.location,period_local,table_name_td,"stock_trade_history",False)
                
                # 批量写入数据
                TDEngineWriter.write_daily_data_batch(
                    data=db_ready_data,
                    company_id=stock_id,
                    table_name = table_name_td
                )
                

def dig_income_statment(start_date,end_date):
    setup_logger()
    tushare = TushareService()
    db_manager = DBManager()
    
    stock_list = db_manager.get_stock_id_list(is_new=1)
    for stock in stock_list:
        time.sleep(0.301)
        stock_id = stock.stock_id
        print(stock_id)
        location = stock.location
        newStockId = TushareService.convert_stock_id(stock_id=stock_id,location=location)
        # 确保表存在 
        table_name_td = f"is_{stock_id}"
        TDEngineWriter.create_dynamic_table("nb_stock",stock_id,stock.location,'',table_name_td,"income_statement",True)

        tushare_data = tushare.get_income_statement(stock_id=newStockId,start_time=start_date,end_time=end_date)
        TDEngineWriter.insert_income_statement(tushare_data=tushare_data,stock_id=stock_id)
        

def dig_income_statment_yoy():
    setup_logger()
    db_manager = DBManager()
    report_date = date(2025, 6, 30).strftime('%Y%m%d')
    stock_list = db_manager.get_stock_id_list()
    tdreader = tdReader()
    
    for stock in stock_list:
        stock_id = stock.stock_id
        print(stock_id+' get income statement yoy')
        
        # 确保表存在 
        table_name_td = f"is_{stock_id}"
        TDEngineWriter.create_dynamic_table("nb_stock", stock_id, stock.location,'', table_name_td, "income_statement_yoy", True)
        # todo 获取 当季度的利润表数据，并做计算
        xueqiu_current_data = tdreader.get_finance_report(stock_id=stock_id,report_date=report_date,report_type="income_statement")
        if xueqiu_current_data:
            TDEngineWriter.insert_income_statement_yoy(stock_id=stock_id,xueqiu_mapped_data=xueqiu_current_data)

# 从tushare 获取资产负债表的数据，遍历每个股票，并捞取存入tdengine,
def dig_balance_sheet(start_date,end_date):
    setup_logger()
    tushare = TushareService()
    db_manager = DBManager()
    # start_date = date(2025, 6, 1).strftime('%Y%m%d')
    # end_date = date(2025, 10, 2).strftime('%Y%m%d')
    stock_list = db_manager.get_stock_id_list()
    

    for stock in stock_list:
        time.sleep(0.301)
        stock_id = stock.stock_id
        print(stock_id)
        location = stock.location
        newStockId = TushareService.convert_stock_id(stock_id=stock_id,location=location)
        # 确保表存在 
        table_name_td = f"bs_{stock_id}"
        TDEngineWriter.create_dynamic_table("nb_stock",stock_id,stock.location,'',table_name_td,"balance_sheets",True)

        tushare_data = tushare.get_balanceSheet(stock_id = newStockId,start_time=start_date,end_time=end_date)
        TDEngineWriter.insert_balance_sheet(tushare_data=tushare_data,stock_id=stock_id,location= location)

# 从tushare 获取现金流表的数据，遍历每个股票，并捞取存入tdengine,
def dig_cash_flow_statement(start_date,end_date):
    setup_logger()
    tushare = TushareService()
    db_manager = DBManager()
    # start_date = date(2025, 6, 1).strftime('%Y%m%d')
    # end_date = date(2025, 10, 2).strftime('%Y%m%d')
    stock_list = db_manager.get_stock_id_list()
    

    for stock in stock_list:
        time.sleep(0.301)
        stock_id = stock.stock_id
        print(stock_id)
        location = stock.location
        newStockId = TushareService.convert_stock_id(stock_id=stock_id,location=location)
        # 确保表存在
        table_name_td = f"cfs_{stock_id}"
        TDEngineWriter.create_dynamic_table("nb_stock",stock_id,stock.location,'',table_name_td,"cash_flow_statements",True)

        tushare_data = tushare.get_cashflowstatement(stock_id = newStockId,start_time=start_date,end_time=end_date)
        TDEngineWriter.insert_cash_flow_statement(tushare_data=tushare_data,stock_id=stock_id)
        
# 新股 1、获取从 上市日到当前日的 周级交易历史插入 per_day_fianl
# 2、财报全量获取插入 tdengine
# 支持港股、美股、A股
def digNewStock(self):
    try:
        setup_logger()
        # 1.准备
        tushare = TushareService()
        db_manager = DBManager()
        
        end_date = date(2025, 10, 25).strftime('%Y%m%d')
        stock_list = db_manager.get_new_stock_id_list()
        print(stock_list)
        # tushare: daily_day,weekly_week,monthly_month
        input_str = "weekly_week"
        items = input_str.split(',')
        for item in items:
            parts = item.split('_')
            for stock in stock_list:
                stock_id = stock.stock_id
                location = stock.location
                start_date =  stock.launch_date.strftime('%Y%m%d')
                newStockId = TushareService.convert_stock_id(stock_id=stock_id,location=location)
                # weekly
                period_tu = parts[0]
                # week
                period_local = parts[1]
                log.info(f"正在处理股票: {stock},{period_local},{period_tu}")
                raw_data = tushare.get_adj_stock_data(
                    symbol= newStockId,
                    period = period_tu,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"
                )
                
                # todo  循环遍历
                if raw_data is not None:
                    """同步数据到 mysql"""
                    db_ready_data = db_manager.convert_to_db_format_tushare(stock_id,raw_data,period_local)
                    
                    db_manager.save_daily_data(db_ready_data)

                    """同步数据到TDEngine"""
                    # 确保表存在
                    table_name_td = f"{period_local}_{stock_id}"
                    TDEngineWriter.create_dynamic_table("nb_stock",stock_id,stock.location,period_local,table_name_td,"stock_trade_history",False)
                    
                    # 批量写入数据
                    TDEngineWriter.write_daily_data_batch(
                        data=db_ready_data,
                        company_id=stock_id,
                        table_name = table_name_td
                    )

                # 财报数据 获取并写入 tdengine
                
                dig_income_statment(start_date=start_date,end_date=end_date)
                dig_balance_sheet(start_date=start_date,end_date=end_date)
                
        
    except Exception as e:
        logger.error(f"检查新增股票失败: {e}")
        return []