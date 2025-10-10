from datasource.akshare_client import AKShareClient
from database.db_manager import DBManager
from datasource.tushare_client import TushareService
from utils.logger import setup_logger,log
from datetime import date
from database.tdengine_writer import TDEngineWriter
import time
import dig_data
import database.tdengine_connector

import akshare as ak

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
    retired_list = ak.check_delisted_stocks(stock_id_list)
    print(retired_list)

# 新股上市
def updateNewStocks():
    setup_logger()
    ts = TushareService()
    db_manager = DBManager()
    stock_list = db_manager.get_stock_id_list()
    # 获取当前数据库中已有的A股股票id列表
    stock_id_list = [
        ts.convert_stock_id(stock.stock_id, stock.location) 
        for stock in stock_list 
        if stock.stock_id is not None and stock.location is not None
    ]
    # 从tushare获取当前上市的A股股票列表
    new_list = ts.check_new_stocks(stock_id_list)
    
    

if __name__ == "__main__":
    ts = TushareService()
    ts.update_basic_get_stock()
    # dig_data.dig_income_statment()
    # dig_data.dig_income_statment_yoy()
    dig_data.dig_balance_sheet()
    # todo copy balance_sheet
    dig_data.dig_cash_flow_statement()
    # 2. HK stock list , basic_info, finance_report,
    # 3. USA stock list, basic_info, finance_report
    # updateRetiredStocks()
    # updateNewStocks()
