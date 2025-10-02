from data_acquisition.akshare_client import AKShareClient
from database.db_manager import DBManager
from data_acquisition.tushare_client import TushareService
from utils.logger import setup_logger,log
from datetime import date
from database.tdengine_writer import TDEngineWriter
import time
import dig_data

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
    
def updateNewStocks():
    setup_logger()
    ak = AKShareClient()
    db_manager = DBManager()
    stock_list = db_manager.get_stock_id_list()
    stock_id_list = [stock.stock_id for stock in stock_list if stock.stock_id is not None]
    
    new_list = ak.check_new_stocks(stock_id_list)
    for stock in new_list:
        stock_ob = {'stock_id':stock}
        db_manager.update_basic_info(stock_ob)
    

if __name__ == "__main__":
    dig_data.dig_income_statment()
    # updateRetiredStocks()
    # updateNewStocks()
