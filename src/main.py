from datasource.akshare_client import AKShareClient
from database.db_manager import DBManager
from datasource.tushare_client import TushareService
from utils.logger import setup_logger,log
from datetime import date
from database.tdengine_writer import TDEngineWriter
import time
import dig_data
import database.tdengine_connector
import dig_finance_report_tushare as dig_tsh
import dig_market_data as dig_mkt

import akshare as ak
from checkPerDayFinal import fixDataAfterFQ

def updateCycleValue():
    setup_logger()
    ak = AKShareClient()
    db_manager = DBManager()

    stock_list = db_manager.get_stock_id_list(is_new=0)
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
    ts_service = TushareService()
    db_manager = DBManager()

    stock_list = db_manager.get_stock_id_list_all()
    stock_id_list = [stock.stock_id for stock in stock_list if stock.stock_id is not None]
    retired_list = ts_service.check_delisted_stocks(stock_id_list)
    print(retired_list)
    db_manager.update_delisted_status(retired_list)

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
    
    # 每周交易数据
    # ts.update_basic_get_stock()
    # 新股
    dig_data.dig_new_stock_info()
    
    # ====== 财报 ======
    # 方式1：智能增量 —— 自动检测每只股票缺失的报告期并补采（推荐日常使用）
    # dig_tsh.dig_latest_finance_reports()

    # 方式2：全量更新 —— 遍历所有股票拉全部历史（首次或补数据用，约2.6小时）
    # dig_tsh.dig_income_statment_tushare()
    # dig_tsh.dig_income_statment_yoy_tushare()
    # dig_tsh.dig_balance_sheet_tushare()
    # dig_tsh.dig_balance_sheet_yoy_tushare()
    # dig_tsh.dig_cash_flow_statement_tushare()
    # dig_tsh.dig_cash_flow_statement_yoy_tushare()

    # ====== 完整性检测 ======
    # 检测所有股票三张报表是否齐全，输出到 CSV
    # dig_tsh.check_finance_report_completeness(output_csv='doc/finance_report_check.csv')

    # ====== 多周期行情数据 ======
    # 全量采集: day / month / quarter / year / 60min
    # dig_mkt.dig_market_data()
    # 按需采集单个周期:
    # dig_mkt.dig_market_data(scopes=['day'])
    # dig_mkt.dig_market_data(scopes=['60min'])
    # dig_mkt.dig_market_data(scopes=['month'])
    # dig_mkt.dig_market_data(scopes=['quarter', 'year'])
    # dig_mkt.dig_market_data(scopes=[ 'year'])
    # 2. HK stock list , basic_info, finance_report,
    # 3. USA stock list, basic_info, finance_report
    # 退市
    # updateRetiredStocks()
    
    # 赋权后更新股价
    # fixDataAfterFQ()
