from datasource.akshare_client import AKShareClient
from database.db_manager import DBManager
from datasource.tushare_client import TushareService
from utils.logger import setup_logger,log
from datetime import date
from datetime import datetime
from database.tdengine_writer import TDEngineWriter
from database.tdengine_tushare_writer import TDEngineTushareWriter
import time
from database.tdengine_reader import TDEngineReader as tdReader
import logging
from finance_report.finance_report_tushare import FinanceReportTushare


def dig_income_statment_tushare():
    setup_logger()
    tushare = TushareService()
    db_manager = DBManager()
    
    stock_list = db_manager.get_stock_id_list_all()
    for stock in stock_list:
        time.sleep(0.301)
        stock_id = stock.stock_id
        location = stock.location
        start_date = datetime.strptime('2010-01-01', '%Y-%m-%d')
        if stock.launch_date < start_date :
            start_date = stock.launch_date.strftime('%Y%m%d')
        else:
            start_date = start_date.strftime('%Y%m%d')
        end_date = datetime.strptime('2025-11-10', '%Y-%m-%d').strftime('%Y%m%d')
        print(stock_id)
        newStockId = TushareService.convert_stock_id(stock_id=stock_id,location=location)
        # 确保表存在 
        TDEngineWriter.create_dynamic_table("nb_stock",stock_id,location,'',f"is_tsh_{stock_id}","income_statement_tushare",True,"RMB")

        tushare_data = tushare.get_income_statement(stock_id=newStockId,start_time=start_date,end_time=end_date)
        TDEngineTushareWriter.insert_income_statement_tushare(tushare_data=tushare_data,stock_id=stock_id)
        
def dig_income_statment_yoy_tushare():
    setup_logger()
    db_manager = DBManager()
    
    stock_list = db_manager.get_stock_id_list_all()
    tdreader = tdReader()
    finance_reporter = FinanceReportTushare()
    
    for stock in stock_list:
        current_stock_id = stock.stock_id
        location = stock.location
        logging.info(f'{current_stock_id} - 开始计算利润表同比')
        try:
            is_reports = tdreader.get_finance_report_all(stock_id=current_stock_id, report_type="income_statement_tushare")
            
            if not is_reports or len(is_reports) < 2:
                logging.warning(f'{current_stock_id} - 利润表数据不足，跳过')
                continue
            
            # 按报告期排序
            is_reports_sorted = sorted(is_reports, key=lambda x: x.get('ts', ''),reverse=True)
            
            # 2、计算同比变化
            for i in range(0, len(is_reports_sorted)):
                current_report = is_reports_sorted[i]
                previous_report = finance_reporter.find_comparable_report(is_reports_sorted, current_report, i)
                
                if previous_report:
                    # 确保表存在
                    table_name_td = f"is_yoy_tsh_{current_stock_id}"
                    TDEngineWriter.create_dynamic_table("nb_stock", current_stock_id, location, '',  table_name_td, "income_statement_yoy_tushare", True, "RMB")
                    
                    # 3、计算同比并插入数据
                    finance_reporter.insert_income_statement_yoy_tushare(
                        stock_id=current_stock_id,
                        current_report=current_report,
                        previous_report=previous_report,
                        location=location
                    )
                    
            logging.info(f'{current_stock_id} - 利润表同比计算完成')
            
        except Exception as e:
            logging.error(f'{current_stock_id} - 计算利润表同比时出错: {e}')
            
def dig_balance_sheet_tushare():
    setup_logger()
    tushare = TushareService()
    db_manager = DBManager()
    
    stock_list = db_manager.get_stock_id_list_all()
    for stock in stock_list:
        time.sleep(0.301)
        stock_id = stock.stock_id
        location = stock.location
        start_date = datetime.strptime('2010-01-01', '%Y-%m-%d')
        if stock.launch_date < start_date :
            start_date = stock.launch_date.strftime('%Y%m%d')
        else:
            start_date = start_date.strftime('%Y%m%d')
        end_date = datetime.strptime('2025-11-10', '%Y-%m-%d').strftime('%Y%m%d')
        print(stock_id)
        newStockId = TushareService.convert_stock_id(stock_id=stock_id,location=location)
        # 确保表存在 
        TDEngineWriter.create_dynamic_table("nb_stock",stock_id,location,'',f"bs_tsh_{stock_id}","balance_sheet_tushare",True,"RMB")

        tushare_data = tushare.get_balanceSheet(stock_id=newStockId,start_time=start_date,end_time=end_date)
        TDEngineTushareWriter.insert_balance_sheet_tushare(tushare_data=tushare_data,stock_id=stock_id)
        
def dig_balance_sheet_yoy_tushare():
    setup_logger()
    db_manager = DBManager()
    
    stock_list = db_manager.get_stock_id_list_all()
    tdreader = tdReader()
    finance_reporter = FinanceReportTushare()
    
    for stock in stock_list:
        current_stock_id = stock.stock_id
        location = stock.location
        logging.info(f'{current_stock_id} - 开始计算资产负债表同比')
        try:
            is_reports = tdreader.get_finance_report_all(stock_id=current_stock_id, report_type="balance_sheet_tushare")
            
            if not is_reports or len(is_reports) < 2:
                logging.warning(f'{current_stock_id} - 资产负债表数据不足，跳过')
                continue
            
            # 按报告期排序
            is_reports_sorted = sorted(is_reports, key=lambda x: x.get('ts', ''),reverse=True)
            
            # 2、计算同比变化
            for i in range(0, len(is_reports_sorted)):
                current_report = is_reports_sorted[i]
                previous_report = finance_reporter.find_comparable_report(is_reports_sorted, current_report, i)
                
                if previous_report:
                    # 确保表存在
                    table_name_td = f"bs_yoy_tsh_{current_stock_id}"
                    TDEngineWriter.create_dynamic_table("nb_stock", current_stock_id, location, '',  table_name_td, "balance_sheet_yoy_tushare", True, "RMB")
                    
                    # 3、计算同比并插入数据
                    finance_reporter.insert_balance_sheet_yoy_tushare(
                        stock_id=current_stock_id,
                        current_report=current_report,
                        previous_report=previous_report,
                        location=location
                    )
                    
            logging.info(f'{current_stock_id} - 资产负债表同比计算完成')
            
        except Exception as e:
            logging.error(f'{current_stock_id} - 计算资产负债表同比时出错: {e}')
            

def dig_cash_flow_statement_tushare():
    setup_logger()
    tushare = TushareService()
    db_manager = DBManager()
    
    stock_list = db_manager.get_stock_id_list_all()
    for stock in stock_list:
        time.sleep(0.301)
        stock_id = stock.stock_id
        location = stock.location
        start_date = datetime.strptime('2010-01-01', '%Y-%m-%d')
        if stock.launch_date < start_date :
            start_date = stock.launch_date.strftime('%Y%m%d')
        else:
            start_date = start_date.strftime('%Y%m%d')
        end_date = datetime.strptime('2025-11-10', '%Y-%m-%d').strftime('%Y%m%d')
        print(stock_id)
        newStockId = TushareService.convert_stock_id(stock_id=stock_id,location=location)
        # 确保表存在 
        TDEngineWriter.create_dynamic_table("nb_stock",stock_id,location,'',f"cfs_tsh_{stock_id}","cash_flow_statement_tushare",True,"RMB")

        tushare_data = tushare.get_cashflowstatement(stock_id=newStockId,start_time=start_date,end_time=end_date)
        TDEngineTushareWriter.insert_cash_flow_statement_tushare(tushare_data=tushare_data,stock_id=stock_id)
        
def dig_cash_flow_statement_yoy_tushare():
    setup_logger()
    db_manager = DBManager()
    
    stock_list = db_manager.get_stock_id_list_all()
    tdreader = tdReader()
    finance_reporter = FinanceReportTushare()
    
    for stock in stock_list:
        current_stock_id = stock.stock_id
        location = stock.location
        logging.info(f'{current_stock_id} - 开始计算现金流表同比')
        try:
            is_reports = tdreader.get_finance_report_all(stock_id=current_stock_id, report_type="cash_flow_statement_tushare")
            
            if not is_reports or len(is_reports) < 2:
                logging.warning(f'{current_stock_id} - 现金流表数据不足，跳过')
                continue
            
            # 按报告期排序
            is_reports_sorted = sorted(is_reports, key=lambda x: x.get('ts', ''),reverse=True)
            
            # 2、计算同比变化
            for i in range(0, len(is_reports_sorted)):
                current_report = is_reports_sorted[i]
                previous_report = finance_reporter.find_comparable_report(is_reports_sorted, current_report, i)
                
                if previous_report:
                    # 确保表存在
                    table_name_td = f"cfs_yoy_tsh_{current_stock_id}"
                    TDEngineWriter.create_dynamic_table("nb_stock", current_stock_id, location, '',  table_name_td, "cash_flow_statement_yoy_tushare", True, "RMB")
                    
                    # 3、计算同比并插入数据
                    finance_reporter.insert_cash_flow_statement_yoy_tushare(
                        stock_id=current_stock_id,
                        current_report=current_report,
                        previous_report=previous_report,
                        location=location
                    )
                    
            logging.info(f'{current_stock_id} - 现金流表同比计算完成')
            
        except Exception as e:
            logging.error(f'{current_stock_id} - 计算现金流表同比时出错: {e}')