from datasource.akshare_client import AKShareClient
from database.db_manager import DBManager
from datasource.tushare_client import TushareService
from utils.logger import setup_logger,log
from datetime import date
from datetime import datetime
from database.tdengine_writer import TDEngineWriter
from database.tdengine_tushare_writer import TDEngineTushareWriter
import time
import pandas as pd
from database.tdengine_reader import TDEngineReader as tdReader
from database.tdengine_connector import tdengine
import logging
from finance_report.finance_report_tushare import FinanceReportTushare
from finance_report.finance_report_constant import FinanceReportConstant


def dig_income_statment_tushare(stock_list=None):
    setup_logger()
    tushare = TushareService()
    db_manager = DBManager()
    
    if stock_list is None:
        stock_list = db_manager.get_stock_id_list_all()
    fin = FinanceReportConstant()
    for stock in stock_list:
        time.sleep(0.301)
        stock_id = stock.stock_id
        location = stock.location
        start_date = datetime.strptime('2010-01-01', '%Y-%m-%d')
        if stock.launch_date < start_date :
            start_date = stock.launch_date.strftime('%Y%m%d')
        else:
            start_date = start_date.strftime('%Y%m%d')
        end_date = date.today().strftime('%Y%m%d')
        print(stock_id)
        newStockId = TushareService.convert_stock_id(stock_id=stock_id,location=location)
        # 确保表存在 
        TDEngineWriter.create_dynamic_table("nb_stock",stock_id,location,'',f"is_tsh_{stock_id}","income_statement_tushare",True,"RMB")

        tushare_data = tushare.get_income_statement(stock_id=newStockId,start_time=start_date,end_time=end_date)
        TDEngineTushareWriter.insert_tushare(tushare_data=tushare_data,stock_id=stock_id,numeric_fields=fin.is_numeric_fields,type='is')
        
def dig_income_statment_yoy_tushare(stock_list=None):
    setup_logger()
    db_manager = DBManager()
    
    if stock_list is None:
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
            
def dig_balance_sheet_tushare(stock_list=None):
    setup_logger()
    tushare = TushareService()
    db_manager = DBManager()
    
    if stock_list is None:
        stock_list = db_manager.get_stock_id_list_all()
    fin = FinanceReportConstant()
    for stock in stock_list:
        time.sleep(0.301)
        stock_id = stock.stock_id
        location = stock.location
        start_date = datetime.strptime('2010-01-01', '%Y-%m-%d')
        if stock.launch_date < start_date :
            start_date = stock.launch_date.strftime('%Y%m%d')
        else:
            start_date = start_date.strftime('%Y%m%d')
        end_date = date.today().strftime('%Y%m%d')
        print(stock_id)
        newStockId = TushareService.convert_stock_id(stock_id=stock_id,location=location)
        # 确保表存在 
        TDEngineWriter.create_dynamic_table("nb_stock",stock_id,location,'',f"bs_tsh_{stock_id}","balance_sheet_tushare",True,"RMB")

        tushare_data = tushare.get_balanceSheet(stock_id=newStockId,start_time=start_date,end_time=end_date)
        print(tushare_data)
        TDEngineTushareWriter.insert_tushare(tushare_data=tushare_data,stock_id=stock_id, numeric_fields=fin.bs_numeric_fields,type='bs' )
        
def dig_balance_sheet_yoy_tushare(stock_list=None):
    setup_logger()
    db_manager = DBManager()
    
    if stock_list is None:
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
            

def dig_cash_flow_statement_tushare(stock_list=None):
    setup_logger()
    tushare = TushareService()
    db_manager = DBManager()
    
    if stock_list is None:
        stock_list = db_manager.get_stock_id_list_all()
    fin = FinanceReportConstant()
    for stock in stock_list:
        time.sleep(0.301)
        stock_id = stock.stock_id
        location = stock.location
        start_date = datetime.strptime('2010-01-01', '%Y-%m-%d')
        if stock.launch_date < start_date :
            start_date = stock.launch_date.strftime('%Y%m%d')
        else:
            start_date = start_date.strftime('%Y%m%d')
        end_date = date.today().strftime('%Y%m%d')
        print(stock_id)
        newStockId = TushareService.convert_stock_id(stock_id=stock_id,location=location)
        # 确保表存在 
        TDEngineWriter.create_dynamic_table("nb_stock",stock_id,location,'',f"cfs_tsh_{stock_id}","cash_flow_statement_tushare",True,"RMB")

        tushare_data = tushare.get_cashflowstatement(stock_id=newStockId,start_time=start_date,end_time=end_date,)
        TDEngineTushareWriter.insert_tushare(tushare_data=tushare_data,stock_id=stock_id,numeric_fields=fin.cfs_numeric_fields,type='cfs')
        
def _get_latest_period():
    """
    根据当前日期推算最新已披露的财报报告期（YYYYMMDD）。
    A股财报披露截止日：Q1→4/30, 半年报→8/31, Q3→10/31, 年报→次年4/30。
    返回截止日已过的最新报告期。
    """
    today = date.today()
    year = today.year
    deadlines = [
        (date(year, 4, 30), f'{year}0331'),
        (date(year, 8, 31), f'{year}0630'),
        (date(year, 10, 31), f'{year}0930'),
        (date(year + 1, 4, 30), f'{year}1231'),
    ]
    latest = f'{year - 1}1231'
    for deadline, period in deadlines:
        if today > deadline:
            latest = period
        else:
            break
    return latest


def _get_all_expected_periods(launch_date_str, end_period):
    """生成从上市日期到 end_period 的所有预期报告期"""
    try:
        if isinstance(launch_date_str, datetime):
            launch = launch_date_str
        else:
            launch = datetime.strptime(str(launch_date_str)[:10], '%Y-%m-%d')
    except Exception:
        launch = datetime(2010, 1, 1)

    end_dt = datetime.strptime(end_period, '%Y%m%d')
    periods = []
    year = launch.year
    while True:
        for q_end in ['0331', '0630', '0930', '1231']:
            p = f'{year}{q_end}'
            p_dt = datetime.strptime(p, '%Y%m%d')
            if p_dt < launch:
                continue
            if p_dt > end_dt:
                return periods
            periods.append(p)
        year += 1
    return periods


def _get_latest_end_date_from_td(stock_id, table_prefix):
    """从 TDEngine 查询某只股票某张报表已有的最新 end_date（返回 YYYYMMDD 字符串或 None）"""
    table_name = f"{table_prefix}_{stock_id}"
    try:
        result = tdengine.conn.query(f"SELECT end_date FROM {table_name} ORDER BY ts DESC LIMIT 1")
        if result:
            for row in result:
                ed = row[0]
                if ed:
                    return str(ed).replace('-', '')[:8]
    except Exception:
        pass
    return None


def _next_period(period_str):
    """给定一个报告期（如 20250930），返回下一个报告期（20251231）"""
    q_map = {'0331': '0630', '0630': '0930', '0930': '1231'}
    suffix = period_str[4:]
    year = int(period_str[:4])
    if suffix in q_map:
        return f'{year}{q_map[suffix]}'
    else:
        return f'{year + 1}0331'


def dig_latest_finance_reports(target_period=None):
    """
    智能增量更新：对每只股票检查 TDEngine 中已有的最新报告期，
    自动补采从该期之后到当前最新可获取报告期之间的所有缺失财报。
    例如：已有 20250930 → 补采 20251231 + 20260331。

    注意：Tushare income/balancesheet/cashflow 接口的 start_date/end_date
    过滤的是 ann_date（公告日期），而非 end_date（报告期）。
    例如 Q1 报告期是 20260331，但公告日可能是 20260425。
    因此 end_time 必须用今天的日期，而非报告期。
    """
    setup_logger()
    tushare = TushareService()
    db_manager = DBManager()
    fin = FinanceReportConstant()
    today_str = date.today().strftime('%Y%m%d')

    if target_period is None:
        target_period = _get_latest_period()
    logging.info(f'===== 智能增量采集财报，目标报告期: {target_period}，查询公告日截止: {today_str} =====')

    stock_list = db_manager.get_stock_id_list_all()
    logging.info(f'待处理股票数: {len(stock_list)}')

    report_configs = [
        {
            'name': '利润表',
            'fetch': tushare.get_income_statement,
            'numeric_fields': fin.is_numeric_fields,
            'type': 'is',
            'td_prefix': 'is_tsh',
            'stable': 'income_statement_tushare',
        },
        {
            'name': '资产负债表',
            'fetch': tushare.get_balanceSheet,
            'numeric_fields': fin.bs_numeric_fields,
            'type': 'bs',
            'td_prefix': 'bs_tsh',
            'stable': 'balance_sheet_tushare',
        },
        {
            'name': '现金流量表',
            'fetch': tushare.get_cashflowstatement,
            'numeric_fields': fin.cfs_numeric_fields,
            'type': 'cfs',
            'td_prefix': 'cfs_tsh',
            'stable': 'cash_flow_statement_tushare',
        },
    ]

    for cfg in report_configs:
        logging.info(f'--- 开始采集 {cfg["name"]}，目标报告期 {target_period} ---')
        inserted = 0
        skipped = 0
        already_up_to_date = 0

        for idx, stock in enumerate(stock_list):
            stock_id = stock.stock_id
            location = stock.location
            ts_code = TushareService.convert_stock_id(stock_id, location)

            existing_latest = _get_latest_end_date_from_td(stock_id, cfg['td_prefix'])

            if existing_latest and existing_latest >= target_period:
                already_up_to_date += 1
                continue

            if existing_latest:
                ann_start = existing_latest
            else:
                ann_start = target_period

            time.sleep(0.301)
            try:
                df = cfg['fetch'](stock_id=ts_code, start_time=ann_start, end_time=today_str)
                if df is None or df.empty:
                    skipped += 1
                    continue

                if existing_latest:
                    df = df[df['end_date'] > existing_latest]
                    if df.empty:
                        skipped += 1
                        continue

                TDEngineWriter.create_dynamic_table(
                    "nb_stock", stock_id, location, '',
                    f"{cfg['type']}_tsh_{stock_id}", cfg['stable'], True, "RMB"
                )
                TDEngineTushareWriter.insert_tushare(
                    tushare_data=df,
                    stock_id=stock_id,
                    numeric_fields=cfg['numeric_fields'],
                    type=cfg['type']
                )
                inserted += 1
                periods_got = sorted(df['end_date'].unique().tolist())
                logging.info(f'  {stock_id}: 已有最新={existing_latest or "无"} → 补采 {len(df)} 条 {periods_got}')
            except Exception as e:
                logging.error(f'{stock_id} {cfg["name"]} 采集失败: {e}')

            if (idx + 1) % 500 == 0:
                logging.info(f'  进度: {idx + 1}/{len(stock_list)}，写入 {inserted}，跳过 {skipped}，已最新 {already_up_to_date}')

        logging.info(
            f'{cfg["name"]} 完成: 写入 {inserted}，无数据跳过 {skipped}，已是最新 {already_up_to_date}'
        )

    logging.info(f'===== 智能增量采集完成 =====')


def check_finance_report_completeness(output_csv=None):
    """
    检测所有股票在 TDEngine 中三张报表的数据完整性。
    输出：缺期数据、跨表不对齐、最新期未采集的股票。
    :param output_csv: 可选，将结果保存为 CSV 文件路径
    """
    setup_logger()
    db_manager = DBManager()
    tdreader = tdReader()

    stock_list = db_manager.get_stock_id_list_all()
    latest_period = _get_latest_period()
    logging.info(f'===== 完整性检测开始，最新报告期: {latest_period}，股票数: {len(stock_list)} =====')

    report_types = {
        'is': 'income_statement_tushare',
        'bs': 'balance_sheet_tushare',
        'cfs': 'cash_flow_statement_tushare',
    }

    issues = []
    for idx, stock in enumerate(stock_list):
        stock_id = stock.stock_id
        launch_date = stock.launch_date

        expected = set(_get_all_expected_periods(launch_date, latest_period))
        actual_map = {}

        for short_name, report_type in report_types.items():
            try:
                reports = tdreader.get_finance_report_all(stock_id=stock_id, report_type=report_type)
                if reports:
                    end_dates = set()
                    for r in reports:
                        ed = r.get('end_date', '')
                        if ed:
                            end_dates.add(str(ed).replace('-', '')[:8])
                    actual_map[short_name] = end_dates
                else:
                    actual_map[short_name] = set()
            except Exception:
                actual_map[short_name] = set()

        is_dates = actual_map.get('is', set())
        bs_dates = actual_map.get('bs', set())
        cfs_dates = actual_map.get('cfs', set())

        missing_latest = []
        for sn, dates in actual_map.items():
            if latest_period not in dates:
                missing_latest.append(sn)

        all_actual = is_dates | bs_dates | cfs_dates
        misaligned = []
        for d in sorted(all_actual):
            present_in = []
            if d in is_dates: present_in.append('is')
            if d in bs_dates: present_in.append('bs')
            if d in cfs_dates: present_in.append('cfs')
            if len(present_in) < 3:
                missing_tables = [t for t in ['is', 'bs', 'cfs'] if t not in present_in]
                misaligned.append(f"{d}缺{','.join(missing_tables)}")

        missing_periods = sorted(expected - all_actual) if expected else []

        if missing_latest or misaligned or len(missing_periods) > 0:
            issue = {
                'stock_id': stock_id,
                'is_count': len(is_dates),
                'bs_count': len(bs_dates),
                'cfs_count': len(cfs_dates),
                'missing_latest': ','.join(missing_latest) if missing_latest else '',
                'misaligned_top5': '; '.join(misaligned[:5]) if misaligned else '',
                'missing_periods_count': len(missing_periods),
                'missing_periods_sample': ','.join(missing_periods[-5:]) if missing_periods else '',
            }
            issues.append(issue)

        if (idx + 1) % 500 == 0:
            logging.info(f'已检测 {idx + 1}/{len(stock_list)} 只股票，发现 {len(issues)} 只有问题')

    logging.info(f'===== 检测完成: {len(stock_list)} 只股票，{len(issues)} 只存在问题 =====')

    if issues:
        result_df = pd.DataFrame(issues)
        if output_csv:
            result_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
            logging.info(f'检测报告已保存: {output_csv}')
        else:
            logging.info(f'\n--- 问题汇总（前20条） ---')
            for i, row in enumerate(issues[:20]):
                logging.info(
                    f"  {row['stock_id']}: IS={row['is_count']} BS={row['bs_count']} CFS={row['cfs_count']} "
                    f"最新期缺=[{row['missing_latest']}] 不对齐=[{row['misaligned_top5']}] "
                    f"缺期数={row['missing_periods_count']}"
                )
            if len(issues) > 20:
                logging.info(f'  ... 还有 {len(issues) - 20} 只股票有问题，建议传入 output_csv 参数导出完整报告')

    return issues


def dig_cash_flow_statement_yoy_tushare(stock_list=None):
    setup_logger()
    db_manager = DBManager()
    
    if stock_list is None:
        stock_list = db_manager.get_stock_id_list_all()
    tdreader = tdReader()
    finance_reporter = FinanceReportTushare()
    
    for stock in stock_list:
        current_stock_id = stock.stock_id
        location = stock.location
        logging.info(f'{current_stock_id} - 开始计算现金流同比表同比')
        try:
            is_reports = tdreader.get_finance_report_all(stock_id=current_stock_id, report_type="cash_flow_statement_tushare")
            
            if not is_reports or len(is_reports) < 2:
                logging.warning(f'{current_stock_id} - 现金流同比表数据不足，跳过')
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