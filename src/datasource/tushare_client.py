import tushare as ts
import pandas as pd
from typing import List, Optional, Literal
import utils.config_loader as cfg
import logging
from database.db_manager import DBManager
import time
from datetime import date
from utils.logger import setup_logger,log
from database.tdengine_writer import TDEngineWriter
from finance_report.balance_sheet import BalanceSheet
from finance_report.cash_flow_statement import CashFlowStatement
from database.tdengine_connector import tdengine
from database.models import StockBasicInfo
from finance_report.finance_report_constant import FinanceReportConstant

logger = logging.getLogger(__name__)

class TushareService:
    _latest_trade_date_cache = None

    def __init__(self):
        config = cfg.get_tushare()
        print("Loaded DB config:", config)
        self.token = config['api_key']
        ts.set_token(self.token)
        self.pro = ts.pro_api()

    def _get_latest_trade_date(self) -> str:
        """获取当前日期的最近一个交易日（YYYYMMDD），结果缓存避免重复查询"""
        if TushareService._latest_trade_date_cache:
            return TushareService._latest_trade_date_cache
        today = date.today().strftime('%Y%m%d')
        cal = self.pro.trade_cal(exchange='SSE', start_date='20260101', end_date=today, is_open='1')
        if cal is not None and not cal.empty:
            latest = cal.sort_values('cal_date', ascending=False).iloc[0]['cal_date']
            TushareService._latest_trade_date_cache = latest
            print(f"最近交易日: {latest}")
            return latest
        return today
    
    def get_stock_data(
        self, 
        symbol: str,
        period: Literal['daily', 'weekly', 'monthly'] = 'daily',
        start_date: str = None,
        end_date: str = None,
        adjust: str = ""
    ) -> Optional[pd.DataFrame]:
        """
        获取股票历史数据(日/周/月)
        :param symbol: 股票代码 (格式: 600000.SH 或 000001.SZ)
        :param period: 周期类型 daily-日线 weekly-周线 monthly-月线
        :param start_date: 开始日期 (YYYYMMDD)
        :param end_date: 结束日期 (YYYYMMDD)
        :param adjust: 复权类型 ("qfq"-前复权, "hfq"-后复权, ""-不复权)
        :return: DataFrame 或 None
        """
        try:
            logger.info(f"{symbol},{period},{start_date},{end_date},{adjust}")
            
            # Tushare股票代码格式转换（如果需要）
            if not '.' in symbol:
                if symbol.startswith(('6', '5', '9')):  # 上海股票
                    symbol = f"{symbol}.SH"
                else:  # 深圳股票
                    symbol = f"{symbol}.SZ"
            
            # 根据周期选择不同的接口:cite[1]:cite[8]
            if period == 'daily':
                df = self.pro.daily(
                    ts_code=symbol,
                    start_date=start_date,
                    end_date=end_date
                )
            elif period == 'weekly':
                df = self.pro.weekly(
                    ts_code=symbol,
                    start_date=start_date,
                    end_date=end_date
                )
                
            elif period == 'monthly':
                df = self.pro.monthly(
                    ts_code=symbol,
                    start_date=start_date,
                    end_date=end_date
                )
            else:
                raise ValueError(f"不支持的周期类型: {period}")
            
            if df.empty:
                logger.warning(f"未获取到{symbol}的数据")
                return None
            
            # 字段重命名以保持与你原有代码兼容:cite[8]
            df = df.rename(columns={
                "trade_date": "trade_date",
                "open": "open",
                "close": "close", 
                "high": "high",
                "low": "low",
                "vol": "vol",
                "amount": "amount",
                "pct_chg": "pct_chg",
                "change": "change"
            })
            
            # 添加股票标识
            df["stock_id"] = symbol
            
            # Tushare的成交量单位是手，与AKShare一致，不需要转换
            # 但成交额单位是千元，如果需要统一可以转换
            df["amount"] = df["amount"] * 1000  # 转换为元
            
            # 按日期排序（Tushare返回的数据可能是倒序的）
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Tushare获取{symbol}{period}数据失败: {e}")
            return None

    def get_adj_stock_data(
        self,
        symbol: str,
        period: Literal['daily', 'weekly', 'monthly'] = 'daily',
        start_date: str = None, 
        end_date: str = None,
        adjust: str = 'qfq'
    ) -> Optional[pd.DataFrame]:
        """
        通过 ts.pro_bar 通用行情接口获取股票行情数据
        :param adjust: 'qfq'=前复权(默认), 'hfq'=后复权, None=不复权
                       注意：复权目前仅日线生效，周/月线会自动回退为不复权
        """
        freq_map = {
            'daily': 'D',
            'weekly': 'W',
            'monthly': 'M'
        }
        try:
            freq = freq_map[period]
            adj = adjust if freq == 'D' else None
            print(f"pro_bar: {symbol} freq={freq} adj={adj} {start_date}~{end_date}")

            df = ts.pro_bar(
                ts_code=symbol,
                freq=freq,
                start_date=start_date,
                end_date=end_date,
                adj=adj,
                factors=['tor'],
            )

            if df is None or df.empty:
                logger.warning(f"未获取到{symbol}的行情数据")
                return None

            if 'tor' in df.columns:
                df = df.rename(columns={'tor': 'turnover_rate'})

            keep_cols = [
                'trade_date', 'open', 'high', 'low', 'close',
                'vol', 'amount', 'change', 'pct_chg', 'turnover_rate',
            ]
            df = df[[c for c in keep_cols if c in df.columns]]
            df['stock_id'] = symbol

            return df.sort_values('trade_date').reset_index(drop=True)

        except Exception as e:
            logger.error(f"Tushare pro_bar 获取{symbol}行情失败: {e}")
            return None
    
    def get_minute_data(
        self,
        symbol: str,
        freq: str = '60min',
        start_date: str = None,
        end_date: str = None,
    ) -> Optional[pd.DataFrame]:
        """
        通过 stk_mins 获取分钟级别行情数据
        :param symbol: 股票代码 (格式: 600000.SH)
        :param freq: 频率 1min/5min/15min/30min/60min
        :param start_date: 开始日期 (YYYY-MM-DD HH:MM:SS 或 YYYYMMDD)
        :param end_date:   结束日期
        :return: DataFrame，列名与 get_adj_stock_data 对齐
        """
        try:
            if start_date and len(start_date) == 8:
                start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]} 09:00:00"
            if end_date and len(end_date) == 8:
                end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]} 16:00:00"

            logger.info(f"stk_mins: {symbol} freq={freq} {start_date}~{end_date}")

            df = self.pro.stk_mins(
                ts_code=symbol,
                freq=freq,
                start_date=start_date,
                end_date=end_date,
            )

            if df is None or df.empty:
                logger.warning(f"未获取到{symbol}的分钟数据")
                return None

            df = df.rename(columns={
                'trade_time': 'trade_date',
                'vol': 'vol',
            })
            df['change'] = 0.0
            df['pct_chg'] = 0.0
            df['turnover_rate'] = 0.0
            df['stock_id'] = symbol

            keep_cols = [
                'trade_date', 'open', 'high', 'low', 'close',
                'vol', 'amount', 'change', 'pct_chg', 'turnover_rate',
            ]
            df = df[[c for c in keep_cols if c in df.columns]]
            return df.sort_values('trade_date').reset_index(drop=True)

        except Exception as e:
            logger.error(f"Tushare stk_mins 获取{symbol}分钟数据失败: {e}")
            return None

    # stock_id:'000001.SH' start_time:'20180101' end_date='20180730'
    def get_income_statement(self,stock_id:str,start_time,end_time):
        fin = FinanceReportConstant()
        fields = fin.is_fields_all
        
        df = self.pro.income(ts_code=stock_id, start_date=start_time, end_date=end_time, fields=fields)
        # 方法1：临时设置显示选项
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
            # print(df)
        return df
    
    def get_balanceSheet(self,stock_id:str,start_time,end_time):
        bs = BalanceSheet()
        fin = FinanceReportConstant()

        tushare_fields  = fin.bs_fields_all
        fields = ','.join(tushare_fields)
        
        df = self.pro.balancesheet(ts_code=stock_id, start_date=start_time, end_date=end_time, fields=fields)
        # 方法1：临时设置显示选项
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
        #     # print(df)
        return df
    
    def get_cashflowstatement(self,stock_id:str,start_time,end_time):
        cfs = CashFlowStatement()
        fin = FinanceReportConstant()
        tushare_fields  = fin.cfs_fields_all
        fields = ','.join(tushare_fields)
        
        df = self.pro.cashflow(ts_code=stock_id, start_date=start_time, end_date=end_time, fields=fields)
        # 方法1：临时设置显示选项
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
        #     print(df)
        return df


    def get_income_by_period(self, period: str) -> Optional[pd.DataFrame]:
        """按报告期批量获取所有公司利润表（如 period='20260331'）"""
        fin = FinanceReportConstant()
        fields = fin.is_fields_all
        try:
            df = self.pro.income(period=period, fields=fields)
            logger.info(f"利润表 period={period} 获取到 {len(df) if df is not None else 0} 条")
            return df
        except Exception as e:
            logger.error(f"批量获取利润表失败 period={period}: {e}")
            return None

    def get_balancesheet_by_period(self, period: str) -> Optional[pd.DataFrame]:
        """按报告期批量获取所有公司资产负债表"""
        fin = FinanceReportConstant()
        fields = ','.join(fin.bs_fields_all)
        try:
            df = self.pro.balancesheet(period=period, fields=fields)
            logger.info(f"资产负债表 period={period} 获取到 {len(df) if df is not None else 0} 条")
            return df
        except Exception as e:
            logger.error(f"批量获取资产负债表失败 period={period}: {e}")
            return None

    def get_cashflow_by_period(self, period: str) -> Optional[pd.DataFrame]:
        """按报告期批量获取所有公司现金流量表"""
        fin = FinanceReportConstant()
        fields = ','.join(fin.cfs_fields_all)
        try:
            df = self.pro.cashflow(period=period, fields=fields)
            logger.info(f"现金流量表 period={period} 获取到 {len(df) if df is not None else 0} 条")
            return df
        except Exception as e:
            logger.error(f"批量获取现金流量表失败 period={period}: {e}")
            return None

    def check_delisted_stocks(self, stock_ids: List[str]) -> List[str]:
        """
        通过 Tushare stock_basic 批量检查退市股票
        :param stock_ids: 数据库中的股票代码列表（如 ['000023', '600070']）
        :return: 其中已退市的股票代码列表
        """
        try:
            delisted_df = self.pro.stock_basic(
                exchange='',
                list_status='D',
                fields='ts_code,symbol,name,delist_date'
            )

            delisted_symbols = set(delisted_df['symbol'].tolist())

            delisted_in_db = [sid for sid in stock_ids if sid in delisted_symbols]

            print(f"数据库股票数量: {len(stock_ids)}")
            print(f"Tushare已退市数量: {len(delisted_symbols)}")
            print(f"数据库中已退市: {len(delisted_in_db)}")
            if delisted_in_db:
                print(f"退市列表: {delisted_in_db}")

            return delisted_in_db

        except Exception as e:
            logger.error(f"检查退市股票失败: {e}")
            return []

    @staticmethod
    def convert_stock_id(stock_id,location):
        localtionMap = {
                'china.shenzhen': 'SZ',
                'china.shanghai': 'SH', 
                'china.beijing': 'BJ'
            }
        newStockId = stock_id.lower()+"."+localtionMap[location]
        return newStockId
    
    @staticmethod
    def convert_to_basic_stock_id(full_stock_id):
        """推荐使用的方法"""
        if full_stock_id and '.' in full_stock_id:
            return full_stock_id.rsplit('.', 1)[0]
        return full_stock_id
    
    def check_new_stocks(self, stock_id_list: List[str]) -> List[str]:
        try:
            tushare_stocks_df = self.pro.stock_basic(
                exchange='', 
                list_status='L', 
                fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type'
            )
            
            tushare_stock_ids = tushare_stocks_df['ts_code'].tolist()
            
            # 两个 list 去重
            existing_stocks_set = set(stock_id_list)
            tushare_stocks_set = set(tushare_stock_ids)
            # 两个 list的 元素 求差集
            new_stocks = list(tushare_stocks_set - existing_stocks_set)
            
            # 4. 输出统计信息
            print(f"Tushare上市股票数量: {len(tushare_stock_ids)}")
            print(f"数据库已有股票数量: {len(stock_id_list)}")
            print(f"新增股票数量: {len(new_stocks)}")
            
            # 5. 过滤出新股票并插入基本信息（is_new=1）
            new_stocks_df = tushare_stocks_df[tushare_stocks_df['ts_code'].isin(new_stocks)]
            inserted_count = 0
            db = DBManager()
            for _, stock_row in new_stocks_df.iterrows():
                try:
                    mapped_data = self.map_tushare_to_stock_basic(stock_row, is_new=1)
                    db.update_basic_info(mapped_data)
                    inserted_count += 1
                    print(f"已插入新股: {mapped_data['stock_id']} - {mapped_data['stock_name']}")
                except Exception as e:
                    logger.error(f"插入股票 {stock_row['ts_code']} 失败: {e}")
                    continue
            
            return new_stocks
            
        except Exception as e:
            logger.error(f"检查新增股票失败: {e}")
            return []
    
    # 1.从tushare获取市值和基本信息  2. 更新week级别的数据
    def update_basic_get_stock(self) :
            try:
                setup_logger()
                tushare_stocks_df = self.pro.stock_basic(
                    exchange='', 
                    list_status='L', 
                    fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type'
                )
                # 2.准备
                tushare = TushareService()
                db_manager = DBManager()
                start_date = date(2026, 4, 30).strftime('%Y%m%d')
                end_date = date(2026, 4, 30).strftime('%Y%m%d')
                stock_list = db_manager.get_stock_id_list()
                print(stock_list)
                
                # tushare: daily_day,weekly_week,monthly_month
                input_str = "weekly_week"
                items = input_str.split(',')

                for item in items:
                    
                    parts = item.split('_')

                    for stock in stock_list:
                        
                        time.sleep(0.31)
                        stock_id = stock.stock_id
                        location = stock.location
                        newStockId = TushareService.convert_stock_id(stock_id=stock_id,location=location)
                        # 1.获取该 stock_id 的 基本信息并更新到 mysql.stock_basic_info
                        new_stocks_df = tushare_stocks_df[tushare_stocks_df['ts_code'] ==  newStockId]
                        inserted_count = 0
                        for _, stock_row in new_stocks_df.iterrows():
                            try:
                                # 映射Tushare数据到数据库表结构
                                mapped_data = self.map_tushare_to_stock_basic(stock_row)
                                
                                
                                db = DBManager()
                                db.update_basic_info(mapped_data)
                                
                                inserted_count += 1
                                
                                print(f"已插入: {mapped_data['stock_id']} - {mapped_data['stock_name']}")
                                
                            except Exception as e:
                                logger.error(f"插入股票 {stock_row['ts_code']} 失败: {e}")
                                continue
                        # weekly
                        period_tu = parts[0]
                        # week
                        period_local = parts[1]
                        
                        log.info(f"正在处理股票: {stock},{period_local},{period_tu}")

                        raw_data = tushare.get_adj_stock_data(
                            symbol=newStockId,
                            period=period_tu,
                            start_date=start_date,
                            end_date=end_date
                        )
                        
                        if raw_data is not None:
                            """同步数据到 mysql"""
                            db_ready_data = db_manager.convert_to_db_format_tushare(stock_id,raw_data,period_local)
                            
                            db_manager.save_daily_data(db_ready_data)

                            """同步数据到TDEngine"""
                            # 确保表存在
                            table_name_td = f"{period_local}_{stock_id}"
                            TDEngineWriter.create_dynamic_table("nb_stock",stock_id,stock.location,period_local,table_name_td,"stock_trade_history",False,"RMB")
                            
                            # 批量写入数据
                            TDEngineWriter.write_data_batch(
                                data=db_ready_data,
                                company_id=stock_id,
                                table_name = table_name_td
                            )
                
            except Exception as e:
                logger.error(f"检查新增股票失败: {e}")
                return []

    def map_tushare_to_stock_basic(self, tushare_data, is_new=0):
        mapping = {
            'stock_name': 'name',                    # 股票名称
            'stock_id': 'ts_code',                   # 股票代码
            'location': 'exchange',                  # 交易所
            'market': 'market',                  # 交易所
            'launch_date': 'list_date',              # 上市日期
            'industry': 'industry',                  # 行业
            'is_retired': 'list_status',             # 退市标志
            'fullname': 'fullname',                  # 股票全称
            'enname': 'enname',                      # 英文全称
            'cnspell': 'cnspell',                    # 拼音缩写
            'is_hs': 'is_hs',                        # 退市标志
            'curr_type': 'curr_type',                # 交易货币
            'delist_date': 'delist_date',            # 交易货币
            'act_name': 'act_name',                  # 交易货币
            'act_ent_type': 'act_ent_type',          # 交易货币
            
        }
        
        mapped_data = {}
        for target_field, source_field in mapping.items():
            mapped_data[target_field] = tushare_data.get(source_field)
        
        exchange_map = {
            'SSE': 'china.shanghai',
            'SZSE': 'china.shenzhen',
            'BSE': 'china.beijing'
        }
        exchange = tushare_data.get('exchange', '')
        mapped_data['location'] = exchange_map.get(exchange, exchange)
        
        # 2. 退市标志映射
        list_status = tushare_data.get('list_status', 'L')
        mapped_data['is_retired'] = 1 if list_status in ['D', 'P'] else 0
        
        # 3. 上市日期格式转换
        list_date = tushare_data.get('list_date')
        if list_date:
            mapped_data['launch_date'] = pd.to_datetime(list_date)
        
        delist_date = tushare_data.get('delist_date')
        if delist_date:
            mapped_data['delist_date'] = pd.to_datetime(delist_date)
            
        # 4. 设置默认值 https://tushare.pro/document/2?doc_id=32
        time.sleep(0.301)
        stock_id = mapped_data['stock_id']
        latest_trade_date = self._get_latest_trade_date()
        trade_basic = self.pro.daily_basic(ts_code=stock_id, trade_date=latest_trade_date, fields='ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv,total_share,float_share')
        
        mapped_data.update({
            'circulating_market_value': self.get_numeric_value(trade_basic,'circ_mv',0.0) * 10000,
            'total_market_value': self.get_numeric_value(trade_basic,'total_mv',0.0) * 10000,
            'circulating_stock': self.get_numeric_value(trade_basic,'float_share',0.0) * 10000,
            'total_stock': self.get_numeric_value(trade_basic,'total_share',0.0) * 10000,
            'create_user': 'tushare_sync',
            'is_new': is_new
        })
        stock_id = self.convert_to_basic_stock_id(stock_id)
        mapped_data.update({
            'stock_id': stock_id         
        })
        return mapped_data
    
    # 处理数值字段，确保是具体的数值而不是Series
    @staticmethod
    def get_numeric_value(data, key, default=0.0):
        """从 DataFrame 或 dict 中安全提取数值"""
        value = data.get(key)
        if value is None:
            return default
        if isinstance(value, pd.Series):
            if value.empty:
                return default
            value = value.iloc[0]
        if pd.isna(value):
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def getBJStock(self,stock_id:str):
        setup_logger()
        tushare = TushareService()
        db_manager = DBManager()
        stock_list = db_manager.get_beijing_stock_list()
        print(stock_list)
        #获取方大新材新旧代码对照数据
        for stock in stock_list:
            time.sleep(0.31)
            oldStockIdFull = tushare.convert_stock_id(stock_id=stock.stock_id,location=stock.location)
            df = self.pro.bse_mapping(o_code=oldStockIdFull)
            
            n_code = df['n_code'].str.replace('.BJ', '', regex=False).iloc[0]
            print(n_code)
            # 1.tdengine 中 income_statement 表的所有子表都要更新 表名和tag名
            #   第一步是新建tdengine的表，再执行 insert into is_903103 select * from is_803103; 下面都是类似的
            tushare.tdengine_op(stock,n_code,'is','income_statement')
            tushare.tdengine_op(stock,n_code,'is_yoy','income_statement_yoy')
            tushare.tdengine_op(stock,n_code,'bs','balance_sheets')
            tushare.tdengine_op(stock,n_code,'bs_yoy','balance_sheets_growth')
            tushare.tdengine_op(stock,n_code,'cfs','cash_flow_statements')
            tushare.tdengine_op(stock,n_code,'cfs_yoy','cash_flow_statements_growth')
            # 2 更新mysql的basic
            update_basic = f'update stock_basic_info set stock_id = {n_code} where stock_id = {stock.stock_id}'
            db_manager.execute_sql(update_basic)
            
            # 3 更新 mysql的 perdayFianl
            update_perday = f'update stock_per_day_final set stock_id = {n_code} where stock_id = {stock.stock_id}'
            db_manager.execute_sql(update_perday)
            
    
    def tdengine_op(self,stock:StockBasicInfo,n_code,type,fin_type):
        try:
            td_table = f'{type}_{stock.stock_id}'
            new_td_table = f'{type}_{n_code}'
            TDEngineWriter.create_dynamic_table('nb_stock',n_code,stock.location,'',new_td_table,fin_type,True,"RMB")
            query = f'select * from {td_table}'
            result = tdengine.execute(query)
            if(result):
                insert_sql = f'insert into {new_td_table} select * from {td_table}'
                tdengine.execute(insert_sql)
                delete_sql = f'drop table {td_table}'
                tdengine.execute(delete_sql)
        except Exception as e:
            
            logger.error(f"操作tdengine异常: {e}")
        finally:
            return

    def test_stk_week_month_adj(self):
        """测试 stk_week_month_adj 接口，获取600479周线级别数据，对比三种复权"""
        df = self.pro.stk_week_month_adj(
            ts_code='600479.SH',
            freq='week',
            start_date='20260403',
            end_date='20260403'
        )
        print("=== 返回所有列 ===")
        print(df.columns.tolist())

        common_cols = ['ts_code', 'trade_date', 'vol', 'amount', 'change', 'pct_chg']

        print("\n=== 不复权 ===")
        raw_cols = common_cols + ['open', 'high', 'low', 'close']
        print(df[[c for c in raw_cols if c in df.columns]])

        print("\n=== 前复权 (qfq) ===")
        qfq_cols = common_cols + ['open_qfq', 'high_qfq', 'low_qfq', 'close_qfq']
        print(df[[c for c in qfq_cols if c in df.columns]])

        print("\n=== 后复权 (hfq) ===")
        hfq_cols = common_cols + ['open_hfq', 'high_hfq', 'low_hfq', 'close_hfq']
        print(df[[c for c in hfq_cols if c in df.columns]])

    def test_income_000001(self):
        """测试 000001 的 2026Q1 利润表实际返回"""
        import pandas as pd
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 200)

        ts_code = '000001.SZ'

        print("=" * 60)
        print(f"测试1: income(ts_code={ts_code}, period='20260331')")
        print("=" * 60)
        df1 = self.pro.income(ts_code=ts_code, period='20260331')
        print(f"返回行数: {len(df1) if df1 is not None else 'None'}")
        if df1 is not None and not df1.empty:
            print(df1[['ts_code', 'ann_date', 'f_ann_date', 'end_date', 'report_type', 'basic_eps', 'total_revenue', 'n_income']])
        else:
            print("无数据")

        print("\n" + "=" * 60)
        print(f"测试2: income(ts_code={ts_code}, start_date='20251231', end_date='20260331')")
        print("=" * 60)
        df2 = self.pro.income(ts_code=ts_code, start_date='20251231', end_date='20260331')
        print(f"返回行数: {len(df2) if df2 is not None else 'None'}")
        if df2 is not None and not df2.empty:
            print(df2[['ts_code', 'ann_date', 'f_ann_date', 'end_date', 'report_type', 'basic_eps', 'total_revenue', 'n_income']])
        else:
            print("无数据")

        print("\n" + "=" * 60)
        print(f"测试3: income(ts_code={ts_code}, start_date='20250101', end_date='20260501') — 宽范围")
        print("=" * 60)
        df3 = self.pro.income(ts_code=ts_code, start_date='20250101', end_date='20260501')
        print(f"返回行数: {len(df3) if df3 is not None else 'None'}")
        if df3 is not None and not df3.empty:
            print(df3[['ts_code', 'ann_date', 'f_ann_date', 'end_date', 'report_type', 'basic_eps', 'total_revenue', 'n_income']])
        else:
            print("无数据")

