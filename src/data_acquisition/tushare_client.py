import tushare as ts
import pandas as pd
from typing import List, Optional, Literal
import utils.config_loader as cfg
import logging
from database.db_manager import DBManager
import time

logger = logging.getLogger(__name__)

class TushareService:
    def __init__(self):
        config = cfg.get_tushare()
        print("Loaded DB config:", config)
        self.token = config['api_key']
        ts.set_token(self.token)
        self.pro = ts.pro_api()
    
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
        adjust: Literal['qfq', 'hfq'] = 'qfq'
    ) -> Optional[pd.DataFrame]:
        
        try:
            freq_map = {
                'daily': 'D',
                'weekly': 'W', 
                'monthly': 'M'
            }

            df = ts.pro_bar( ts_code = symbol, freq=freq_map[period], adj=adjust, start_date=start_date, end_date=end_date,factors= 'tor')

            if df is None or df.empty:
                logger.warning(f"未获取到{symbol}的复权数据")
                return None
            
            # 添加必要字段
            df["stock_id"] = symbol
            # df["trade_date"] = df.index.strftime('%Y%m%d')
            
            return df.reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"Tushare获取{symbol}复权数据失败: {e}")
            return None
    
    # stock_id:'000001.SH' start_time:'20180101' end_date='20180730'
    def get_income_statement(self,stock_id:str,start_time,end_time):
        fields = 'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,rd_exp,fin_exp_int_exp,fin_exp_int_inc,transfer_surplus_rese,transfer_housing_imprest,transfer_oth,adj_lossgain,withdra_legal_surplus,withdra_legal_pubfund,withdra_biz_devfund,withdra_rese_fund,withdra_oth_ersu,workers_welfare,distr_profit_shrhder,prfshare_payable_dvd,comshare_payable_dvd,capit_comstock_div,net_after_nr_lp_correct,credit_impa_loss,net_expo_hedging_benefits,oth_impair_loss_assets,total_opcost,amodcost_fin_assets,oth_income,asset_disp_income,continued_net_profit,end_net_profit,update_flag'
        
        df = self.pro.income(ts_code=stock_id, start_date=start_time, end_date=end_time, fields=fields)
        # 方法1：临时设置显示选项
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
            # print(df)
        return df
    
    def get_balanceSheet(self,stock_id:str,start_time,end_time):
        fields = ''
        
        df = self.pro.balancesheet(ts_code=stock_id, start_date=start_time, end_date=end_time, fields=fields)
        # 方法1：临时设置显示选项
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
            print(df)
        return df
    
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
            
            # 5. 过滤出新股票
            new_stocks_df = tushare_stocks_df[tushare_stocks_df['ts_code'].isin(new_stocks)]
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
            
            
            return new_stocks
            
        except Exception as e:
            logger.error(f"检查新增股票失败: {e}")
            return []
        

    def map_tushare_to_stock_basic(self,tushare_data):
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
        trade_basic = self.pro.daily_basic(ts_code= stock_id, trade_date='20250930', fields='ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv,total_share,float_share')
        
        mapped_data.update({
            'circulating_market_value': self.get_numeric_value(trade_basic,'circ_mv',0.0) * 10000,
            'total_market_value': self.get_numeric_value(trade_basic,'total_mv',0.0) * 10000,
            'circulating_stock': self.get_numeric_value(trade_basic,'float_share',0.0) * 10000,
            'total_stock': self.get_numeric_value(trade_basic,'total_share',0.0) * 10000,
            'create_user': 'tushare_sync',
            'is_new': 1
        })
        stock_id = self.convert_to_basic_stock_id(stock_id)
        mapped_data.update({
            'stock_id': stock_id         
        })
        return mapped_data
    
    # 处理数值字段，确保是具体的数值而不是Series
    @staticmethod
    def get_numeric_value(data, key, default=0.0):
        value = data.get(key)
        if hasattr(value, 'item'):  # 如果是numpy类型
            return float(value.item())
        elif value is None:
            return default
        else:
            try:
                return float(value)
            except (TypeError, ValueError):
                return default