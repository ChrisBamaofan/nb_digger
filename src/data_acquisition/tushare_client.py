import tushare as ts
import pandas as pd
from typing import Optional, Literal
import logging


logger = logging.getLogger(__name__)

class TushareService:
    def __init__(self, token: str):
        """
        初始化Tushare客户端
        :param token: Tushare token，需要在https://tushare.pro注册获取
        """
        self.token = token
        ts.set_token(token)
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
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
            print(df)
        return df
