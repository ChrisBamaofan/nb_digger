import pandas as pd
from typing import Dict, List, Optional
from database.tdengine_reader import TDEngineReader
from database.tdengine_connector import TDEngineConnector
from utils.date_utils import DateUtils

class IncomeStatementYOYCalculator:
    def __init__(self):
        self.numeric_fields = [
            'net_profit', 'net_profit_atsopc', 'total_revenue', 'op', 
            'income_from_chg_in_fv', 'invest_incomes_from_rr', 'invest_income',
            'exchg_gain', 'operating_taxes_and_surcharge', 'asset_impairment_loss',
            'non_operating_income', 'non_operating_payout', 'profit_total_amt',
            'minority_gal', 'basic_eps', 'dlt_earnings_per_share', 
            'othr_compre_income_atoopc', 'othr_compre_income_atms', 
            'total_compre_income', 'total_compre_income_atsopc', 
            'total_compre_income_atms', 'othr_compre_income',
            'net_profit_after_nrgal_atsolc', 'income_tax_expenses',
            'credit_impairment_loss', 'revenue', 'operating_costs',
            'operating_cost', 'sales_fee', 'manage_fee', 'financing_expenses',
            'rad_cost', 'finance_cost_interest_fee', 'finance_cost_interest_income',
            'asset_disposal_income', 'other_income','noncurrent_assets_dispose_gain' ,
            'noncurrent_asset_disposal_loss' ,'net_profit_bi' ,'continous_operating_np' ,
            'int_income' ,'prem_earned'  ,
            'comm_income'  ,'n_commis_income'  ,'prem_income'  ,
            'n_sec_tb_income' ,'n_sec_uw_income' ,'ebit' ,'ebitda' 

        ]

    """
    计算同比增长率
    current_data: 当前期间数据 (如2025中报)
    previous_data: 去年同期数据 (如2024中报)
    """
    def calculate_yoy_growth(self, current_data: Dict, previous_data: Dict,stock_id:str) -> Dict:
       
        yoy_data = {}
        ts = DateUtils.format_date_to_ymd(current_data.get('ts') )
        # 复制基础信息字段
        td = TDEngineConnector()
        ts = td._convert_to_utc(ts)
        yoy_data['ts'] = ts
        yoy_data['report_name'] = current_data.get('report_name')
        yoy_data['ctime'] = current_data.get('ctime')
        
        for field in self.numeric_fields:
            current_value = current_data.get(field)
            previous_value = previous_data.get(field)
            
            # 计算增长率
            growth_rate = self._calculate_growth_rate(current_value, previous_value)
            yoy_data[f"{field}_yoy"] = growth_rate
        
        # yoy_data['company_id'] = stock_id
        yoy_data['create_time'] = pd.Timestamp.now()
        
        return yoy_data
    
    def _calculate_growth_rate(self, current: Optional[float], previous: Optional[float]) -> Optional[float]:
        """计算增长率: (当前值 - 上期值) / 上期值"""
        if current is None or previous is None:
            return None
        
        if previous == 0:
            # 如果上期值为0，避免除零错误
            return None if current == 0 else (1.0 if current > 0 else -1.0)
        
        return (current - previous) / abs(previous)
    
    def get_previous_period_data(self, stock_id,current_end_date: str) -> Dict:
        """
        获取去年同期数据
        current_end_date: 当前报告期 (如 '2025-06-30')
        company_id: 公司ID
        """
        current_date = pd.to_datetime(current_end_date)
        previous_year = current_date.year - 1
        
        # 根据当前报告期类型确定去年同期
        if current_date.month == 3:  # 一季报
            previous_end_date = f"{previous_year}0331"
        elif current_date.month == 6:  # 中报
            previous_end_date = f"{previous_year}0630"
        elif current_date.month == 9:  # 三季报
            previous_end_date = f"{previous_year}0930"
        elif current_date.month == 12:  # 年报
            previous_end_date = f"{previous_year}1231"
        else:
            return {}
        td_reader = TDEngineReader()
        return td_reader.get_finance_report(stock_id=stock_id,report_date=previous_end_date,report_type='income_statement')
    
    def _result_to_dict(self, result) -> Dict:
        """将查询结果转换为字典"""
        # 根据你的ORM或数据库驱动调整这个方法
        data = {}
        for field in self.numeric_fields:
            data[field] = getattr(result, field, None)
        return data