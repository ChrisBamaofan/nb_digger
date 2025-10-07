# src/database/tdengine_writer.py
import pandas as pd
from typing import List, Dict
from .tdengine_connector import tdengine
from database.tdengine_connector import tdengine

class TDEngineReader:

    def __init__(self):
        # 少了 comp_type 多了 invest_incomes_from_rr
        self.is_numeric_fields = [
                    'ts','report_name','ctime','net_profit', 'net_profit_atsopc', 'total_revenue', 'op', 
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
                    'create_time' ,'int_income' ,'prem_earned'  ,
                    'comm_income'  ,'n_commis_income'  ,'prem_income'  ,
                    'n_sec_tb_income' ,'n_sec_uw_income' ,'ebit' ,'ebitda' 

                ]
        
        
    def get_finance_report(self, stock_id,report_date: str,report_type:str) -> Dict:
        """
        获取去年同期数据
        current_end_date: 当前报告期 (如 '2025-06-30')
        company_id: 公司ID
        """
        # report_date = pd.to_datetime(report_date)
        
        if report_type == 'income_statement':
            t_prefix = 'is'
        elif report_type == 'balance_sheet':
            t_prefix = 'bs'
        elif report_type == 'cash_flow_statement':
            t_prefix = 'cfs'
        elif report_type == 'income_statement_yoy':
            t_prefix = 'is_yoy'
        elif report_type == 'balance_sheet_yoy':
            t_prefix = 'bs_yoy'
        elif report_type == 'cash_flow_statement_yoy':
            t_prefix = 'cfs_yoy'
        else:
            return {}

        report_date = tdengine._convert_to_utc(report_date)
        print(report_date)

        # 从TDengine查询去年同期数据
        try:
            result = tdengine.conn.query(f"""
                SELECT * FROM {t_prefix}_{stock_id} 
                WHERE  ts='{report_date}'
            """)
            
            if result:
                # 获取第一行
                for row in result:
                    return self._result_to_dict(row)
                # 如果没有数据
                return {}
            else:
                print(f"未找到去年同期数据: {stock_id} {report_date}")
                return {}
                
        except Exception as e:
            print(f"查询去年同期数据失败: {e}")
            return {}
    
    def _result_to_dict(self, result_row) -> Dict:
        data = {}
    
        # 使用索引访问，就像官方例子中的 row[0], row[1], row[2]
        for i, field in enumerate(self.is_numeric_fields):
            if i < len(result_row):
                data[field] = result_row[i]
            else:
                data[field] = None
        print(data)
        return data