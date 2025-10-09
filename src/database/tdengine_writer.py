# src/database/tdengine_writer.py
from datetime import datetime
import pandas as pd
from typing import List, Dict
from .tdengine_connector import tdengine
from utils.logger import log
from utils.date_utils import date_utils
from utils.finance_util import map_tushare_to_xueqiu,generate_report_name,map_tushare_to_xueqiu_balance
from finance_report.income_statement_yoy import IncomeStatementYOYCalculator

class TDEngineWriter:

    @staticmethod
    def write_daily_data_batch(data: pd.DataFrame, company_id: str,table_name:str):
        """
        批量写入日线数据到TDEngine动态表(day_公司ID)
        :param data: 包含交易数据的DataFrame
        :param company_id: 公司ID (如: 000001)
        """
        if data.empty:
            log.warning("空数据，跳过TDEngine写入")
            return False
        
        try:
            sql_values = []
            for _, row in data.iterrows():
                # 格式化时间戳为TDEngine兼容格式
                trade_date = pd.to_datetime(row['trade_date']).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                # 构建单行数据SQL片段
                value_str = f"('{date_utils.prepare_timestamp(trade_date)}', " \
                            f"{int(row['amount'])}, " \
                            f"{int(row['volume'])}, " \
                            f"{float(row['start_price'])}, " \
                            f"{float(row['end_price'])}, " \
                            f"{float(row['high_price'])}, " \
                            f"{float(row['low_price'])}, " \
                            f"{float(row.get('change_price', 0))}, " \
                            f"{float(row.get('change_percent', 0))}, " \
                            f"{float(row.get('turnover_ratio', 0))})"
                sql_values.append(value_str)
            
            # 构建完整SQL语句
            sql = f"""
                INSERT INTO {table_name} 
                (trade_date, amount, volume, open, close, high, low, change_price, change_percent, turnover) 
                VALUES {','.join(sql_values)}
            """
            # 批量写入
            tdengine.execute(sql)
                
            
            log.success(f"成功批量写入{len(sql_values)}条数据到TDEngine表 {table_name}")
            return True
        except Exception as e:
            log.error(f"TDEngine批量写入失败: {e}")
            return False
    
    @staticmethod
    def create_dynamic_table(db:str,company_id: str, location:str, scope:str ,table_name:str,stable:str ,fin:bool):
        values = []
        values.append(location)
        values.append(company_id)
        values.append(scope)

        try:
            if fin:
                formatted_sql = f"""
                CREATE TABLE IF NOT EXISTS {db}.{table_name} USING {stable} (`company_id`, `location`)  TAGS (
                    '{company_id}', '{location}'
                )
                """.strip().replace('\n', ' ')
            else:
                formatted_sql = f"""
                CREATE TABLE IF NOT EXISTS {db}.{table_name} USING {stable} TAGS (
                    '{values[0]}', '{values[1]}', '{values[2]}'
                )
                """.strip().replace('\n', ' ')
            
            log.info(f"Executing SQL:\n{formatted_sql}")
            
            tdengine.execute(formatted_sql)
            # log.info(f"TDEngine动态表 {db}.{table_name} 已创建/确认存在")
            return True
        except Exception as e:
            log.error(f"TDEngine表创建失败: {e}")
            return False

    
        
    @staticmethod
    def execute_bulk_price_adjustment(stock_id: str, table_name:str,adjustment_diff: float, adjustment_date: str):
        
        utc_date_str = tdengine._convert_to_utc(adjustment_date)
        
        # 1. 查询原始数据
        query_sql = f"""
        SELECT trade_date, amount,volume,open, close,high, low, change_price,  change_percent,turnover 
        FROM `{table_name}`
        WHERE trade_date <= '{utc_date_str}'
        """
        
        try:
            # 2. 获取原始数据
            result = tdengine.conn.query(query_sql)

            if not result.rows:
                log.warning(f"表 {table_name} 中没有符合条件的数据")
                return 0
            
            # 4. 防御性结果检查
            if not hasattr(result, 'rows'):
                raise ValueError("查询结果缺少rows属性")
                
            if isinstance(result, int):
                log.warning(f"警告: 查询返回行数而非数据，实际影响行数={result}")
                return result.rows  # 如果是行数，直接返回
                

            # 3. 在Python中计算新值并重新插入
            update_count = 0
            for row in result:
                trade_date, amount_p,volume_p,open_p, close_p,high_p, low_p, change_price_p,  change_percent_p,turnover_p  = row
                
                # 在内存中计算调整后的值
                new_open = open_p + adjustment_diff
                new_high = high_p + adjustment_diff
                new_low = low_p + adjustment_diff
                new_close = close_p + adjustment_diff
                
                new_time = tdengine._convert_to_utc2(trade_date)
                # 4. 重新插入（覆盖）
                insert_sql = f"""
                INSERT INTO `{table_name}` (trade_date, amount,volume,open, close,high, low, change_price,  change_percent,turnover)
                VALUES ('{new_time}',{amount_p},{volume_p}, {new_open}, {new_close}, {new_high}, {new_low},{change_price_p},{change_percent_p},{turnover_p})
                """
                tdengine.execute(insert_sql)
                update_count += 1
                
            log.info(f"成功更新表 {table_name} 中的 {update_count} 条数据")
            return update_count
            
        except Exception as e:
            log.error(f"更新表 {table_name} 失败: {str(e)}")
            raise

    @staticmethod
    def insert_income_statement(tushare_data,stock_id:str):
        
        # 映射到雪球结构
        for _, row in tushare_data.iterrows():
            xueqiu_mapped_data = map_tushare_to_xueqiu(row)
            utc_ts = tdengine._convert_to_utc2(xueqiu_mapped_data['ts'])
            # 写入TDengine
            sql =f"""
                INSERT INTO is_{stock_id}
                VALUES (
                    '{utc_ts}', 
                    '{xueqiu_mapped_data['report_name']}',
                    {xueqiu_mapped_data['ctime']},
                    {xueqiu_mapped_data['net_profit'] or 'NULL'},
                    {xueqiu_mapped_data['net_profit_atsopc'] or 'NULL'},
                    {xueqiu_mapped_data['total_revenue'] or 'NULL'},
                    {xueqiu_mapped_data['op'] or 'NULL'},
                    {xueqiu_mapped_data['income_from_chg_in_fv'] or 'NULL'},
                    {xueqiu_mapped_data['invest_incomes_from_rr'] or 'NULL'},
                    {xueqiu_mapped_data['invest_income'] or 'NULL'},
                    {xueqiu_mapped_data['exchg_gain'] or 'NULL'},
                    {xueqiu_mapped_data['operating_taxes_and_surcharge'] or 'NULL'},
                    {xueqiu_mapped_data['asset_impairment_loss'] or 'NULL'},
                    {xueqiu_mapped_data['non_operating_income'] or 'NULL'},
                    {xueqiu_mapped_data['non_operating_payout'] or 'NULL'},
                    {xueqiu_mapped_data['profit_total_amt'] or 'NULL'},
                    {xueqiu_mapped_data['minority_gal'] or 'NULL'},
                    {xueqiu_mapped_data['basic_eps'] or 'NULL'},
                    {xueqiu_mapped_data['dlt_earnings_per_share'] or 'NULL'},
                    {xueqiu_mapped_data['othr_compre_income_atoopc'] or 'NULL'},
                    {xueqiu_mapped_data['othr_compre_income_atms'] or 'NULL'},
                    {xueqiu_mapped_data['total_compre_income'] or 'NULL'},
                    {xueqiu_mapped_data['total_compre_income_atsopc'] or 'NULL'},
                    {xueqiu_mapped_data['total_compre_income_atms'] or 'NULL'},
                    {xueqiu_mapped_data['othr_compre_income'] or 'NULL'},
                    {xueqiu_mapped_data['net_profit_after_nrgal_atsolc'] or 'NULL'},
                    {xueqiu_mapped_data['income_tax_expenses'] or 'NULL'},
                    {xueqiu_mapped_data['credit_impairment_loss'] or 'NULL'},
                    {xueqiu_mapped_data['revenue'] or 'NULL'},
                    {xueqiu_mapped_data['operating_costs'] or 'NULL'},
                    {xueqiu_mapped_data['operating_cost'] or 'NULL'},
                    {xueqiu_mapped_data['sales_fee'] or 'NULL'},
                    {xueqiu_mapped_data['manage_fee'] or 'NULL'},
                    {xueqiu_mapped_data['financing_expenses'] or 'NULL'},
                    {xueqiu_mapped_data['rad_cost'] or 'NULL'},
                    {xueqiu_mapped_data['finance_cost_interest_fee'] or 'NULL'},
                    {xueqiu_mapped_data['finance_cost_interest_income'] or 'NULL'},
                    {xueqiu_mapped_data['asset_disposal_income'] or 'NULL'},
                    {xueqiu_mapped_data['other_income'] or 'NULL'},
                    {xueqiu_mapped_data['noncurrent_assets_dispose_gain'] or 'NULL'},
                    {xueqiu_mapped_data['noncurrent_asset_disposal_loss'] or 'NULL'},
                    {xueqiu_mapped_data['net_profit_bi'] or 'NULL'},
                    {xueqiu_mapped_data['continous_operating_np'] or 'NULL'},
                    NOW,
                    {xueqiu_mapped_data['int_income'] or 'NULL'},
                    {xueqiu_mapped_data['prem_earned'] or 'NULL'},
                    {xueqiu_mapped_data['comm_income'] or 'NULL'},
                    {xueqiu_mapped_data['n_commis_income'] or 'NULL'},
                    {xueqiu_mapped_data['prem_income'] or 'NULL'},
                    {xueqiu_mapped_data['n_sec_tb_income'] or 'NULL'},
                    {xueqiu_mapped_data['n_sec_uw_income'] or 'NULL'},
                    {xueqiu_mapped_data['ebit'] or 'NULL'},
                    {xueqiu_mapped_data['ebitda'] or 'NULL'},
                    {xueqiu_mapped_data['comp_type'] or 'NULL'}
                )
            """
            tdengine.execute(sql)
            TDEngineWriter.insert_income_statement_yoy(stock_id=stock_id,xueqiu_mapped_data=xueqiu_mapped_data)

    @staticmethod
    def insert_income_statement_yoy(stock_id,xueqiu_mapped_data):
        # yoy
        calculator = IncomeStatementYOYCalculator()
        # 计算 yoy
        previous_data = calculator.get_previous_period_data(stock_id = stock_id,current_end_date =xueqiu_mapped_data['ts'],report_type='income_statement')
        
        if previous_data:
            yoy_data = calculator.calculate_yoy_growth(current_data = xueqiu_mapped_data, previous_data = previous_data,stock_id=stock_id)
            
            TDEngineWriter._insert_income_statement_yoy(yoy_type='is',yoy_data=yoy_data,stock_id=stock_id)
        else:
            print(f"无法计算增长率，缺少去年同期数据: {stock_id} {xueqiu_mapped_data['ts']}")
    

    @staticmethod
    def insert_balance_sheet_yoy(stock_id,xueqiu_mapped_data):
        # yoy
        calculator = IncomeStatementYOYCalculator()
        # 计算 yoy
        previous_data = calculator.get_previous_period_data(stock_id = stock_id,current_end_date =xueqiu_mapped_data['ts'],report_type='balance_sheets')
        
        if previous_data:
            yoy_data = calculator.calculate_yoy_growth(current_data = xueqiu_mapped_data, previous_data = previous_data,stock_id=stock_id)
            
            TDEngineWriter._insert_income_statement_yoy(yoy_type='bs',yoy_data=yoy_data,stock_id=stock_id)
        else:
            print(f"无法计算增长率，缺少去年同期数据: {stock_id} {xueqiu_mapped_data['ts']}")
            
    # insert yoy to tdengine
    @staticmethod
    def _insert_income_statement_yoy(yoy_type:str,yoy_data: Dict,stock_id:str):
        """写入增长率数据"""
        fields = []
        values = []
        
        for field, value in yoy_data.items():
            fields.append(field)
            if value is not None:
                # 字符串和时间戳需要加引号
                if isinstance(value, (str, pd.Timestamp)):
                    values.append(f"'{value}'")
                else:
                    values.append(str(value))
            else:
                values.append('NULL')
        
        # 构建完整的INSERT语句，包含字段名
        sql = f"""
            INSERT INTO {yoy_type}_yoy_{stock_id} 
            ({', '.join(fields)})
            VALUES ({', '.join(values)})
        """
        
        print(f"执行的SQL: {sql}")  # 调试用
        tdengine.execute(sql)


    @staticmethod
    def insert_balance_sheet(tushare_data,stock_id):
        # 映射到雪球结构
        for _, row in tushare_data.iterrows():
            xueqiu_mapped_data = map_tushare_to_xueqiu_balance(row)
            utc_ts = tdengine._convert_to_utc2(xueqiu_mapped_data['ts'])
            # 写入TDengine
            sql =f"""
                INSERT INTO bs_{stock_id} 
                (ts, report_name, ctime, total_assets, total_liab, asset_liab_ratio, 
                total_quity_atsopc, tradable_fnncl_assets, interest_receivable, 
                saleable_finacial_assets, held_to_maturity_invest, fixed_asset, 
                intangible_assets, construction_in_process, dt_assets, 
                tradable_fnncl_liab, payroll_payable, tax_payable, estimated_liab, 
                dt_liab, bond_payable, shares, capital_reserve, earned_surplus, 
                undstrbtd_profit, minority_equity, total_holders_equity, 
                total_liab_and_holders_equity, lt_equity_invest, derivative_fnncl_liab, 
                general_risk_provision, frgn_currency_convert_diff, goodwill, 
                invest_property, interest_payable, treasury_stock, othr_compre_income, 
                othr_equity_instruments, currency_funds, bills_receivable, 
                account_receivable, pre_payment, dividend_receivable, othr_receivables, 
                inventory, nca_due_within_one_year, othr_current_assets, 
                current_assets_si, total_current_assets, lt_receivable, dev_expenditure, 
                lt_deferred_expense, othr_noncurrent_assets, noncurrent_assets_si, 
                total_noncurrent_assets, st_loan, bill_payable, accounts_payable, 
                pre_receivable, dividend_payable, othr_payables, 
                noncurrent_liab_due_in1y, current_liab_si, total_current_liab, 
                lt_loan, lt_payable, special_payable, othr_non_current_liab, 
                noncurrent_liab_si, total_noncurrent_liab, salable_financial_assets, 
                othr_current_liab, ar_and_br, contractual_assets, bp_and_ap, 
                contract_liabilities, to_sale_asset, other_eq_ins_invest, 
                other_illiquid_fnncl_assets, fixed_asset_sum, fixed_assets_disposal, 
                construction_in_process_sum, project_goods_and_material, 
                productive_biological_assets, oil_and_gas_asset, to_sale_debt, 
                lt_payable_sum, noncurrent_liab_di, perpetual_bond, special_reserve, 
                create_date)
                VALUES (
                    {utc_ts},
                    '{xueqiu_mapped_data.get('report_name') or 'NULL'}',
                    {xueqiu_mapped_data.get('ctime') or 'NULL'},
                    {xueqiu_mapped_data.get('total_assets') or 'NULL'},
                    {xueqiu_mapped_data.get('total_liab') or 'NULL'},
                    {xueqiu_mapped_data.get('asset_liab_ratio') or 'NULL'},
                    {xueqiu_mapped_data.get('total_quity_atsopc') or 'NULL'},
                    {xueqiu_mapped_data.get('tradable_fnncl_assets') or 'NULL'},
                    {xueqiu_mapped_data.get('interest_receivable') or 'NULL'},
                    {xueqiu_mapped_data.get('saleable_finacial_assets') or 'NULL'},
                    {xueqiu_mapped_data.get('held_to_maturity_invest') or 'NULL'},
                    {xueqiu_mapped_data.get('fixed_asset') or 'NULL'},
                    {xueqiu_mapped_data.get('intangible_assets') or 'NULL'},
                    {xueqiu_mapped_data.get('construction_in_process') or 'NULL'},
                    {xueqiu_mapped_data.get('dt_assets') or 'NULL'},
                    {xueqiu_mapped_data.get('tradable_fnncl_liab') or 'NULL'},
                    {xueqiu_mapped_data.get('payroll_payable') or 'NULL'},
                    {xueqiu_mapped_data.get('tax_payable') or 'NULL'},
                    {xueqiu_mapped_data.get('estimated_liab') or 'NULL'},
                    {xueqiu_mapped_data.get('dt_liab') or 'NULL'},
                    {xueqiu_mapped_data.get('bond_payable') or 'NULL'},
                    {xueqiu_mapped_data.get('shares') or 'NULL'},
                    {xueqiu_mapped_data.get('capital_reserve') or 'NULL'},
                    {xueqiu_mapped_data.get('earned_surplus') or 'NULL'},
                    {xueqiu_mapped_data.get('undstrbtd_profit') or 'NULL'},
                    {xueqiu_mapped_data.get('minority_equity') or 'NULL'},
                    {xueqiu_mapped_data.get('total_holders_equity') or 'NULL'},
                    {xueqiu_mapped_data.get('total_liab_and_holders_equity') or 'NULL'},
                    {xueqiu_mapped_data.get('lt_equity_invest') or 'NULL'},
                    {xueqiu_mapped_data.get('derivative_fnncl_liab') or 'NULL'},
                    {xueqiu_mapped_data.get('general_risk_provision') or 'NULL'},
                    {xueqiu_mapped_data.get('frgn_currency_convert_diff') or 'NULL'},
                    {xueqiu_mapped_data.get('goodwill') or 'NULL'},
                    {xueqiu_mapped_data.get('invest_property') or 'NULL'},
                    {xueqiu_mapped_data.get('interest_payable') or 'NULL'},
                    {xueqiu_mapped_data.get('treasury_stock') or 'NULL'},
                    {xueqiu_mapped_data.get('othr_compre_income') or 'NULL'},
                    {xueqiu_mapped_data.get('othr_equity_instruments') or 'NULL'},
                    {xueqiu_mapped_data.get('currency_funds') or 'NULL'},
                    {xueqiu_mapped_data.get('bills_receivable') or 'NULL'},
                    {xueqiu_mapped_data.get('account_receivable') or 'NULL'},
                    {xueqiu_mapped_data.get('pre_payment') or 'NULL'},
                    {xueqiu_mapped_data.get('dividend_receivable') or 'NULL'},
                    {xueqiu_mapped_data.get('othr_receivables') or 'NULL'},
                    {xueqiu_mapped_data.get('inventory') or 'NULL'},
                    {xueqiu_mapped_data.get('nca_due_within_one_year') or 'NULL'},
                    {xueqiu_mapped_data.get('othr_current_assets') or 'NULL'},
                    {xueqiu_mapped_data.get('current_assets_si') or 'NULL'},
                    {xueqiu_mapped_data.get('total_current_assets') or 'NULL'},
                    {xueqiu_mapped_data.get('lt_receivable') or 'NULL'},
                    {xueqiu_mapped_data.get('dev_expenditure') or 'NULL'},
                    {xueqiu_mapped_data.get('lt_deferred_expense') or 'NULL'},
                    {xueqiu_mapped_data.get('othr_noncurrent_assets') or 'NULL'},
                    {xueqiu_mapped_data.get('noncurrent_assets_si') or 'NULL'},
                    {xueqiu_mapped_data.get('total_noncurrent_assets') or 'NULL'},
                    {xueqiu_mapped_data.get('st_loan') or 'NULL'},
                    {xueqiu_mapped_data.get('bill_payable') or 'NULL'},
                    {xueqiu_mapped_data.get('accounts_payable') or 'NULL'},
                    {xueqiu_mapped_data.get('pre_receivable') or 'NULL'},
                    {xueqiu_mapped_data.get('dividend_payable') or 'NULL'},
                    {xueqiu_mapped_data.get('othr_payables') or 'NULL'},
                    {xueqiu_mapped_data.get('noncurrent_liab_due_in1y') or 'NULL'},
                    {xueqiu_mapped_data.get('current_liab_si') or 'NULL'},
                    {xueqiu_mapped_data.get('total_current_liab') or 'NULL'},
                    {xueqiu_mapped_data.get('lt_loan') or 'NULL'},
                    {xueqiu_mapped_data.get('lt_payable') or 'NULL'},
                    {xueqiu_mapped_data.get('special_payable') or 'NULL'},
                    {xueqiu_mapped_data.get('othr_non_current_liab') or 'NULL'},
                    {xueqiu_mapped_data.get('noncurrent_liab_si') or 'NULL'},
                    {xueqiu_mapped_data.get('total_noncurrent_liab') or 'NULL'},
                    {xueqiu_mapped_data.get('salable_financial_assets') or 'NULL'},
                    {xueqiu_mapped_data.get('othr_current_liab') or 'NULL'},
                    {xueqiu_mapped_data.get('ar_and_br') or 'NULL'},
                    {xueqiu_mapped_data.get('contractual_assets') or 'NULL'},
                    {xueqiu_mapped_data.get('bp_and_ap') or 'NULL'},
                    {xueqiu_mapped_data.get('contract_liabilities') or 'NULL'},
                    {xueqiu_mapped_data.get('to_sale_asset') or 'NULL'},
                    {xueqiu_mapped_data.get('other_eq_ins_invest') or 'NULL'},
                    {xueqiu_mapped_data.get('other_illiquid_fnncl_assets') or 'NULL'},
                    {xueqiu_mapped_data.get('fixed_asset_sum') or 'NULL'},
                    {xueqiu_mapped_data.get('fixed_assets_disposal') or 'NULL'},
                    {xueqiu_mapped_data.get('construction_in_process_sum') or 'NULL'},
                    {xueqiu_mapped_data.get('project_goods_and_material') or 'NULL'},
                    {xueqiu_mapped_data.get('productive_biological_assets') or 'NULL'},
                    {xueqiu_mapped_data.get('oil_and_gas_asset') or 'NULL'},
                    {xueqiu_mapped_data.get('to_sale_debt') or 'NULL'},
                    {xueqiu_mapped_data.get('lt_payable_sum') or 'NULL'},
                    {xueqiu_mapped_data.get('noncurrent_liab_di') or 'NULL'},
                    {xueqiu_mapped_data.get('perpetual_bond') or 'NULL'},
                    {xueqiu_mapped_data.get('special_reserve') or 'NULL'},
                    '{xueqiu_mapped_data.get('create_date') or 'NULL'}'
                )
            """
            tdengine.execute(sql)
            TDEngineWriter.insert_balance_sheet_yoy(stock_id=stock_id,xueqiu_mapped_data=xueqiu_mapped_data)