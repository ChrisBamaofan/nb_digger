
from datetime import datetime
from typing import Dict, List, Optional
from utils.logger import setup_logger,log
from database.tdengine_connector import tdengine
from finance_report.finance_report_constant import FinanceReportConstant
import pandas as pd

class FinanceReportTushare:

    def __init__(self):
        pass
    
    # debug *****
    def find_comparable_report(self, reports: List[Dict], current_report: Dict, current_index: int) -> Optional[Dict]:
        """
        找到去年同期的报告数据
        
        Args:
            reports: 排序后的报告列表
            current_report: 当前报告
            current_index: 当前报告在列表中的索引
            
        Returns:
            Optional[Dict]: 去年同期的报告，如果没有返回None
        """
        try:
            current_end_date = current_report.get('end_date')
            if not current_end_date:
                return None
            
            # 当前报告的年月 (如: 202409 -> 2024年9月)
            current_year_month = current_end_date[:6]  # 取前6位 YYYYMM
            
            # 计算去年同期的年月
            current_year = int(current_year_month[:4])
            current_month = current_year_month[4:]
            last_year = current_year - 1
            last_year_month = f"{last_year}{current_month}"  # 如: 202309
            
            # 在报告中查找去年同期的数据
            for report in reports:
                report_end_date = report.get('end_date', '')
                if report_end_date.startswith(last_year_month):
                    return report
            
            return None
            
        except Exception as e:
            log.error(f"寻找可比报告时出错: {e}")
            return None
    
    def insert_income_statement_yoy_tushare(self,stock_id: str, current_report: Dict, previous_report: Dict, location: str):
        """
        插入利润表同比变化数据到TDengine
        
        Args:
            stock_id: 股票代码
            current_report: 当期报告
            previous_report: 上期报告
            location: 地区标签
        """
        try:
            print(current_report)
            print("======================")
            print(previous_report)
            # 计算同比变化
            yoy_data = self.calculate_income_yoy_tushare(current_report, previous_report)
            
            if not yoy_data:
                log.warning(f"{stock_id} - 无法计算同比数据")
                return
            
            # 构建插入SQL
            sql = self.build_income_yoy_insert_sql(stock_id, yoy_data, current_report)
            
            # 执行插入
            tdengine.execute(sql)  # 根据您的实际执行方法调整
            log.info(f"{stock_id} - 成功插入同比数据: {current_report.get('end_date')}")
            
        except Exception as e:
            log.error(f"{stock_id} - 插入同比数据时出错: {e}")

    def insert_balance_sheet_yoy_tushare(self,stock_id: str, current_report: Dict, previous_report: Dict, location: str):
        """
        插入资产负债表同比变化数据到TDengine
        
        Args:
            stock_id: 股票代码
            current_report: 当期报告
            previous_report: 上期报告
            location: 地区标签
        """
        try:
            # 计算同比变化
            constant = FinanceReportConstant()
            
            yoy_data = self.calculate_balance_sheet_yoy(current_report, previous_report,constant.bs_numeric_fields)
            
            if not yoy_data:
                log.warning(f"{stock_id} - 无法计算资产负债表同比数据")
                return
            
            # 构建插入SQL
            sql = self.build_balance_sheet_yoy_insert_sql(stock_id, yoy_data, current_report)
            
            # 执行插入
            tdengine.execute(sql)  # 根据您的实际执行方法调整
            log.info(f"{stock_id} - 成功插入资产负债表同比数据: {current_report.get('end_date')}")
            
            return sql
            
        except Exception as e:
            log.error(f"{stock_id} - 插入资产负债表同比数据时出错: {e}")
            return None

    def insert_cash_flow_statement_yoy_tushare(self,stock_id: str, current_report: Dict, previous_report: Dict, location: str):
        """
        插入现金流表同比变化数据到TDengine
        
        Args:
            stock_id: 股票代码
            current_report: 当期报告
            previous_report: 上期报告
            location: 地区标签
        """
        try:
            # 计算同比变化
            
            yoy_data = self.calculate_balance_sheet_yoy(current_report, previous_report,FinanceReportConstant.cfs_numeric_fields)
            
            if not yoy_data:
                log.warning(f"{stock_id} - 无法计算现金流表同比数据")
                return
            
            # 构建插入SQL
            sql = self.build_balance_sheet_yoy_insert_sql(stock_id, yoy_data, current_report)
            
            # 执行插入
            tdengine.execute(sql)  # 根据您的实际执行方法调整
            log.info(f"{stock_id} - 成功插入现金流表同比数据: {current_report.get('end_date')}")
            
            return sql
            
        except Exception as e:
            log.error(f"{stock_id} - 插入现金流表同比数据时出错: {e}")
            return None
        
    def calculate_balance_sheet_yoy(self,current_report: Dict, previous_report: Dict,numeric_field:List) -> Dict:
        """
        计算资产负债表各项指标的同比变化
        
        Args:
            current_report: 当期报告
            previous_report: 上期报告
            
        Returns:
            Dict: 包含同比变化数据的字典
        """
        yoy_data = {
            'ts_code': current_report.get('ts_code'),
            'end_date': current_report.get('end_date'),
            'report_type': current_report.get('report_type')
        }
        
        
        for field in numeric_field:
            current_value = current_report.get(field)
            previous_value = previous_report.get(field)
            
            # 计算绝对值变化
            abs_change = self.calculate_change(current_value, previous_value, 'abs')
            # 计算百分比变化
            pct_change = self.calculate_change(current_value, previous_value, 'pct')
            
            yoy_data[f'{field}_abs_yoy'] = abs_change
            yoy_data[f'{field}_pct_yoy'] = pct_change
            
            # 同时保存当期值用于参考
            yoy_data[f'{field}_current'] = current_value
        
        return yoy_data

    def build_balance_sheet_yoy_insert_sql(self,stock_id: str, yoy_data: Dict, current_report: Dict) -> str:
        """
        构建资产负债表同比数据插入SQL
        
        Args:
            stock_id: 股票代码
            yoy_data: 同比数据
            current_report: 当期报告（用于获取原始数据）
            
        Returns:
            str: 插入SQL语句
        """
        # 使用报告期末日期作为时间戳
        end_date = yoy_data['end_date']
        end_date = datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
        utc_ts = tdengine._convert_to_utc2(end_date)
        
        constant = FinanceReportConstant()
        
        # 构建字段值列表
        values = [f"'{utc_ts}'"]
        
        # 添加基础信息字段
        values.extend([
            f"'{yoy_data['ts_code'] or ''}'",
            f"'{yoy_data['end_date'] or ''}'",
            f"'{yoy_data['report_type'] or ''}'"
        ])
        
        # 添加所有数值字段的同比变化
        for field in constant.bs_numeric_fields:
            abs_yoy = yoy_data.get(f'{field}_abs_yoy')
            pct_yoy = yoy_data.get(f'{field}_pct_yoy')
            current_value = yoy_data.get(f'{field}_current')
            
            values.extend([
                self._format_sql_value(abs_yoy),      # 绝对值变化
                self._format_sql_value(pct_yoy),      # 百分比变化
                self._format_sql_value(current_value) # 当期值
            ])
        
        # 添加系统字段
        values.extend([
            "NOW()",  # created_time
            "NOW()"   # updated_time
        ])
        
        # 构建完整的SQL语句
        sql = f"INSERT INTO bs_yoy_tsh_{stock_id} VALUES ({', '.join(values)})"
        
        return sql

    def calculate_income_yoy_tushare(self,current_report: Dict, previous_report: Dict) -> Dict:
        """
        计算利润表各项指标的同比变化
        Args:
            current_report: 当期报告
            previous_report: 上期报告
        Returns:
            Dict: 包含同比变化数据的字典
        """
        yoy_data = {
            'ts_code': current_report.get('ts_code'),
            'end_date': current_report.get('end_date'),
            'report_type': current_report.get('report_type')
        }
        finance_report_constant = FinanceReportConstant()
        # 
        
        for field in finance_report_constant.is_numeric_fields:
            current_value = current_report.get(field)
            previous_value = previous_report.get(field)
            
            # 计算绝对值变化
            abs_change = self.calculate_change(current_value, previous_value, 'abs')
            # 计算百分比变化
            pct_change = self.calculate_change(current_value, previous_value, 'pct')
            
            yoy_data[f'{field}_abs_yoy'] = abs_change
            yoy_data[f'{field}_pct_yoy'] = pct_change
        
        return yoy_data
    
    def calculate_change(self,current_value, previous_value, change_type: str):
        """
        计算变化值
        
        Args:
            current_value: 当期值
            previous_value: 上期值
            change_type: 变化类型 'abs'绝对值变化, 'pct'百分比变化
            
        Returns:
            float: 变化值
        """
        if current_value is None or previous_value is None:
            return None
        
        try:
            current = float(current_value)
            previous = float(previous_value)
            
            if previous == 0:
                return None
                
            if change_type == 'abs':
                return current - previous
            elif change_type == 'pct':
                return (current - previous) / abs(previous) * 100
            else:
                return None
                
        except (ValueError, TypeError):
            return None
    

    
    def build_income_yoy_insert_sql(self,stock_id: str, yoy_data: Dict, current_report: Dict) -> str:
        """
        构建同比数据插入SQL
        
        Args:
            stock_id: 股票代码
            yoy_data: 同比数据
            current_report: 当期报告（用于获取原始数据）
            
        Returns:
            str: 插入SQL语句
        """
        # 使用报告期末日期作为时间戳
        end_date = yoy_data['end_date']
        end_date = datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
        utc_ts = tdengine._convert_to_utc2(end_date)
        # ts = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]} 00:00:00.000"
        
        sql = f"""
            INSERT INTO is_yoy_tsh_{stock_id} VALUES (
                '{utc_ts}',
                '{yoy_data['ts_code'] or ''}',
                '{yoy_data['end_date'] or ''}',
                '{yoy_data['report_type'] or ''}',
                {self._format_sql_value(yoy_data.get('basic_eps_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('basic_eps_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('diluted_eps_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('diluted_eps_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('total_revenue_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('total_revenue_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('revenue_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('revenue_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('int_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('int_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('prem_earned_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('prem_earned_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('comm_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('comm_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('n_commis_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('n_commis_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('n_oth_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('n_oth_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('n_oth_b_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('n_oth_b_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('prem_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('prem_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('out_prem_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('out_prem_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('une_prem_reser_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('une_prem_reser_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('reins_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('reins_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('n_sec_tb_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('n_sec_tb_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('n_sec_uw_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('n_sec_uw_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('n_asset_mg_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('n_asset_mg_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('oth_b_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('oth_b_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('fv_value_chg_gain_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('fv_value_chg_gain_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('invest_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('invest_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('ass_invest_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('ass_invest_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('forex_gain_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('forex_gain_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('total_cogs_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('total_cogs_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('oper_cost_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('oper_cost_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('int_exp_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('int_exp_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('comm_exp_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('comm_exp_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('biz_tax_surchg_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('biz_tax_surchg_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('sell_exp_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('sell_exp_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('admin_exp_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('admin_exp_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('fin_exp_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('fin_exp_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('assets_impair_loss_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('assets_impair_loss_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('prem_refund_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('prem_refund_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('compens_payout_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('compens_payout_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('reser_insur_liab_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('reser_insur_liab_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('div_payt_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('div_payt_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('reins_exp_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('reins_exp_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('oper_exp_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('oper_exp_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('compens_payout_refu_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('compens_payout_refu_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('insur_reser_refu_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('insur_reser_refu_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('reins_cost_refund_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('reins_cost_refund_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('other_bus_cost_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('other_bus_cost_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('operate_profit_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('operate_profit_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('non_oper_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('non_oper_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('non_oper_exp_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('non_oper_exp_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('nca_disploss_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('nca_disploss_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('total_profit_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('total_profit_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('income_tax_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('income_tax_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('n_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('n_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('n_income_attr_p_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('n_income_attr_p_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('minority_gain_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('minority_gain_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('oth_compr_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('oth_compr_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('t_compr_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('t_compr_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('compr_inc_attr_p_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('compr_inc_attr_p_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('compr_inc_attr_m_s_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('compr_inc_attr_m_s_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('ebit_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('ebit_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('ebitda_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('ebitda_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('insurance_exp_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('insurance_exp_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('undist_profit_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('undist_profit_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('distable_profit_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('distable_profit_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('rd_exp_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('rd_exp_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('fin_exp_int_exp_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('fin_exp_int_exp_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('fin_exp_int_inc_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('fin_exp_int_inc_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('transfer_surplus_rese_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('transfer_surplus_rese_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('transfer_housing_imprest_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('transfer_housing_imprest_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('transfer_oth_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('transfer_oth_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('adj_lossgain_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('adj_lossgain_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('withdra_legal_surplus_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('withdra_legal_surplus_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('withdra_legal_pubfund_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('withdra_legal_pubfund_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('withdra_biz_devfund_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('withdra_biz_devfund_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('withdra_rese_fund_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('withdra_rese_fund_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('withdra_oth_ersu_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('withdra_oth_ersu_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('workers_welfare_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('workers_welfare_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('distr_profit_shrhder_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('distr_profit_shrhder_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('prfshare_payable_dvd_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('prfshare_payable_dvd_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('comshare_payable_dvd_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('comshare_payable_dvd_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('capit_comstock_div_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('capit_comstock_div_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('net_after_nr_lp_correct_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('net_after_nr_lp_correct_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('credit_impa_loss_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('credit_impa_loss_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('net_expo_hedging_benefits_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('net_expo_hedging_benefits_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('oth_impair_loss_assets_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('oth_impair_loss_assets_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('total_opcost_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('total_opcost_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('amodcost_fin_assets_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('amodcost_fin_assets_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('oth_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('oth_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('asset_disp_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('asset_disp_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('continued_net_profit_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('continued_net_profit_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('end_net_profit_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('end_net_profit_pct_yoy'))},
                NOW(),
                NOW()
            )
            """
        return sql

    def _format_sql_value(self, value):
        """格式化SQL值，处理负数值"""
        if value is None:
            return 'NULL'
        elif isinstance(value, (int, float)):
            # 对于负数值，确保前面有空格
            sql_value = str(value)
            if sql_value.startswith('-'):
                return ' ' + sql_value  # 在负号前加空格
            else:
                return sql_value
        else:
            return f"'{value}'"