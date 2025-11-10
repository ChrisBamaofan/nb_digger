
from datetime import datetime
from typing import Dict, List, Optional
from utils.logger import setup_logger,log
from database.tdengine_connector import tdengine

class FinanceReportTushare:

    def __init__(self):
        pass
    
    # debug *****
    def find_comparable_report(reports: List[Dict], current_report: Dict, current_index: int) -> Optional[Dict]:
        """
        找到可比较的上一期报告（去年同期）
        
        Args:
            reports: 排序后的报告列表
            current_report: 当前报告
            current_index: 当前报告在列表中的索引
            
        Returns:
            Optional[Dict]: 可比较的上一期报告，如果没有返回None
        """
        try:
            current_end_date = current_report.get('end_date')
            if not current_end_date:
                return None
            
            current_date = datetime.strptime(current_end_date, '%Y%m%d')
            
            # 寻找去年同期报告（去年同季度）
            for i in range(current_index - 1, -1, -1):
                report = reports[i]
                report_end_date = report.get('end_date')
                if not report_end_date:
                    continue
                    
                report_date = datetime.strptime(report_end_date, '%Y%m%d')
                
                # 检查是否是去年同期（年份差1，月份和季度相同）
                if (current_date.year - report_date.year == 1 and 
                    current_date.month == report_date.month):
                    return report
            
            # 如果没有找到完全匹配的去年同期，找最接近的上一期报告
            if current_index > 0:
                return None
                
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
            # 计算同比变化
            yoy_data = self.calculate_income_yoy(current_report, previous_report)
            
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
            
    def calculate_income_yoy(self,current_report: Dict, previous_report: Dict) -> Dict:
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
        
        # 定义需要计算同比的数值字段
        numeric_fields = [
            'total_revenue', 'revenue', 'n_income', 'n_income_attr_p', 
            'operate_profit', 'total_profit', 'basic_eps', 'diluted_eps',
            'oper_cost', 'sell_exp', 'admin_exp', 'fin_exp', 'rd_exp'
        ]
        
        for field in numeric_fields:
            current_value = current_report.get(field)
            previous_value = previous_report.get(field)
            
            # 计算绝对值变化
            abs_change = self.calculate_change(current_value, previous_value, 'abs')
            # 计算百分比变化
            pct_change = self.calculate_change(current_value, previous_value, 'pct')
            
            yoy_data[f'{field}_abs_yoy'] = abs_change
            yoy_data[f'{field}_pct_yoy'] = pct_change
        
        return yoy_data
    
    def calculate_change(current_value, previous_value, change_type: str):
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
        ts = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]} 00:00:00.000"
        
        sql = f"""
            INSERT INTO is_yoy_tsh_{stock_id} VALUES (
                '{ts}',
                '{yoy_data['ts_code'] or ''}',
                '{yoy_data['end_date'] or ''}',
                '{yoy_data['report_type'] or ''}',
                {self._format_sql_value(yoy_data.get('total_revenue_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('total_revenue_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('revenue_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('revenue_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('n_income_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('n_income_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('n_income_attr_p_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('n_income_attr_p_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('operate_profit_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('operate_profit_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('total_profit_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('total_profit_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('basic_eps_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('basic_eps_pct_yoy'))},
                {self._format_sql_value(yoy_data.get('diluted_eps_abs_yoy'))},
                {self._format_sql_value(yoy_data.get('diluted_eps_pct_yoy'))},
                {self._format_sql_value(current_report.get('total_revenue'))},
                {self._format_sql_value(current_report.get('revenue'))},
                {self._format_sql_value(current_report.get('n_income'))},
                {self._format_sql_value(current_report.get('n_income_attr_p'))},
                NOW(),
                NOW()
            )
            """
        return sql

    def _format_sql_value(value):
        """格式化SQL值"""
        if value is None or pd.isna(value):
            return 'NULL'
        if isinstance(value, str):
            return f"'{value}'"
        return str(value)