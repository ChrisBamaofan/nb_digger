# src/database/tdengine_writer.py
from datetime import datetime
import pandas as pd
from typing import List, Dict
from .tdengine_connector import tdengine
from utils.logger import log
from utils.date_utils import date_utils

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
    def create_dynamic_table(db:str,company_id: str, location:str, scope:str ,table_name:str,stable:str ):
        """
        为指定公司创建动态日线表(如果不存在)
        :param company_id: 公司ID (如: 000001)
        """
        values = []
        values.append(location)
        values.append(company_id)
        values.append(scope)

        try:
            formatted_sql = f"""
            CREATE TABLE IF NOT EXISTS {db}.{table_name} USING {stable} TAGS (
                '{values[0]}', '{values[1]}', '{values[2]}'
            )
            """.strip().replace('\n', ' ')
            
            # log.info(f"Executing SQL:\n{formatted_sql}")
            
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


    
# TDEngineWriter.execute_bulk_price_adjustment('000858','week_000858',1,'20250711')