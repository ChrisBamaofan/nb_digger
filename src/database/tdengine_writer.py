# src/database/tdengine_writer.py
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