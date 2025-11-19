from database.tdengine_connector import tdengine
import pandas as pd
from datetime import datetime
from finance_report.finance_report_constant import FinanceReportConstant

class TDEngineTushareWriter:
    
    
    @staticmethod
    def insert_tushare(tushare_data, stock_id: str,numeric_fields:list,type:str):
        
        for _, row in tushare_data.iterrows():
            # print(row)
            # 处理NaN值为NULL
            row = row.where(pd.notna(row), None)
            
            # 转换时间戳
            report_date = row['end_date']
            report_date = datetime.strptime(report_date, '%Y%m%d').strftime('%Y-%m-%d')
            utc_ts = tdengine._convert_to_utc2(report_date)
            
            # 构建值列表
            values = []
            # 添加时间戳和股票ID '{row['ann_date'] or ''}',
            values.append(f"'{utc_ts}'")
            values.append(f"'{stock_id}'")
            values.append(f"'{row['ann_date'] or ''}'")
            values.append(f"'{row['f_ann_date']  or ''}'")
            values.append(f"'{row['end_date']  or ''}'")
            values.append(f"'{row['comp_type']  or ''}'")
            values.append(f"'{row['report_type']  or ''}'")
            values.append(f"'{row['end_type']  or ''}'")
            
            # 添加所有资产负债表字段
            for field in numeric_fields:
                values.append(tdengine._format_sql_value(row[field]))
            
            # 添加更新时间标记
            values.append(f"'{row['update_flag'] or ''}'")
            
            # 添加创建和更新时间
            values.append("NOW()")
            values.append("NOW()")
            
            # 构建SQL
            sql = f"""
                INSERT INTO {type}_tsh_{stock_id}
                VALUES ({', '.join(values)})
                """
            
            tdengine.execute(sql)
            
   