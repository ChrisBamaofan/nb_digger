# src/database/tdengine_connector.py
import taosrest
from typing import Dict, List, Optional, Tuple
from utils.logger import log
from utils.config_loader import load_config
from taosrest import TaosRestConnection
from datetime import datetime, timedelta
import pytz

class TDEngineConnector:
    def __init__(self):
        self._conn = None
        self.config = load_config().get('tdengine', {})
        self.tz_shanghai = pytz.timezone('Asia/Shanghai')
        
    def _convert_to_utc(self, dt_str):
        """将本地时间字符串转换为UTC时间字符串（TDengine存储的是UTC时间）"""
        dt = datetime.strptime(dt_str, '%Y%m%d')
        dt_shanghai = self.tz_shanghai.localize(dt)
        dt_utc = dt_shanghai.astimezone(pytz.utc)
        return dt_utc.strftime('%Y-%m-%d %H:%M:%S')
    
    def _convert_to_utc2(self, time_input):
            """
            通用时间转换方法，支持多种输入类型：
            - datetime 对象
            - 时间戳（毫秒/秒）
            - 字符串（自动解析格式）
            - TDengine 原生时间类型
            """
            from datetime import datetime
            import pytz

            # 处理TDengine的Native类型
            if hasattr(time_input, 'to_datetime'):
                dt = time_input.to_datetime()
            elif isinstance(time_input, (int, float)):
                # 处理时间戳（假设是毫秒）
                dt = datetime.fromtimestamp(time_input/1000, pytz.UTC)
            elif isinstance(time_input, datetime):
                dt = time_input
            else:
                # 尝试字符串解析
                try:
                    dt = datetime.strptime(str(time_input), '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    dt = datetime.strptime(str(time_input), '%Y-%m-%d')

            # 本地化并转换时区
            if not dt.tzinfo:
                dt = pytz.timezone('Asia/Shanghai').localize(dt)
            return dt.astimezone(pytz.UTC)
    @property
    def conn(self) -> TaosRestConnection:
        """获取TDEngine连接(单例模式)"""
        if self._conn is None:
            try:
                self._conn = taosrest.connect(
                    url=self.config.get('url', 'localhost'),
                    user=self.config.get('user', 'root'),
                    password=self.config.get('password', 'taosdata'),
                    database=self.config.get('database','nb_stock'),
                    timezone='Asia/Shanghai',
                    timeout=30
                )
                # log.success("TDEngine连接成功")
            except Exception as e:
                log.error(f"TDEngine连接失败: {e}")
                raise
        return self._conn
    
    def close(self):
        """关闭连接"""
        if self._conn:
            self._conn.close()
            self._conn = None
            log.info("TDEngine连接已关闭")
    
    def execute(self, sql: str):
        """执行SQL语句"""
        try:
            self.conn.execute(sql)
        except Exception as e:
            log.error(f"TDEngine执行失败: {sql[:100]}... 错误: {e}")
            raise
    
    def __del__(self):
        self.close()
        
    @staticmethod
    def _format_sql_value(value):
        """
        格式化SQL值，处理NaN、None、无穷大等特殊值
        
        Args:
            value: 需要格式化的值，可以是数字、字符串、None、NaN等
            
        Returns:
            str: 格式化后的SQL值，可以直接用于INSERT语句
        """
        # 处理None值
        if value is None:
            return 'NULL'
        
        # 处理pandas的NaN值
        if pd.isna(value):
            return 'NULL'
        
        # 处理numpy的NaN值
        try:
            import numpy as np
            if np.isnan(value):
                return 'NULL'
        except (ImportError, TypeError):
            pass
        
        # 处理无穷大值
        try:
            if float(value) == float('inf'):
                return 'NULL'
            if float(value) == float('-inf'):
                return 'NULL'
        except (ValueError, TypeError):
            pass
        
        # 处理字符串类型 - 需要转义单引号
        if isinstance(value, str):
            # 转义单引号（SQL注入防护）
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        
        # 处理布尔值
        if isinstance(value, bool):
            return '1' if value else '0'
        
        # 处理整数和浮点数
        if isinstance(value, (int, float)):
            # 确保数值在合理范围内
            try:
                float_value = float(value)
                # 检查是否为有限数（非NaN、非无穷大）
                if not np.isfinite(float_value):
                    return 'NULL'
                return str(float_value)
            except (ValueError, TypeError):
                return 'NULL'
        
        # 处理datetime对象
        if isinstance(value, (datetime, pd.Timestamp)):
            try:
                # 转换为TDengine支持的格式
                return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
            except:
                return 'NULL'
        
        # 处理其他类型（如列表、字典等）- 转换为字符串
        try:
            str_value = str(value)
            escaped_value = str_value.replace("'", "''")
            return f"'{escaped_value}'"
        except:
            return 'NULL'

# 单例实例
tdengine = TDEngineConnector()



