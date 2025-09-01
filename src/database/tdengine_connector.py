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

# 单例实例
tdengine = TDEngineConnector()


