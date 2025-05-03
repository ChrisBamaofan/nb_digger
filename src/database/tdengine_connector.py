# src/database/tdengine_connector.py
import taosrest
from typing import Dict, List, Optional, Tuple
from utils.logger import log
from utils.config_loader import load_config
from taosrest import TaosRestConnection

class TDEngineConnector:
    def __init__(self):
        self._conn = None
        self.config = load_config().get('tdengine', {})
        
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
                log.success("TDEngine连接成功")
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


