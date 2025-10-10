# src/utils/date_utils.py
from datetime import datetime, timedelta
from typing import Union, Optional, Tuple
import pandas as pd
from utils.logger import log
import pytz
from datetime import datetime

class DateUtils:

    @staticmethod
    def prepare_timestamp(local_dt):
        """将本地时间转换为UTC时间"""
        # 确保输入是datetime对象
        if isinstance(local_dt, str):
            local_dt = pd.to_datetime(local_dt)
        
        # 标记为上海时区
        shanghai_tz = pytz.timezone('Asia/Shanghai')
        localized = shanghai_tz.localize(local_dt) if local_dt.tzinfo is None else local_dt
        
        # 转换为UTC
        utc_dt = localized.astimezone(pytz.UTC)
        return utc_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # 保留毫秒

    """日期时间处理工具集"""
    
    @staticmethod
    def convert_date_format(date_str: str, 
                          from_format: str = '%Y%m%d',
                          to_format: str = '%Y-%m-%d') -> str:
        """
        转换日期字符串格式
        :param date_str: 原始日期字符串
        :param from_format: 原始格式
        :param to_format: 目标格式
        :return: 格式化后的字符串
        """
        try:
            dt = datetime.strptime(date_str, from_format)
            return dt.strftime(to_format)
        except ValueError as e:
            log.error(f"日期格式转换失败: {date_str}, 错误: {e}")
            return date_str
    
    @staticmethod
    def get_trade_date_range(days: int = 30, 
                           end_date: Union[str, datetime, None] = None) -> Tuple[str, str]:
        """
        获取交易日期范围
        :param days: 天数跨度
        :param end_date: 结束日期(默认为今天)
        :return: (start_date, end_date) 格式: YYYYMMDD
        """
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y%m%d')
        end_date = end_date or datetime.now()
        
        start_date = end_date - timedelta(days=days)
        return start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')
    
    @staticmethod
    def is_weekend(date: Union[str, datetime]) -> bool:
        """判断是否为周末"""
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y%m%d')
        return date.weekday() >= 5
    
    @staticmethod
    def get_last_trade_day(date: Union[str, datetime, None] = None) -> str:
        """
        获取上一个交易日
        :param date: 基准日期(默认为今天)
        :return: 交易日字符串(YYYYMMDD)
        """
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y%m%d')
        date = date or datetime.now()
        
        delta = timedelta(days=1)
        while True:
            date -= delta
            if not DateUtils.is_weekend(date):
                return date.strftime('%Y%m%d')
    
    @staticmethod
    def convert_to_timestamp(date_str: str, 
                            format: str = '%Y%m%d') -> Optional[datetime]:
        """
        字符串转时间戳
        :param date_str: 日期字符串
        :param format: 日期格式
        :return: datetime对象或None
        """
        try:
            return datetime.strptime(date_str, format)
        except ValueError as e:
            log.error(f"日期转换失败: {date_str}, 错误: {e}")
            return None
    
    @staticmethod
    def pandas_date_to_str(date_series: pd.Series) -> pd.Series:
        """
        将Pandas日期序列转换为YYYYMMDD格式字符串
        :param date_series: 包含日期的Series
        :return: 字符串Series
        """
        return date_series.dt.strftime('%Y%m%d')

    
    @staticmethod
    def format_date_to_ymd(date_value):
        """将日期转为 %Y%m%d 格式"""
        if not date_value:
            return None
        
        # 如果是字符串，先转为datetime对象
        if isinstance(date_value, str):
            # 尝试不同的日期格式
            for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y%m%d', '%Y-%m-%d %H:%M:%S']:
                try:
                    date_obj = datetime.strptime(date_value, fmt)
                    return date_obj.strftime('%Y%m%d')
                except ValueError:
                    continue
            return None  # 无法解析的格式
        
        # 如果是datetime对象
        elif isinstance(date_value, (datetime, pd.Timestamp)):
            return date_value.strftime('%Y%m%d')
        
        # 其他类型
        else:
            try:
                return pd.to_datetime(date_value).strftime('%Y%m%d')
            except:
                return None

# 工具类实例导出
date_utils = DateUtils()

