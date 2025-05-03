import akshare as ak
from loguru import logger
import pandas as pd
from typing import Optional
from typing import Optional, Literal
from utils.config_loader import load_config

class AKShareClient:
    def __init__(self):
        self.config = load_config()
        
    def get_stock_data(
        self, 
        symbol: str,
        period: Literal['daily', 'weekly', 'monthly'] = 'daily',
        start_date: str = None,
        end_date: str = None,
        adjust: str = ""
    ) -> Optional[pd.DataFrame]:
        """
        获取股票历史数据(日/周/月)
        :param symbol: 股票代码 (格式: sh600000 或 sz000001)
        :param period: 周期类型 daily-日线 weekly-周线 monthly-月线
        :param start_date: 开始日期 (YYYYMMDD)
        :param end_date: 结束日期 (YYYYMMDD)
        :param adjust: 复权类型 ("qfq"-前复权, "hfq"-后复权, ""-不复权)
        :return: DataFrame 或 None
        """
        try:
            
            print(symbol+","+period+","+start_date+","+end_date+","+adjust)
            # 获取数据
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            
            # 统一字段命名
            df = df.rename(columns={
                "日期": "trade_date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "vol",
                "成交额": "amount",
                "换手率": "turnover_rate",
                "涨跌幅": "pct_chg",
                "涨跌额": "change"
            })
            
            # 添加必要字段
            df["stock_id"] = symbol
            # df["pct_chg"] = (df["close"] - df["close"].shift(1)) / df["close"].shift(1) * 100
            df["vol"] = df["vol"]*100
            
            
            # 去除首行(无涨跌幅数据)
            # if len(df) > 0:
            #     df = df.iloc[1:]
            # print(df)
            
            return df.reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"AKShare获取{period}数据失败: {e}")
            return None
    
    # 获取日线数据
    # symbol: 股票编号
    # start_date: 开始日期
    # end_date: 结束日期
    # adjust: 前复权 后复权 不复权
    # def get_daily_data(self, symbol: str, start_date: str, end_date: str, adjust: str = "") -> Optional[pd.DataFrame]:
    #     return self.get_stock_data(symbol, 'daily', start_date, end_date, adjust)
    
    @staticmethod
    def convert_to_db_format(akshare_data: pd.DataFrame,period:str) -> pd.DataFrame:
        if akshare_data.empty:
            return pd.DataFrame()
        """将AKShare数据转换为数据库表字段格式"""
        return pd.DataFrame({
            'stock_id': akshare_data['stock_id'], 
            'trade_date': (akshare_data['trade_date']),
            'amount': (akshare_data['amount'] ), 
            'volume': (akshare_data['vol']),    
            'start_price': akshare_data['open'],
            'end_price': akshare_data['close'],
            'high_price': akshare_data['high'],
            'low_price': akshare_data['low'],
            'change_price': akshare_data['change'],
            'change_percent': akshare_data['pct_chg'],
            'turnover_ratio': akshare_data['turnover_rate'],
            'scope_type': period,
            'create_user': 'system'
        })