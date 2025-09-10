import akshare as ak
from loguru import logger
import pandas as pd
from typing import List, Optional
from typing import Optional, Literal
import pymysql
from datetime import datetime
from utils.config_loader import load_config
import time

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
            
            logger.info(symbol+","+period+","+start_date+","+end_date+","+adjust)
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
            logger.error(f"AKShare获取{symbol}{period}数据失败: {e}")
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
    

    @staticmethod
    def check_delisted_stocks( stock_ids: List[str]) -> List[str]:
        """
        1 获取当前所有的活跃股票
        2 
        """
        delisted_stocks = []
        
        for stock_id in stock_ids:
            try:
                stock_info = ak.stock_individual_info_em(symbol=stock_id)
                total_share = stock_info.loc[stock_info["item"] == "总股本", "value"].values[0]
                
                if total_share == "-":
                    delisted_stocks.append(stock_id)
                    
                    print(f"检测到退市股票: {stock_id}")
            except Exception as e:
                print(f"检查股票 {stock_id} 时出错: {e}")
        
        return delisted_stocks
    
    def check_new_stocks(stock_ids: List[str]) -> List[str]:
        new_stocks = []
        try:
            stock_list = ak.stock_info_a_code_name()
            all_active_stocks = stock_list['code'].tolist()
            
            print(f"数据库中已有股票数量: {len(stock_ids)}")
            print(f"当前活跃股票数量: {len(all_active_stocks)}")
            
            for stock_code in all_active_stocks:
                if stock_code not in stock_ids:
                    try:
                        stock_info = ak.stock_individual_info_em(symbol=stock_code)
                        if not stock_info.empty:
                            new_stocks.append(stock_code)
                            print(f"发现新股票: {stock_code}")
                    except Exception as e:
                        print(f"验证股票 {stock_code} 时出错: {e}")
        
        except Exception as e:
            print(f"获取股票列表时出错: {e}")
            return []
        
        print(f"发现 {len(new_stocks)} 只新股票")
        return new_stocks


    def get_stock_basic(self,stock_id:str) ->  Optional[pd.DataFrame]:
        df = ak.stock_individual_info_em(symbol=stock_id)
        
        df[df['item'] == '股票代码']['value'].iloc[0]
        metrics = {
            'stock_id': stock_id,
            'total_stock': float(df[df['item'] == '总股本']['value'].iloc[0]),
            'circulating_stock': float(df[df['item'] == '流通股']['value'].iloc[0]),
            'total_market_value': float(df[df['item'] == '总市值']['value'].iloc[0]),
            'circulating_market_value': float(df[df['item'] == '流通市值']['value'].iloc[0]),
            'industry': df[df['item'] == '行业']['value'].iloc[0],
            'launch_date': df[df['item'] == '上市时间']['value'].iloc[0]
        }
        
        return metrics
    
    def get_realtime_data(self,stock_id:str) -> float:
        df = ak.stock_bid_ask_em(symbol=stock_id)
        va = df[df['item'] == '最新']['value'].values[0]
        return va

    def getHold(self,stock_id:str) :
        return ak.stock_ggcg_em(symbol="股东减持")
    

