import baostock as bs
import pandas as pd
from typing import Optional
import logging

logger = logging.getLogger(__name__)

LOCATION_TO_BS_PREFIX = {
    'china.shanghai': 'sh',
    'china.shenzhen': 'sz',
}


class BaostockClient:

    def __init__(self):
        self._logged_in = False

    def login(self):
        if not self._logged_in:
            lg = bs.login()
            if lg.error_code != '0':
                logger.error(f"Baostock 登录失败: {lg.error_msg}")
                return False
            self._logged_in = True
        return True

    def logout(self):
        if self._logged_in:
            bs.logout()
            self._logged_in = False

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logout()
        return False

    @staticmethod
    def convert_stock_id(stock_id: str, location: str) -> Optional[str]:
        prefix = LOCATION_TO_BS_PREFIX.get(location)
        if prefix is None:
            logger.warning(f"Baostock 不支持该市场: {location} (stock_id={stock_id})")
            return None
        return f"{prefix}.{stock_id}"

    def get_weekly_data(
        self,
        stock_id: str,
        location: str,
        start_date: str,
        end_date: str,
        adjust: str = "3",
    ) -> Optional[pd.DataFrame]:
        """
        获取周线数据
        :param stock_id: 股票代码，如 '600479'
        :param location: 市场，如 'china.shanghai'
        :param start_date: 开始日期 YYYYMMDD
        :param end_date:   结束日期 YYYYMMDD
        :param adjust: '3'=不复权(默认), '2'=前复权, '1'=后复权
        """
        if not self._logged_in and not self.login():
            return None

        bs_code = self.convert_stock_id(stock_id, location)
        if bs_code is None:
            return None

        bs_start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        bs_end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"

        fields = "date,code,open,high,low,close,volume,amount,turn,pctChg"

        rs = bs.query_history_k_data_plus(
            bs_code,
            fields,
            start_date=bs_start,
            end_date=bs_end,
            frequency="w",
            adjustflag=adjust,
        )

        if rs.error_code != '0':
            logger.error(f"Baostock 查询失败 ({bs_code}): {rs.error_msg}")
            return None

        rows = []
        while (rs.error_code == '0') and rs.next():
            rows.append(rs.get_row_data())

        if not rows:
            logger.warning(f"Baostock 未获取到 {bs_code} 的周线数据")
            return None

        df = pd.DataFrame(rows, columns=rs.fields)

        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn', 'pctChg']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        df = df.rename(columns={
            'date': 'trade_date',
            'volume': 'vol',
            'turn': 'turnover_rate',
            'pctChg': 'pct_chg',
        })

        df['trade_date'] = df['trade_date'].str.replace('-', '')
        df['change'] = 0
        df['stock_id'] = bs_code

        keep_cols = [
            'trade_date', 'open', 'high', 'low', 'close',
            'vol', 'amount', 'change', 'pct_chg', 'turnover_rate', 'stock_id',
        ]
        return df[keep_cols].reset_index(drop=True)

    def test_weekly_data(self, stock_id: str = '600479', location: str = 'china.shanghai'):
        """测试方法：获取指定股票的最近周线数据并打印"""
        start_date = '20260401'
        end_date = '20260404'
        with BaostockClient() as client:
            df = client.get_weekly_data(stock_id, location, start_date, end_date)
        if df is not None:
            print(f"=== Baostock 周线数据 ({stock_id}) ===")
            with pd.option_context('display.max_columns', None, 'display.width', 200):
                print(df)
        else:
            print(f"未获取到 {stock_id} 的数据")
        return df
