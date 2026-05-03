"""
多周期行情数据采集器
支持: day / 60min / month / quarter / year
数据同时写入 MySQL (stock_per_day_final) 和 TDEngine (stock_trade_history)
增量模式: 查询已有最新日期, 只拉取新数据
"""
import time
import pandas as pd
from datetime import date, datetime, timedelta

from database.db_manager import DBManager
from datasource.tushare_client import TushareService
from database.tdengine_writer import TDEngineWriter
from utils.logger import setup_logger, log

SCOPES_ALL = ['day', 'month', 'quarter', 'year', '60min']

TUSHARE_SCOPE_MAP = {
    'day': 'daily',
    'month': 'monthly',
}

RATE_LIMIT_SEC = 0.35

SEGMENT_YEARS = 10


def _ts_code(stock_id: str, location: str) -> str:
    loc_map = {'china.shenzhen': 'SZ', 'china.shanghai': 'SH', 'china.beijing': 'BJ'}
    return f"{stock_id}.{loc_map.get(location, 'SH')}"


def _next_day(dt) -> str:
    """dt (datetime / str) -> 下一天 YYYYMMDD"""
    if isinstance(dt, str):
        dt = pd.to_datetime(dt)
    return (dt + timedelta(days=1)).strftime('%Y%m%d')


def _launch_date_str(launch_date) -> str:
    """将 launch_date (datetime/str/None) 转为 YYYYMMDD，兜底 19900101"""
    if launch_date is None:
        return '19900101'
    if isinstance(launch_date, str):
        return launch_date.replace('-', '')[:8]
    return pd.to_datetime(launch_date).strftime('%Y%m%d')


def _save(db_manager: DBManager, db_data: pd.DataFrame, stock_id: str,
          location: str, scope: str):
    """同时写入 MySQL + TDEngine"""
    if db_data.empty:
        return
    db_manager.save_daily_data(db_data)

    table_name_td = f"{scope}_{stock_id}"
    TDEngineWriter.create_dynamic_table(
        "nb_stock", stock_id, location, scope,
        table_name_td, "stock_trade_history", False, "RMB")
    TDEngineWriter.write_data_batch(
        data=db_data, company_id=stock_id, table_name=table_name_td)


def _fetch_and_save_tushare(ts: TushareService, db_manager: DBManager,
                            stock_id: str, location: str, scope: str,
                            start_date: str, end_date: str):
    """
    从 Tushare 拉取 day / month 数据并存储.
    pro_bar 单次最多 ~5000 行, 按 SEGMENT_YEARS 年分段循环拉取.
    """
    symbol = _ts_code(stock_id, location)
    period = TUSHARE_SCOPE_MAP[scope]
    total = 0
    cur_start = pd.to_datetime(start_date)
    dt_end = pd.to_datetime(end_date)

    while cur_start <= dt_end:
        seg_end = min(cur_start + pd.DateOffset(years=SEGMENT_YEARS) - timedelta(days=1), dt_end)
        s_str = cur_start.strftime('%Y%m%d')
        e_str = seg_end.strftime('%Y%m%d')

        raw = ts.get_adj_stock_data(
            symbol=symbol, period=period,
            start_date=s_str, end_date=e_str,
        )
        if raw is not None and not raw.empty:
            db_data = DBManager.convert_to_db_format_tushare(stock_id, raw, scope)
            _save(db_manager, db_data, stock_id, location, scope)
            total += len(db_data)

        cur_start = seg_end + timedelta(days=1)
        time.sleep(RATE_LIMIT_SEC)

    return total


def _fetch_and_save_60min(ts: TushareService, db_manager: DBManager,
                          stock_id: str, location: str,
                          start_date: str, end_date: str):
    """
    拉取 60min 数据, stk_mins 单次最多 8000 行 (~33 个交易日).
    按 30 天分段循环拉取.
    """
    ts_code = _ts_code(stock_id, location)
    scope = '60min'
    total = 0
    cur_start = pd.to_datetime(start_date)
    dt_end = pd.to_datetime(end_date)

    while cur_start <= dt_end:
        seg_end = min(cur_start + timedelta(days=29), dt_end)
        s_str = cur_start.strftime('%Y%m%d')
        e_str = seg_end.strftime('%Y%m%d')

        raw = ts.get_minute_data(
            symbol=ts_code, freq='60min',
            start_date=s_str, end_date=e_str,
        )
        if raw is not None and not raw.empty:
            db_data = DBManager.convert_to_db_format_tushare(stock_id, raw, scope)
            _save(db_manager, db_data, stock_id, location, scope)
            total += len(db_data)

        cur_start = seg_end + timedelta(days=1)
        time.sleep(RATE_LIMIT_SEC)

    return total


def _calc_quarter_year(db_manager: DBManager,
                       stock_id: str, location: str, scope: str,
                       start_date: str, end_date: str):
    """
    从 MySQL 已有的日线数据聚合成季度 / 年度, 再写入.
    不再调用 Tushare API, 速度大幅提升.
    scope: 'quarter' 或 'year'
    """
    raw = db_manager.get_daily_data_from_db(stock_id, start_date, end_date)
    if raw.empty:
        return 0

    raw['trade_date'] = pd.to_datetime(raw['trade_date'])

    if scope == 'quarter':
        raw['group'] = raw['trade_date'].dt.to_period('Q')
    else:
        raw['group'] = raw['trade_date'].dt.to_period('Y')

    rows = []
    for grp, df in raw.groupby('group'):
        df = df.sort_values('trade_date')
        rows.append({
            'trade_date': df['trade_date'].iloc[-1].strftime('%Y%m%d'),
            'open': df['open'].iloc[0],
            'close': df['close'].iloc[-1],
            'high': df['high'].max(),
            'low': df['low'].min(),
            'vol': df['vol'].sum(),
            'amount': df['amount'].sum() if 'amount' in df.columns else 0,
            'change': df['close'].iloc[-1] - df['open'].iloc[0],
            'pct_chg': round((df['close'].iloc[-1] - df['open'].iloc[0]) / df['open'].iloc[0] * 100, 4) if df['open'].iloc[0] != 0 else 0,
            'turnover_rate': df['turnover_rate'].sum() if 'turnover_rate' in df.columns else 0,
        })

    if not rows:
        return 0

    agg = pd.DataFrame(rows)
    db_data = DBManager.convert_to_db_format_tushare(stock_id, agg, scope)
    _save(db_manager, db_data, stock_id, location, scope)
    return len(db_data)


def dig_market_data(scopes=None):
    """
    主入口: 采集多周期行情数据 (增量).
    :param scopes: 要处理的周期列表, 默认全部
    """
    setup_logger()
    scopes = scopes or SCOPES_ALL
    log.info(f"========== 多周期行情采集 开始 scopes={scopes} ==========")

    ts = TushareService()
    db_manager = DBManager()
    today = date.today().strftime('%Y%m%d')

    stock_list = db_manager.get_stock_id_list()
    log.info(f"待处理股票数量: {len(stock_list)}")

    for scope in scopes:
        log.info(f"------ 处理周期: {scope} ------")
        processed = 0
        skipped = 0

        for stock in stock_list:
            stock_id = stock.stock_id
            location = stock.location
            ipo_date = _launch_date_str(stock.launch_date)

            latest = db_manager.get_latest_trade_date_by_scope(stock_id, scope)
            if latest:
                start = _next_day(latest)
            else:
                start = ipo_date

            if start > today:
                skipped += 1
                continue

            try:
                if scope in ('day', 'month'):
                    time.sleep(RATE_LIMIT_SEC)
                    n = _fetch_and_save_tushare(
                        ts, db_manager, stock_id, location, scope, start, today)
                elif scope == '60min':
                    time.sleep(RATE_LIMIT_SEC)
                    n = _fetch_and_save_60min(
                        ts, db_manager, stock_id, location, start, today)
                elif scope in ('quarter', 'year'):
                    qy_start = start if latest else ipo_date
                    n = _calc_quarter_year(
                        db_manager, stock_id, location, scope, qy_start, today)
                else:
                    log.warning(f"未知scope: {scope}")
                    continue

                if n > 0:
                    log.success(f"[{scope}] {stock_id} 写入 {n} 条")
                processed += 1
            except Exception as e:
                log.error(f"[{scope}] {stock_id} 失败: {e}")
                continue

        log.info(f"[{scope}] 完成: 处理={processed} 跳过={skipped}")

    log.info("========== 多周期行情采集 结束 ==========")


if __name__ == '__main__':
    dig_market_data()
