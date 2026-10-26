# stock_list = ['871694', '871553', '839273', '838971', '838171', '830779', '688582', '688510', '688486', '688368', '688307', '688200', '688195', '688123', '688100', '688093', '605499', '605368', '605218', '605077', '605028', '603949', '603866', '603809', '603697', '603682', '603677', '603223', '603199', '603162', '603031', '601988', '601886', '601882', '601658', '601328', '600976', '600965', '600711', '600605', '600502', '600211', '600113', '301389', '301362', '301280', '301277', '301187', '301127', '301113', '301069', '301066', '301016', '301004', '300999', '300997', '300969', '300960', '300926', '300918', '300896', '300842', '300824', '300801', '300783', '300782', '300750', '300693', '300562', '300476', '300458', '300441', '300406', '300403', '300401', '300373', '300304', '300146', '300102', '300059', '300047', '300018', '003022', '003006', '003000', '002972', '002967', '002920', '002911', '002879', '002876', '002870', '002852', '002807', '002774', '002749', '002692', '002661', '002658', '002600', '002516', '002469', '002467', '002455', '002404', '002393', '002320', '002318', '002315', '002282', '002236', '002233', '002185', '002145', '002054', '002014', '001376', '001368', '001358', '001299', '000989', '000895', '000823', '000538', '000063', '000062', '000048']
# stock_list = ['300801', '300783', '300782', '300750', '300693', '300562', '300476', '300458', '300441', '300406', '300403', '300401', '300373', '300304', '300146', '300102', '300059', '300047', '300018', '003022', '003006', '003000', '002972', '002967', '002920', '002911', '002879', '002876', '002870', '002852', '002807', '002774', '002749', '002692', '002661', '002658', '002600', '002516', '002469', '002467', '002455', '002404', '002393', '002320', '002318', '002315', '002282', '002236', '002233', '002185', '002145', '002054', '002014', '001376', '001368', '001358', '001299', '000989', '000895', '000823', '000538', '000063', '000062', '000048']
from datasource.tushare_client import TushareService
from database.db_manager import DBManager
from database.models import StockDaily
from utils.logger import setup_logger,log
from datetime import datetime, timedelta,date
import pandas as pd
from database.tdengine_writer import TDEngineWriter
import time
import numpy as np
from decimal import Decimal
import csv
from io import StringIO

# ── 字段映射：Tushare 列 → 本地 DB 列 ──
_FIELD_MAP = {
    'open':  'start_price',
    'close': 'end_price',
    'high':  'high_price',
    'low':   'low_price',
    'vol':   'volume',
    'amount':'amount',
}

# ── Tushare scope 配置（day/week/month 拉取口径见 TushareService.fetch_qfq_by_scope）──
_SCOPES_CONFIG = [
    {'scope': 'day'},
    {'scope': 'week'},
    {'scope': 'month'},
]

def validate_data_consistency(
    start_date: str = None,
    end_date: str = None,
    scopes: list = None,
    stock_list: list = None,
    fields: list = None,
    diff_threshold: float = 0.01,
    output_csv: str = None,
):
    """
    校验本地行情数据与 Tushare qfq 数据的一致性。

    :param start_date:    起始日期 YYYYMMDD，默认当前日期往前 6 个月
    :param end_date:      结束日期 YYYYMMDD，默认今天
    :param scopes:        要校验的 scope 列表，默认 ['day','week','month']
    :param stock_list:    要校验的股票代码列表 (含 location)，默认全部未退市股票
    :param fields:        要校验的字段列表，默认 ['close']
    :param diff_threshold: 差异阈值（元），超过此值视为不一致
    :param output_csv:    可选，输出 CSV 文件路径
    :returns:             按 scope 分组的校验结果汇总
    """
    setup_logger()

    # ── 默认参数 ──
    if start_date is None:
        start_date = (date.today() - timedelta(days=180)).strftime('%Y%m%d')
    if end_date is None:
        end_date = date.today().strftime('%Y%m%d')
    if scopes is None:
        scopes = ['day', 'week', 'month']
    if fields is None:
        fields = ['close']

    ts_service = TushareService()
    db_manager = DBManager()

    if stock_list is None:
        stock_list = db_manager.get_stock_id_list()
        log.info(f"校验日期范围: {start_date} ~ {end_date}")
        log.info(f"校验 scope: {scopes}")
        log.info(f"校验字段: {fields}")
        log.info(f"校验股票数量: {len(stock_list)}")

    # ── 结果容器 ──
    # summary[scope] = {'total': n, 'ok': n, 'diff': n, 'no_ts': n, 'no_local': n}
    # field_diff[scope][field] = []  → 每条差异记录
    # stock_summary[scope] = {stock_id: diff_count}

    summary = {}
    field_diff_count = {}   # scope → field → count
    field_diff_buckets = {} # scope → field → { '0.01-0.05': n, ... }
    stock_summary = {}      # scope → { stock_id: {'diff_dates': n, 'diff_count': n} }
    all_diff_details = []   # 详细差异记录 (用于 CSV 输出)

    # 过滤 scope 配置
    active_scopes = [c for c in _SCOPES_CONFIG if c['scope'] in scopes]

    for scope_cfg in active_scopes:
        scope = scope_cfg['scope']
        summary[scope] = {'total': 0, 'ok': 0, 'diff': 0, 'no_ts': 0, 'no_local': 0}
        field_diff_count[scope] = {f: 0 for f in fields}
        field_diff_buckets[scope] = {f: {
            '0.01-0.05': 0, '0.05-0.10': 0, '0.10-0.50': 0, '0.50+': 0
        } for f in fields}
        stock_summary[scope] = {}

    for stock in stock_list:
        time.sleep(0.3)
        stock_id = stock.stock_id

        if not hasattr(stock, 'location') or stock.location is None:
            continue

        ts_symbol = TushareService.convert_stock_id(stock_id, stock.location)

        for scope_cfg in active_scopes:
            scope = scope_cfg['scope']
            summary[scope]['total'] += 1

            # ── 1) 拉 Tushare qfq 基准数据（口径与 update_basic_get_stock 一致）──
            ts_df = ts_service.fetch_qfq_by_scope(
                symbol=ts_symbol, scope=scope,
                start_date=start_date, end_date=end_date,
            )

            if ts_df is None or ts_df.empty:
                summary[scope]['no_ts'] += 1
                continue

            ts_df = ts_df.sort_values('trade_date')
            ts_df['trade_date_str'] = pd.to_datetime(ts_df['trade_date']).dt.strftime('%Y%m%d')

            # ── 2) 读本地数据 ──
            db_records = db_manager.get_stock_per_day_list(stock_id, scope, end_date)
            if not db_records:
                summary[scope]['no_local'] += 1
                continue

            # 构建本地 {日期: {字段: 值}} 字典
            local_dict = {}
            for r in db_records:
                td = r.trade_date.strftime('%Y%m%d')
                local_dict[td] = {
                    'start_price': float(r.start_price),
                    'end_price':   float(r.end_price),
                    'high_price':  float(r.high_price),
                    'low_price':   float(r.low_price),
                    'volume':      int(r.volume),
                    'amount':      int(r.amount),
                }

            # ── 3) 逐日期逐字段比对 ──
            stock_has_diff = False
            stock_diff_date_count = 0
            stock_diff_total_count = 0

            for _, row in ts_df.iterrows():
                td = row['trade_date_str']
                if td not in local_dict:
                    continue

                for field in fields:
                    ts_field = field
                    db_field = _FIELD_MAP[field]
                    ts_val = float(row[ts_field])
                    db_val = local_dict[td][db_field]

                    if field in ('vol', 'volume'):
                        # vol 差异用比例判断（成交量相对误差 < 1%）
                        diff = abs(ts_val - db_val) / max(db_val, 1)
                        if diff > 0.01:
                            stock_has_diff = True
                            stock_diff_total_count += 1
                            field_diff_count[scope][field] += 1
                            all_diff_details.append({
                                'stock_id': stock_id, 'scope': scope,
                                'date': td, 'field': field,
                                'ts_value': ts_val, 'db_value': db_val,
                                'diff': f'{diff*100:.1f}%'
                            })
                    elif field == 'amount':
                        diff = abs(ts_val - db_val) / max(db_val, 1)
                        if diff > 0.05:
                            stock_has_diff = True
                            stock_diff_total_count += 1
                            field_diff_count[scope][field] += 1
                            all_diff_details.append({
                                'stock_id': stock_id, 'scope': scope,
                                'date': td, 'field': field,
                                'ts_value': ts_val, 'db_value': db_val,
                                'diff': f'{diff*100:.1f}%'
                            })
                    else:
                        # 价格字段：两位小数量化后比较，避免 float 误差
                        ts_val_q = DBManager.quantize_price(row[ts_field])
                        db_val_q = DBManager.quantize_price(db_val)
                        if ts_val_q is None or db_val_q is None:
                            continue
                        abs_diff = abs(ts_val_q - db_val_q)
                        if abs_diff >= Decimal(str(diff_threshold)):
                            stock_has_diff = True
                            stock_diff_total_count += 1
                            field_diff_count[scope][field] += 1

                            # 分桶
                            bs = field_diff_buckets[scope][field]
                            if abs_diff <= 0.05:
                                bs['0.01-0.05'] += 1
                            elif abs_diff <= 0.10:
                                bs['0.05-0.10'] += 1
                            elif abs_diff <= 0.50:
                                bs['0.10-0.50'] += 1
                            else:
                                bs['0.50+'] += 1

                            all_diff_details.append({
                                'stock_id': stock_id, 'scope': scope,
                                'date': td, 'field': field,
                                'ts_value': float(ts_val_q),
                                'db_value': float(db_val_q),
                                'diff': float(ts_val_q - db_val_q),
                            })

            if stock_has_diff:
                summary[scope]['diff'] += 1
                stock_summary[scope][stock_id] = {
                    'diff_dates': stock_diff_date_count,
                    'diff_count': stock_diff_total_count,
                }
            else:
                summary[scope]['ok'] += 1

    # ── 4) 输出报告 ──
    _print_consistency_report(summary, field_diff_count, field_diff_buckets,
                               stock_summary, scopes, fields, start_date, end_date)

    # ── 5) 可选输出 CSV ──
    if output_csv and all_diff_details:
        _write_diff_csv(all_diff_details, output_csv)
        log.info(f"差异明细已导出: {output_csv}")

    return {
        'summary': summary,
        'field_diff_count': field_diff_count,
        'stock_summary': stock_summary,
        'diff_details': all_diff_details,
    }


def _print_consistency_report(summary, field_diff_count, field_diff_buckets,
                               stock_summary, scopes, fields, start_date, end_date):
    """输出分级一致性报告"""
    total_stocks = sum(s['total'] for s in summary.values())

    log.info("")
    log.info("=" * 60)
    log.info("        行情数据一致性校验报告")
    log.info("=" * 60)
    log.info(f"  校验时间: {date.today().strftime('%Y-%m-%d')}")
    log.info(f"  时间范围: {start_date} ~ {end_date}")
    log.info(f"  校验字段: {fields}")
    log.info("")

    for scope in scopes:
        if scope not in summary:
            continue
        s = summary[scope]
        valid = s['total'] - s['no_ts'] - s['no_local']
        log.info(f"┌{'─' * 50}┐")
        log.info(f"│  scope = {scope:<6}                                              │")
        log.info(f"├{'─' * 50}┤")
        log.info(f"│  校验股票:     {s['total']:>6}                                     │")
        log.info(f"│  无 Tushare 数据: {s['no_ts']:>4}                                 │")
        log.info(f"│  无本地数据:     {s['no_local']:>4}                                │")
        log.info(f"│  有效对比:       {valid:>6}                                     │")
        log.info(f"│  完全一致:       {s['ok']:>6}  ({_pct(s['ok'], valid):.1f}%)                        │")
        log.info(f"│  有差异:         {s['diff']:>6}  ({_pct(s['diff'], valid):.1f}%)                        │")
        log.info(f"├{'─' * 50}┤")

        for field in fields:
            dc = field_diff_count[scope][field]
            buckets = field_diff_buckets[scope].get(field, {})
            log.info(f"│  {field:>8} 差异: {dc:>6} 条")
            if buckets:
                for label in ['0.01-0.05', '0.05-0.10', '0.10-0.50', '0.50+']:
                    if buckets[label] > 0:
                        log.info(f"│    [{label:<10}] {buckets[label]:>6}")

        log.info(f"└{'─' * 50}┘")
        log.info("")

    # ── 不一致股票 TOP 20 ──
    all_diff_stocks = set()
    for scope in scopes:
        if scope in stock_summary:
            all_diff_stocks.update(stock_summary[scope].keys())

    if all_diff_stocks:
        log.info(f"{'─' * 60}")
        log.info(f"不一致股票汇总: {len(all_diff_stocks)} 只")
        log.info(f"  (按 scope 展示差异最多的前 20 只)")
        for scope in scopes:
            if scope not in stock_summary or not stock_summary[scope]:
                continue
            sorted_stocks = sorted(stock_summary[scope].items(),
                                   key=lambda x: x[1]['diff_count'], reverse=True)[:20]
            log.info(f"  [{scope}]")
            for sid, info in sorted_stocks:
                log.info(f"    {sid:>8}  {info['diff_count']:>4} 处差异")
        log.info("")

    log.info(f"{'═' * 60}")
    log.info(f"  共需修正 (股票 × scope) 组合: {sum(s['diff'] for s in summary.values())} 条")
    log.info(f"{'═' * 60}")
    log.info("")


def _pct(part, total) -> float:
    return part / total * 100 if total > 0 else 0


def _write_diff_csv(diff_details: list, filepath: str):
    """将差异明细写入 CSV 文件"""
    if not diff_details:
        return
    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=diff_details[0].keys())
        writer.writeheader()
        writer.writerows(diff_details)


# ── 兼容旧调用入口 ──
def checkData():
    """旧 checkData 入口，转发到 validate_data_consistency (close only, week only)"""
    validate_data_consistency(
        start_date='20000101',
        end_date=date.today().strftime('%Y%m%d'),
        scopes=['week'],
        fields=['close'],
    )


def _quantize_close_price(value):
    """兼容别名，统一走 DBManager.quantize_price。"""
    return DBManager.quantize_price(value)


def _find_qfq_fix_boundary(ts_df, local_dict, price_threshold=Decimal('0.01')):
    """
    全量扫描 Tushare 与本地 close，确定待修正区间。

    前复权赋权后：近期数据一致、历史数据不一致，不能用「从最新往旧遇到一致就停」。

    :return: (fix_boundary, earliest_diff, diff_count, extra_local_count)
             fix_boundary   — 所有差异日期中的最大值，fix_df 取 trade_date <= 此值
             earliest_diff  — 差异日期中的最小值，供 quarter/year 重建
    """
    fix_boundary = None
    earliest_diff = None
    diff_count = 0
    ts_dates = set(ts_df['trade_date_str'].values)

    for _, row in ts_df.iterrows():
        td = row['trade_date_str']
        ts_close = _quantize_close_price(row['close'])
        if ts_close is None:
            continue  # Tushare 无有效收盘价，跳过
        if td not in local_dict:
            has_diff = True
        else:
            has_diff = abs(ts_close - local_dict[td]) >= price_threshold

        if has_diff:
            diff_count += 1
            if fix_boundary is None or td > fix_boundary:
                fix_boundary = td
            if earliest_diff is None or td < earliest_diff:
                earliest_diff = td

    extra_local = [d for d in local_dict if d not in ts_dates]
    if extra_local:
        max_extra = max(extra_local)
        min_extra = min(extra_local)
        if fix_boundary is None or max_extra > fix_boundary:
            fix_boundary = max_extra
        if earliest_diff is None or min_extra < earliest_diff:
            earliest_diff = min_extra

    return fix_boundary, earliest_diff, diff_count, len(extra_local)


def fixDataAfterFQ():
    """
    修正复权数据 — 边界探测 + 零 DELETE 方案。

    核心逻辑：
      1. 逐 scope 拉取 Tushare qfq 数据作为基准
      2. 全量扫描所有差异日期，取最新差异日作为边界
      3. 边界及之前 → MySQL UPSERT（INSERT ON DUPLICATE KEY UPDATE）
      4. 边界及之前 → TDEngine INSERT（自动根据时间戳 upsert）
      5. 无需 DELETE，MySQL 和 TDEngine 均原地覆盖
      6. quarter/year 在 month 完成后从 month 重新聚合（仅受影响分组）
    """
    setup_logger()
    
    ts_service = TushareService()
    db_manager = DBManager()

    # ────────── 配置 ──────────
    start_date = date(2000, 1, 1).strftime('%Y%m%d')
    end_date = date.today().strftime('%Y%m%d')

    scopes_config = _SCOPES_CONFIG

    stock_list = db_manager.get_stock_id_list()
    log.info(f"待处理股票数量: {len(stock_list)}")

    total_overwritten = set()                # (stock_id, scope) 被修正的集合
    month_boundaries = {}                    # stock_id → 最早的差异日期（用于 quarter/year 重建）

    for stock in stock_list:
        time.sleep(0.5)
        stock_id = stock.stock_id

        if not hasattr(stock, 'location') or stock.location is None:
            log.warning(f"股票 {stock_id} 缺少 location 信息，跳过")
            continue

        ts_symbol = TushareService.convert_stock_id(stock_id, stock.location)

        for cfg in scopes_config:
            scope = cfg['scope']
            log.info(f"  正在处理: {stock_id}, scope={scope}")

            # ── 1) 拉 Tushare qfq 全量数据（口径与 update_basic_get_stock 一致）──
            ts_df = ts_service.fetch_qfq_by_scope(
                symbol=ts_symbol, scope=scope,
                start_date=start_date, end_date=end_date,
            )

            if ts_df is None or ts_df.empty:
                log.info(f"  {stock_id} {scope}: Tushare 无数据，跳过")
                continue

            ts_df = ts_df.sort_values('trade_date')
            ts_df['trade_date_str'] = pd.to_datetime(ts_df['trade_date']).dt.strftime('%Y%m%d')

            # ── 2) 读本地 MySQL ──
            db_records = db_manager.get_stock_per_day_list(stock_id, scope, end_date)
            if not db_records:
                log.info(f"  {stock_id} {scope}: 本地无数据，跳过")
                continue

            local_dict = {}
            for r in db_records:
                td = r.trade_date.strftime('%Y%m%d')
                price = _quantize_close_price(r.end_price)
                if price is not None:
                    local_dict[td] = price

            # ── 3) 边界探测：全量扫描，取最新差异日作为 fix 边界 ──
            boundary_date, earliest_diff, diff_count, extra_local_count = _find_qfq_fix_boundary(
                ts_df, local_dict
            )
            if extra_local_count:
                log.info(f"  {stock_id} {scope}: 本地多余日期 {extra_local_count} 个")

            if boundary_date is None:
                log.info(f"  {stock_id} {scope}: 数据一致")
                continue

            log.info(f"  {stock_id} {scope}: 发现 {diff_count} 处差异, "
                     f"最早={earliest_diff}, 边界={boundary_date}")

            # ── 4) 截取待修正的子集：trade_date <= boundary_date，且 close 有效 ──
            fix_df = ts_df[ts_df['trade_date_str'] <= boundary_date].copy()
            fix_df = fix_df[fix_df['close'].notna()].copy()
            if fix_df.empty:
                log.info(f"  {stock_id} {scope}: 无有效 Tushare 收盘价可修正，跳过")
                continue
            log.info(f"  {stock_id} {scope}: 修正 {len(fix_df)} 条 "
                     f"(共 {len(ts_df)} 条, 修正比例 {len(fix_df)/len(ts_df)*100:.0f}%)")

            # 4a) MySQL: INSERT ON DUPLICATE KEY UPDATE（原地覆盖，无 DELETE）
            db_data = DBManager.convert_to_db_format_tushare(stock_id, fix_df, scope)
            mysql_affected = db_manager.batch_upsert_tushare_data(
                data_df=db_data, scope_type=scope, stock_id=stock_id
            )
            log.info(f"  {stock_id} {scope}: MySQL UPSERT {mysql_affected} 行")

            # 4b) TDEngine: INSERT（自动根据时间戳 upsert，无 DELETE）
            td_table = f'{scope}_{stock_id}'
            TDEngineWriter.create_dynamic_table(
                'nb_stock', stock_id, '', scope,
                td_table, 'stock_trade_history', False, 'RMB'
            )
            TDEngineWriter.write_data_batch(
                data=db_data, company_id=stock_id, table_name=td_table
            )
            log.info(f"  {stock_id} {scope}: TDEngine INSERT 完成")

            total_overwritten.add((stock_id, scope))
            if scope == 'month' and earliest_diff:
                month_boundaries[stock_id] = min(
                    month_boundaries.get(stock_id, '99999999'), earliest_diff
                )

    # ── 5) 汇总 ──
    log.info(f"========== 修正汇总 ==========")
    log.info(f"共修正 {len(total_overwritten)} 条 (股票 × scope)")
    log.info(f"month 级被修正的股票: {len(month_boundaries)} 只")

    # ── 6) 从修正后的 month 重建 quarter/year（仅受影响分组）──
    if month_boundaries:
        log.info("开始重建 quarter/year...")
        for stock_id, boundary in month_boundaries.items():
            rebuild_quarter_year_for_stock(db_manager, stock_id, boundary)
        log.info("quarter/year 重建完成")
    else:
        log.info("无 month 级别修正，跳过 quarter/year 重建")

    log.info("========== 复权修正完成 ==========")


def rebuild_quarter_year_for_stock(db_manager, stock_id: str, cutoff_date: str):
    """
    从修正后的 month 数据，重建单只股票的 quarter 和 year。
    只重建 trade_date >= cutoff_date 的分组（更旧的数据 month 级别是正确的，无需重建）。

    :param db_manager:  DBManager 实例
    :param stock_id:    股票代码
    :param cutoff_date: 最早差异日期（YYYYMMDD），只重建此日期及之后的分组
    """
    end_date = date.today().strftime('%Y%m%d')
    month_records = db_manager.get_stock_per_day_list(stock_id, 'month', end_date)
    if not month_records:
        return

    df = pd.DataFrame([{
        'trade_date': pd.to_datetime(r.trade_date),
        'open': float(r.start_price),
        'close': float(r.end_price),
        'high': float(r.high_price),
        'low': float(r.low_price),
        'vol': int(r.volume),
        'amount': int(r.amount),
        'turnover_rate': float(getattr(r, 'turnover_ratio', 0) or 0),
    } for r in month_records])

    df = df.sort_values('trade_date')

    for scope in ['quarter', 'year']:
        if scope == 'quarter':
            df['group'] = df['trade_date'].dt.to_period('Q')
        else:
            df['group'] = df['trade_date'].dt.to_period('Y')

        rows = []
        for grp, gdf in df.groupby('group'):
            gdf = gdf.sort_values('trade_date')
            group_last_date = gdf['trade_date'].iloc[-1].strftime('%Y%m%d')
            # 只重建受影响的分组
            if group_last_date < cutoff_date:
                continue

            open_p = gdf['open'].iloc[0]
            close_p = gdf['close'].iloc[-1]
            rows.append({
                'trade_date': group_last_date,
                'open': open_p,
                'close': close_p,
                'high': gdf['high'].max(),
                'low': gdf['low'].min(),
                'vol': int(gdf['vol'].sum()),
                'amount': int(gdf['amount'].sum()),
                'change': close_p - open_p,
                'pct_chg': round((close_p - open_p) / open_p * 100, 4) if open_p != 0 else 0,
                'turnover_rate': round(gdf['turnover_rate'].sum(), 4),
            })

        if not rows:
            continue

        agg = pd.DataFrame(rows)
        db_data = DBManager.convert_to_db_format_tushare(stock_id, agg, scope)

        # MySQL UPSERT
        mysql_affected = db_manager.batch_upsert_tushare_data(
            data_df=db_data, scope_type=scope, stock_id=stock_id
        )

        # TDEngine INSERT（自动 upsert）
        td_table = f'{scope}_{stock_id}'
        TDEngineWriter.create_dynamic_table(
            'nb_stock', stock_id, '', scope,
            td_table, 'stock_trade_history', False, 'RMB'
        )
        TDEngineWriter.write_data_batch(
            data=db_data, company_id=stock_id, table_name=td_table
        )

        log.info(f"  {stock_id} {scope}: 重建 {len(rows)} 条, MySQL={mysql_affected}")


#fixDataAfterFQ()
# checkData()