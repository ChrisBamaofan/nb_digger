# stock_list = ['871694', '871553', '839273', '838971', '838171', '830779', '688582', '688510', '688486', '688368', '688307', '688200', '688195', '688123', '688100', '688093', '605499', '605368', '605218', '605077', '605028', '603949', '603866', '603809', '603697', '603682', '603677', '603223', '603199', '603162', '603031', '601988', '601886', '601882', '601658', '601328', '600976', '600965', '600711', '600605', '600502', '600211', '600113', '301389', '301362', '301280', '301277', '301187', '301127', '301113', '301069', '301066', '301016', '301004', '300999', '300997', '300969', '300960', '300926', '300918', '300896', '300842', '300824', '300801', '300783', '300782', '300750', '300693', '300562', '300476', '300458', '300441', '300406', '300403', '300401', '300373', '300304', '300146', '300102', '300059', '300047', '300018', '003022', '003006', '003000', '002972', '002967', '002920', '002911', '002879', '002876', '002870', '002852', '002807', '002774', '002749', '002692', '002661', '002658', '002600', '002516', '002469', '002467', '002455', '002404', '002393', '002320', '002318', '002315', '002282', '002236', '002233', '002185', '002145', '002054', '002014', '001376', '001368', '001358', '001299', '000989', '000895', '000823', '000538', '000063', '000062', '000048']
# stock_list = ['300801', '300783', '300782', '300750', '300693', '300562', '300476', '300458', '300441', '300406', '300403', '300401', '300373', '300304', '300146', '300102', '300059', '300047', '300018', '003022', '003006', '003000', '002972', '002967', '002920', '002911', '002879', '002876', '002870', '002852', '002807', '002774', '002749', '002692', '002661', '002658', '002600', '002516', '002469', '002467', '002455', '002404', '002393', '002320', '002318', '002315', '002282', '002236', '002233', '002185', '002145', '002054', '002014', '001376', '001368', '001358', '001299', '000989', '000895', '000823', '000538', '000063', '000062', '000048']
from datasource.akshare_client import AKShareClient
from database.db_manager import DBManager
from database.models import StockDaily
from utils.logger import setup_logger,log
from datetime import datetime, timedelta,date
import pandas as pd
from database.tdengine_writer import TDEngineWriter
import time
import numpy as np
from decimal import Decimal, ROUND_HALF_UP
import akshare as ak

def checkData():
    setup_logger()
    
    akshare = AKShareClient()
    db_manager = DBManager()

    # 获取数据时间范围
    start_date = date(2025, 2, 1).strftime('%Y%m%d')
    end_date = date(2025, 8, 2).strftime('%Y%m%d')
    mismatch_list = []  # 存储不一致的股票列表

    # 股票列表 (示例)
    stock_list = db_manager.get_stock_id_list()
    #stock_list = ['002117']
    # stock_list = ['002117','300797','300986','301162','605319','002579','002615','002647','002947','300201','300245','300484','300698','300752','300791','300835','300959','301333','600367','600506','600714','600975','603015','603239','603322','605319']
    list = []
    for stock in stock_list:
        
        # 睡眠6秒钟
        time.sleep(2)
        stock_id = stock.stock_id
        # akshare: daily_day,weekly_week,monthly_month,
        period_ak = 'weekly'
        period_local = 'week'
        log.info(f"正在处理股票: {stock},{period_local}")

        # 获取股票上市日期
        # basic_info = db_manager.get_stock_basic_info(stock_id)

        # 获取AKShare原始数据 symbol, 'daily', start_date, end_date, adjust
        # start = basic_info.launch_date.strftime('%Y%m%d')
        raw_data = akshare.get_stock_data(
            symbol= stock_id.lower(),
            period = period_ak,
            start_date= start_date,
            end_date=end_date,
            adjust="qfq"
        )
        # log.info(raw_data)
        
        if raw_data is None or raw_data.empty:
            log.warning(f"未获取到股票 {stock_id} 的AKShare数据")
            continue
        raw_data = raw_data.sort_values('trade_date',ascending=False)

        db_records = db_manager.get_stock_per_day_list(stock_id,period_local,end_date)
        # log.info(db_records)
        if db_records is None :
            log.warning(f"未获取到股票 {stock_id} 的数据库数据")
            # log.info(db_records)
            continue
            
         # 确保日期格式一致（AKShare日期可能是时间戳，数据库可能是字符串）
        raw_data['trade_date'] = pd.to_datetime(raw_data['trade_date']).dt.strftime('%Y%m%d')
        # 将数据库记录转为{日期: 收盘价}字典
        db_dict = {
            record.trade_date.strftime('%Y%m%d'): Decimal(str(record.end_price)).quantize(Decimal('0.01'))
            for record in db_records
        }

        # 3. 比较数据
        mismatch_dates = []
        for idx, row in raw_data.iterrows():
            trade_date = row['trade_date']
            ak_close = Decimal(str(row['close'])).quantize(Decimal('0.01'))

            if trade_date not in db_dict:
                log.warning(f"日期 {trade_date} 在数据库中不存在")
                mismatch_dates.append(trade_date)
                continue

            db_close = db_dict[trade_date]
            if abs(ak_close - db_close) > Decimal('0.1'):
                log.warning(
                    f"日期 {trade_date} 数据不一致: "
                    f"AK={ak_close} DB={db_close} 差值={ak_close - db_close}"
                )
                mismatch_dates.append(trade_date)

        if mismatch_dates:
            mismatch_list.append(stock_id)
            log.info(f"股票 {stock_id} 共有 {len(mismatch_dates)} 个周线数据不一致")

    log.info(f"最终不一致股票: {mismatch_list}")

def fixDataAfterFQ():
    setup_logger()
    
    akshare = AKShareClient()
    db_manager = DBManager()


    # TDEngineWriter.execute_bulk_price_adjustment('000858','week_000858',1,'20250711')

    # 获取数据时间范围
    start_date = date(2025, 6, 1).strftime('%Y%m%d')
    end_date = date(2025, 8, 15).strftime('%Y%m%d')
    adjusted_stocks = []  # 存储已调整的股票列表

    # 股票列表
    stock_list = db_manager.get_stock_id_list()

    for stock in stock_list:
        time.sleep(3)  # 防止请求过于频繁
        stock_id = stock.stock_id
        period_ak = 'weekly'
        period_local = 'week'
        log.info(f"正在处理股票: {stock_id}, {period_local}")

        # 获取AKShare前复权数据
        raw_data = akshare.get_stock_data(
            symbol=stock_id.lower(),
            period=period_ak,
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"  # 前复权
        )
        
        if raw_data is None or raw_data.empty:
            log.warning(f"未获取到股票 {stock_id} 的AKShare数据")
            continue
        
        raw_data = raw_data.sort_values('trade_date', ascending=False)
        raw_data['trade_date'] = pd.to_datetime(raw_data['trade_date']).dt.strftime('%Y%m%d')

        # 获取本地数据库数据
        db_records = db_manager.get_stock_per_day_list(stock_id, period_local, end_date)
        if db_records is None:
            log.warning(f"未获取到股票 {stock_id} 的数据库数据")
            continue
            
        # 将数据库记录转为{日期: 记录对象}字典
        db_dict = {
            record.trade_date.strftime('%Y%m%d'): record
            for record in db_records
        }

        # 寻找第一个差异点
        adjustment_diff = None
        adjustment_date = None
        for idx, row in raw_data.iterrows():
            trade_date = row['trade_date']
            
            if trade_date not in db_dict:
                continue

            db_record = db_dict[trade_date]
            ak_close = Decimal(str(row['close']))
            db_close = Decimal(str(db_record.end_price))
            current_diff = ak_close - db_close
            
            # 找到第一个有显著差异的日期
            if abs(current_diff) > Decimal('0.01'):
                adjustment_diff = current_diff
                adjustment_date = trade_date
                log.info(
                    f"股票 {stock_id} 在 {trade_date} 需要调整: "
                    f"差值={adjustment_diff}, 将调整此日期前的所有数据"
                )
                break

        # 执行批量调整
        if adjustment_diff is not None:
            affected = db_manager.execute_bulk_price_adjustment(
                        stock_id={stock_id},
                        adjustment_diff={float(adjustment_diff)},  # 需要减去的差值
                        adjustment_date={adjustment_date}  # 除权除息日
                    )
            # TDEngineWriter.execute_bulk_price_adjustment(stock_id,'week_'+stock_id,float(adjustment_diff),adjustment_date)
                                                         
            log.info(f"股票 {stock_id} 成功调整  条历史记录")
            adjusted_stocks.append({
                'stock_id': stock_id,
                'adjustment_diff': float(adjustment_diff),
                'adjustment_date': adjustment_date,
                'affected_rows': affected
            })
        else:
            log.info(f"股票 {stock_id} 数据一致，无需调整")


fixDataAfterFQ()
#checkData()