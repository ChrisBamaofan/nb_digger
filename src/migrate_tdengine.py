"""
将 nb_stock 库中 stock_trade_history 的行情数据迁移到优化后的 nb_stock2 库。
"""
import taosrest
import time
from utils.config_loader import load_config
from utils.logger import setup_logger, log

OLD_DB = "nb_stock"
NEW_DB = "nb_stock2"
STABLE = "stock_trade_history"


def get_conn():
    config = load_config().get('tdengine', {})
    return taosrest.connect(
        url=config.get('url', 'localhost'),
        user=config.get('user', 'root'),
        password=config.get('password', 'taosdata'),
        timezone='Asia/Shanghai',
        timeout=120
    )


def migrate():
    setup_logger()
    conn = get_conn()

    log.info("========== 开始迁移 stock_trade_history ==========")

    # 1. 查询旧库中所有子表名
    log.info("查询子表列表...")
    result = conn.query(
        f"SELECT table_name FROM information_schema.ins_tables "
        f"WHERE db_name = '{OLD_DB}' AND stable_name = '{STABLE}'"
    )
    table_names = [r[0] for r in result]
    log.info(f"共 {len(table_names)} 个子表待迁移")

    if not table_names:
        log.warning("未找到子表，退出")
        return

    success = 0
    failed = 0

    for i, tbl in enumerate(table_names):
        try:
            # 2. 查询子表的 TAG 值
            tag_result = conn.query(
                f"SELECT location, company_id, cycle_type "
                f"FROM {OLD_DB}.{STABLE} WHERE tbname = '{tbl}' LIMIT 1"
            )
            tag_rows = list(tag_result)

            if not tag_rows:
                log.warning(f"[{i+1}/{len(table_names)}] {tbl} 无TAG数据，跳过")
                failed += 1
                continue

            location = tag_rows[0][0] or ''
            company_id = tag_rows[0][1] or ''
            cycle_type = tag_rows[0][2] or ''

            # 3. 在新库创建子表
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {NEW_DB}.{tbl} "
                f"USING {NEW_DB}.{STABLE} "
                f"TAGS ('{location}', '{company_id}', '{cycle_type}')"
            )

            # 4. 跨库 INSERT SELECT
            conn.execute(
                f"INSERT INTO {NEW_DB}.{tbl} "
                f"SELECT trade_date, amount, volume, open, close, high, low, "
                f"change_price, change_percent, turnover "
                f"FROM {OLD_DB}.{tbl}"
            )

            success += 1
            if (i + 1) % 200 == 0:
                log.info(f"进度: {i+1}/{len(table_names)}, 成功={success}, 失败={failed}")

        except Exception as e:
            failed += 1
            log.error(f"[{i+1}/{len(table_names)}] {tbl} 迁移失败: {e}")
            continue

    log.info(f"========== 迁移完成: 成功={success}, 失败={failed}, 总计={len(table_names)} ==========")


if __name__ == '__main__':
    migrate()
