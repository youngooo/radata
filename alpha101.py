import pandas as pd
import rqdatac
from sqlalchemy import create_engine, text
from sqlalchemy import exc as sa_exc
import pymysql
from datetime import datetime
import logging
import time

# ============== 可配置：表名 ==============
TABLE_NAME = "factor_alpha101"

# ============== 初始化数据库连接 ==============
class DataBase_Position:
    __HOST = "106.15.107.93"
    __PORT = 3306
    __USERNAME = "chennianzeng"
    __PASSWORD = "chennianzeng666"
    __SCHEMA_OTC = "StockSignal"

    def __init__(self):
        self.conn = pymysql.connect(
            host=self.__HOST,
            port=self.__PORT,
            user=self.__USERNAME,
            password=self.__PASSWORD,
            charset="utf8",
            database=self.__SCHEMA_OTC
        )
        self.engine = create_engine(
            f"mysql+pymysql://{self.__USERNAME}:{self.__PASSWORD}@{self.__HOST}:{self.__PORT}/{self.__SCHEMA_OTC}?charset=utf8mb4"
        )

# ============== 函数：创建表（如果不存在） ==============
def create_alpha101_table(db):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        order_book_id VARCHAR(20),
        date DATE,
        factor_name VARCHAR(50),
        factor_value DOUBLE,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (order_book_id, date, factor_name)
    );
    """
    with db.engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()

# ============== 函数：拉取因子数据 ==============
def fetch_and_insert_factors(db, start_date, end_date, batch_size=100):
    # 获取因子名和股票列表
    logging.info("开始获取因子列表与股票池...")
    try:
        factor_list = rqdatac.get_all_factor_names(type='alpha101')
    except Exception:
        logging.exception("获取因子列表失败")
        raise
    try:
        all_stocks = rqdatac.all_instruments(type='CS', market='cn', date=start_date)['order_book_id'].tolist()
    except Exception:
        logging.exception("获取股票列表失败")
        raise

    logging.info("因子数量: %d, 股票数量: %d", len(factor_list), len(all_stocks))

    create_alpha101_table(db)

    for i in range(0, len(all_stocks), batch_size):
        stock_batch = all_stocks[i:i+batch_size]
        batch_no = i // batch_size + 1
        total_batches = (len(all_stocks) - 1) // batch_size + 1
        logging.info("开始处理批次 %d/%d, 股票数: %d", batch_no, total_batches, len(stock_batch))

        # 抓取数据，带简单重试
        attempt = 0
        df = None
        while attempt < 3:
            try:
                df = rqdatac.get_factor(order_book_ids=stock_batch,
                                         factor=factor_list,
                                         start_date=start_date,
                                         end_date=end_date,
                                         expect_df=True)
                break
            except Exception as e:
                attempt += 1
                logging.warning("批次 %d 获取因子失败, 第 %d 次重试, 错误: %s", batch_no, attempt, str(e))
                time.sleep(min(5 * attempt, 15))

        if df is None:
            logging.error("批次 %d 因子获取失败，跳过该批", batch_no)
            continue

        if df is None or df.empty:
            logging.info("批次 %d 返回空数据，跳过", batch_no)
            continue

        # 转换为长表（order_book_id, date, factor_name, factor_value）
        try:
            df = df.reset_index().melt(id_vars=['order_book_id', 'date'],
                                       var_name='factor_name',
                                       value_name='factor_value')
        except Exception:
            logging.exception("批次 %d 宽转长失败", batch_no)
            continue

        # 基础清洗
        try:
            df['date'] = pd.to_datetime(df['date']).dt.date
            before_drop = len(df)
            df = df.dropna(subset=['factor_value'])
            logging.info("批次 %d 宽转长后行数: %d, 去除NaN后: %d", batch_no, before_drop, len(df))
        except Exception:
            logging.exception("批次 %d 数据清洗失败", batch_no)
            continue

        if df.empty:
            logging.info("批次 %d 清洗后为空，跳过", batch_no)
            continue

        # 过滤掉已存在的数据（增量，按日期范围粗过滤）
        try:
            existing_dates = pd.read_sql(
                f"SELECT DISTINCT date FROM {TABLE_NAME} WHERE date BETWEEN '{start_date}' AND '{end_date}'",
                con=db.engine
            )['date'].tolist()
            before_filter = len(df)
            df = df[~df['date'].isin(existing_dates)]
            logging.info("批次 %d 日期维度粗过滤: %d -> %d", batch_no, before_filter, len(df))
        except Exception:
            logging.exception("批次 %d 查询已存在日期失败，跳过过滤继续写入", batch_no)

        if df.empty:
            logging.info("批次 %d 过滤后为空，跳过", batch_no)
            continue

        # 入库
        try:
            df.to_sql(TABLE_NAME, con=db.engine, if_exists='append', index=False)
            logging.info("批次 %d 成功插入 %d 行", batch_no, len(df))
        except sa_exc.IntegrityError:
            logging.exception("批次 %d 插入主键冲突，建议使用按三元键增量/或UPSERT", batch_no)
            continue
        except Exception:
            logging.exception("批次 %d 插入失败", batch_no)
            continue

# ============== 主程序 ==============
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s'
    )
    logging.info("初始化 rqdatac 与数据库连接")
    rqdatac.init()  
    db = DataBase_Position()

    start_date = "2020-01-01"
    end_date = "2025-09-16"
    try:
        fetch_and_insert_factors(db, start_date, end_date)
        logging.info("任务完成")
    except Exception:
        logging.exception("任务执行过程中发生未处理异常")
