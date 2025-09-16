import pandas as pd
import pymysql
from sqlalchemy import create_engine, inspect
import rqdatac
from datetime import datetime, timedelta

# ================== 数据库连接类 ==================
class DataBase_Position:
    """MySQL 数据库连接"""
    __HOST = "106.15.107.93"
    __PORT = 3306
    __USERNAME = "chennianzeng"
    __PASSWORD = "chennianzeng666"
    __SCHEMA_OTC = "StockSignal"

    def __init__(self):
        self.conn = pymysql.connect(
            host=DataBase_Position.__HOST,
            port=DataBase_Position.__PORT,
            user=DataBase_Position.__USERNAME,
            password=DataBase_Position.__PASSWORD,
            charset="utf8",
            database=DataBase_Position.__SCHEMA_OTC
        )
        self.engine = create_engine(
            f"mysql+pymysql://{DataBase_Position.__USERNAME}:{DataBase_Position.__PASSWORD}@"
            f"{DataBase_Position.__HOST}:{DataBase_Position.__PORT}/{DataBase_Position.__SCHEMA_OTC}?charset=utf8mb4"
        )

# ================== 获取股票列表 ==================
def get_stock_list(db: DataBase_Position):
    """从 stock_info 表获取股票代码列表"""
    return pd.read_sql("SELECT order_book_id FROM stock_info", con=db.engine)["order_book_id"].tolist()

# ================== 获取某只股票最新已存日期 ==================
def get_latest_date(db: DataBase_Position, table_name: str, order_book_id: str):
    inspector = inspect(db.engine)
    tables = inspector.get_table_names()
    if table_name not in tables:
        return None  # 表不存在，表示需要全量拉取
    sql = f"SELECT MAX(date) AS last_date FROM {table_name} WHERE order_book_id='{order_book_id}'"
    result = pd.read_sql(sql, con=db.engine)
    if result["last_date"].iloc[0] is None:
        return None
    return result["last_date"].iloc[0]

# ================== 获取行情数据 ==================
def get_price_data(order_book_id, start_date, end_date, max_retry=3, retry_wait=3):
    """
    从 rqdatac 拉取行情数据，支持重试，防止返回 None 导致报错
    """
    for attempt in range(1, max_retry + 1):
        try:
            df = rqdatac.get_price(
                order_book_ids=[order_book_id],
                start_date=start_date,
                end_date=end_date,
                frequency="1d",
                fields=None,
                adjust_type="none",
                skip_suspended=False,
                market="cn",
                expect_df=True
            )
            # 处理返回 None 的情况
            if df is None or len(df) == 0:
                print(f"[WARN] {order_book_id} {start_date}~{end_date} 没有数据 (返回 None)")
                return pd.DataFrame()

            df = df.reset_index().rename(columns={"index": "date"})
            df["order_book_id"] = order_book_id
            return df

        except Exception as e:
            print(f"[ERROR] 第 {attempt} 次获取 {order_book_id} 数据失败: {e}")
            if attempt < max_retry:
                print(f"[INFO] 等待 {retry_wait} 秒后重试...")
                time.sleep(retry_wait)
            else:
                print(f"[ERROR] {order_book_id} 获取失败，跳过该股票")
                return pd.DataFrame()

# ================== 插入数据库 ==================
def insert_data(df, table_name, db: DataBase_Position, chunksize=1000):
    """分批写入 MySQL"""
    df.to_sql(name=table_name, con=db.engine, if_exists="append", index=False, chunksize=chunksize)

# ================== 主程序 ==================
if __name__ == "__main__":
    db = DataBase_Position()
    rqdatac.init()

    table_name = "stock_price"
    order_book_ids = get_stock_list(db)

    total_inserted = 0
    for i, obid in enumerate(order_book_ids, start=1):
        print(f"[INFO] ({i}/{len(order_book_ids)}) 处理 {obid} ...")

        # 1. 获取该股票已存的最大日期
        last_date = get_latest_date(db, table_name, obid)
        if last_date is None:
            start_date = "2000-01-01"  # 全量抓取的起始日，可根据业务调整
        else:
            start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = datetime.today().strftime("%Y-%m-%d")

        # 2. 拉取该股票数据
        df = get_price_data(obid, start_date, end_date)
        if df.empty:
            print(f"[INFO] {obid} {start_date}~{end_date} 没有新数据")
            continue

        # 3. 插入数据
        insert_data(df, table_name, db)
        total_inserted += len(df)
        print(f"[INFO] {obid} 插入 {len(df)} 行")

    print(f"[DONE] 共插入 {total_inserted} 行数据")
