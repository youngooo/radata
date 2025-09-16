import pandas as pd
import pymysql
from sqlalchemy import create_engine, inspect
import rqdatac
from datetime import datetime

# ================== 配置区 ==================
class DataBase_Position:
    """从Mysql数据库获取数据"""
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

# ================== 核心逻辑 ==================

def get_all_instruments():
    """通过 rqdatac 获取因子暴露数据"""
    # 登录 rqdatac
    rqdatac.init()

    # 示例：获取全市场股票信息
    df = rqdatac.all_instruments(type='CS', market='cn', date=None)
    # 这里你可以替换成具体的因子暴露接口，比如 rqdatac.get_factor_exposure(...)
    return df

def load_data_to_mysql(df, table_name, db: DataBase_Position):
    """加载数据到 MySQL，如果表不存在则创建表，存在则增量插入"""
    inspector = inspect(db.engine)
    tables = inspector.get_table_names()

    if table_name not in tables:
        print(f"[INFO] 表 {table_name} 不存在，执行全量插入...")
        df.to_sql(name=table_name, con=db.engine, if_exists='replace', index=False)
        print(f"[INFO] 表 {table_name} 创建完成，插入 {len(df)} 行")
    else:
        print(f"[INFO] 表 {table_name} 已存在，执行增量插入...")
        existing_df = pd.read_sql(f"SELECT * FROM {table_name}", con=db.engine)

        # 假设有唯一键 "order_book_id" 判断是否重复
        key_cols = ["order_book_id"]
        merged_df = pd.merge(df, existing_df[key_cols], on=key_cols, how="left", indicator=True)
        new_df = merged_df[merged_df["_merge"] == "left_only"].drop(columns=["_merge"])

        if len(new_df) > 0:
            new_df.to_sql(name=table_name, con=db.engine, if_exists='append', index=False)
            print(f"[INFO] 增量插入 {len(new_df)} 行")
        else:
            print("[INFO] 没有新的数据需要插入")

# ================== 主程序 ==================
if __name__ == "__main__":
    db = DataBase_Position()
    table_name = "stock_info"
    df = get_all_instruments()

    # 确保日期列是 datetime 格式，便于比较
   # if "listed_date" in df.columns:
   #     df["listed_date"] = pd.to_datetime(df["listed_date"])

    load_data_to_mysql(df, table_name, db)
