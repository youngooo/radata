import pandas as pd
import pymysql
from sqlalchemy import create_engine, inspect
import rqdatac
from datetime import datetime, timedelta
import time
import logging
import argparse

# ================== 数据库连接类 ==================
class DataBase_Position:
    """MySQL 数据库连接"""
    __HOST = ""
    __PORT = 3306
    __USERNAME = ""
    __PASSWORD = ""
    __SCHEMA_OTC = ""

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

def get_all_stocks_from_rqdatac(target_date):
    """从rqdatac获取所有股票列表"""
    try:
        all_stocks = rqdatac.all_instruments(type='CS', market='cn', date=target_date)['order_book_id'].tolist()
        return all_stocks
    except Exception as e:
        logging.error(f"获取股票列表失败: {e}")
        return []

# ============== 函数：获取交易日列表 ==============
def get_trading_dates(start_date, end_date):
    """使用rqdatac获取真实的交易日列表"""
    try:
        # 将日期格式从 'YYYYMMDD' 转换为 'YYYY-MM-DD'
        start_date_str = start_date[:4] + '-' + start_date[4:6] + '-' + start_date[6:8]
        end_date_str = end_date[:4] + '-' + end_date[4:6] + '-' + end_date[6:8]
        
        # 使用rqdatac获取交易日列表
        trade_dt_list = rqdatac.get_trading_dates(start_date=start_date, end_date=end_date)
        
        # 将datetime对象转换为字符串格式 'YYYYMMDD'
        date_list = [dt.strftime('%Y%m%d') for dt in trade_dt_list]
        
        return date_list
        
    except Exception as e:
        logging.error(f"获取交易日列表失败: {e}")
        # 如果rqdatac获取失败，回退到原来的方法
        logging.warning("回退到手动计算交易日")
        return get_trading_dates_fallback(start_date, end_date)

def get_trading_dates_fallback(start_date, end_date):
    """回退方法：手动计算交易日（排除周末）"""
    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        
        date_list = []
        current_date = start_dt
        while current_date <= end_dt:
            # 排除周末 (0=Monday, 6=Sunday)
            if current_date.weekday() < 5:  # 0-4 表示周一到周五
                date_list.append(current_date.strftime("%Y%m%d"))
            current_date += timedelta(days=1)
        
        return date_list
        
    except Exception as e:
        logging.error(f"回退方法获取交易日失败: {e}")
        return []

# ================== 获取单日行情数据 ==================
def get_daily_price_data_batch(all_stocks, target_date, max_retry=3, retry_wait=3):
    """
    从 rqdatac 批量拉取单日行情数据，支持重试，防止返回 None 导致报错
    """
    for attempt in range(1, max_retry + 1):
        try:
            df = rqdatac.get_price(
                order_book_ids=all_stocks,
                start_date=target_date,
                end_date=target_date,
                frequency="1d",
                fields=None,
                adjust_type="none",
                skip_suspended=False,
                market="cn",
                expect_df=True
            )
            # 处理返回 None 的情况
            if df is None or len(df) == 0:
                logging.warning(f"所有股票 {target_date} 没有数据 (返回 None)")
                return pd.DataFrame()

            df = df.reset_index().rename(columns={"index": "date"})
            # 将日期格式转换为YYYYMMDD
            df["date"] = df["date"].dt.strftime("%Y%m%d")
            return df

        except Exception as e:
            logging.error(f"第 {attempt} 次批量获取 {target_date} 数据失败: {e}")
            if attempt < max_retry:
                logging.info(f"等待 {retry_wait} 秒后重试...")
                time.sleep(retry_wait)
            else:
                logging.error(f"批量获取 {target_date} 数据失败")
                return pd.DataFrame()

# ================== 插入数据库 ==================
def insert_data(df, table_name, db: DataBase_Position, chunksize=2000):
    """分批写入 MySQL"""
    df.to_sql(name=table_name, con=db.engine, if_exists="append", index=False, chunksize=chunksize)

# ================== 日期验证函数 ==================
def validate_date(date_str, param_name="日期"):
    """
    验证日期格式和有效性
    
    Args:
        date_str (str): 日期字符串
        param_name (str): 参数名称，用于错误提示
    
    Returns:
        datetime: 验证通过的日期对象
    """
    if not date_str:
        return None
        
    try:
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        return date_obj
    except ValueError:
        print(f"错误: {param_name}格式错误 '{date_str}'，请使用 YYYYMMDD 格式")
        exit(1)

# ================== 按天轮询主程序 ==================
def daily_polling_main(start_date=None, end_date=None):
    """
    按天轮询的主程序
    
    Args:
        start_date (str): 开始日期，格式 'YYYYMMDD'，默认为昨天
        end_date (str): 结束日期，格式 'YYYYMMDD'，默认为今天
    """
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('stock_price_daily.log'),
            logging.StreamHandler()
        ]
    )
    
    # 设置默认日期
    if end_date is None:
        end_date = datetime.today().strftime("%Y%m%d")
    if start_date is None:
        start_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    
    # 验证日期格式
    start_dt = validate_date(start_date, "开始日期")
    end_dt = validate_date(end_date, "结束日期")
    
    # 验证日期范围
    if start_dt > end_dt:
        logging.error("开始日期不能晚于结束日期")
        return
    
    if end_dt > datetime.today():
        logging.error("结束日期不能晚于今天")
        return
    
    db = DataBase_Position()
    rqdatac.init()

    table_name = "stock_price"
    
    logging.info(f"开始轮询 {start_date} 到 {end_date} 的数据")
    
    # 获取交易日列表
    date_list = get_trading_dates(start_date, end_date)
    if not date_list:
        logging.error("无法获取交易日列表")
        return
    
    logging.info(f"获取到 {len(date_list)} 个交易日")
    
    total_inserted = 0
    success_count = 0
    fail_count = 0
    
    # 按交易日循环处理
    for date_str in date_list:
        logging.info(f"处理交易日: {date_str}")
        
        # 1. 获取该日期的所有股票列表
        all_stocks = get_all_stocks_from_rqdatac(date_str)
        if not all_stocks:
            logging.warning(f"{date_str} 没有获取到股票列表")
            continue
        
        logging.info(f"{date_str} 获取到 {len(all_stocks)} 只股票")
        
        # 2. 批量拉取该日期的所有股票数据
        df_all = get_daily_price_data_batch(all_stocks, date_str)
        if df_all.empty:
            logging.info(f"{date_str} 没有数据")
            continue
        
        # 3. 检查哪些股票数据已存在，过滤掉已存在的数据
        existing_data = set()
        try:
            check_sql = f"""
            SELECT DISTINCT order_book_id 
            FROM {table_name} 
            WHERE date='{date_str}'
            """
            existing_result = pd.read_sql(check_sql, con=db.engine)
            existing_data = set(existing_result["order_book_id"].tolist())
        except Exception as e:
            logging.warning(f"检查已存在数据时出错: {e}，继续处理所有数据")
        
        # 4. 过滤掉已存在的数据
        if existing_data:
            df_filtered = df_all[~df_all["order_book_id"].isin(existing_data)]
            logging.info(f"{date_str} 过滤掉 {len(existing_data)} 只已存在的股票，剩余 {len(df_filtered)} 只股票")
        else:
            df_filtered = df_all
            logging.info(f"{date_str} 没有已存在的数据，处理所有 {len(df_filtered)} 只股票")
        
        if df_filtered.empty:
            logging.info(f"{date_str} 所有数据都已存在，跳过")
            continue
        
        # 5. 批量插入新数据
        try:
            insert_data(df_filtered, table_name, db)
            total_inserted += len(df_filtered)
            success_count += len(df_filtered)
            logging.info(f"{date_str} 成功插入 {len(df_filtered)} 行数据")
        except Exception as e:
            logging.error(f"{date_str} 插入失败: {e}")
            fail_count += len(df_filtered)

    logging.info(f"交易日轮询完成 - 成功: {success_count}, 失败: {fail_count}, 共插入: {total_inserted} 行")

if __name__ == "__main__":
    # 在脚本内设置开始日期参数
    START_DATE = "20000101"  # 可以修改这个日期作为开始日期
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description='股票价格数据按天轮询程序',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
使用示例:
  python stock_price.py                                    # 默认：{START_DATE}到今天
  python stock_price.py --end-date 20240105               # 从{START_DATE}到指定日期
  python stock_price.py --days 7                          # 最近7天
        """
    )
    
    parser.add_argument(
        '--end-date', 
        type=str, 
        help=f'结束日期 (YYYYMMDD)，默认为今天。例如: 20240105'
    )
    parser.add_argument(
        '--days', 
        type=int, 
        help='从结束日期往前推的天数。例如: --days 7 表示最近7天'
    )
    
    args = parser.parse_args()
    
    # 处理参数
    if args.days is not None:
        if args.days <= 0:
            print("错误: --days 必须大于0")
            exit(1)
        end_date = args.end_date or datetime.today().strftime("%Y%m%d")
        end_dt = validate_date(end_date, "结束日期")
        start_date = (end_dt - timedelta(days=args.days-1)).strftime("%Y%m%d")
    else:
        start_date = START_DATE
        end_date = args.end_date
    
    # 运行主程序
    daily_polling_main(start_date=start_date, end_date=end_date)
