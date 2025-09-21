import pandas as pd
import rqdatac
from sqlalchemy import create_engine, text
import pymysql
from datetime import datetime, timedelta
import time
import logging

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

table_name = "factor_obos"
factor_type = "obos_indicator"
logger_file = "obos.log"

# ============== 函数：创建表（如果不存在） ==============
def create_alpha101_table(db):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
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

# ============== 配置日志 ==============
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(logger_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

# ============== 函数：获取日期列表 ==============
def get_date_range(start_date, end_date):
    """使用rqdatac获取真实的交易日列表"""
    try:
        # 将日期格式从 'YYYY-MM-DD' 转换为 'YYYYMMDD'
        start_date_str = start_date.replace('-', '')
        end_date_str = end_date.replace('-', '')
        
        # 使用rqdatac获取交易日列表
        trade_dt_list = rqdatac.get_trading_dates(start_date=start_date_str, end_date=end_date_str)
        
        # 将datetime对象转换为字符串格式 'YYYY-MM-DD'
        date_list = [dt.strftime('%Y-%m-%d') for dt in trade_dt_list]
        
        return date_list
        
    except Exception as e:
        logging.error(f"获取交易日列表失败: {e}")
        # 如果rqdatac获取失败，回退到原来的方法
        logging.warning("回退到手动计算交易日")
        return get_date_range_fallback(start_date, end_date)

def get_date_range_fallback(start_date, end_date):
    """回退方案：手动生成日期范围列表，跳过周末"""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    date_list = []
    current = start
    while current <= end:
        # 跳过周末（周六=5，周日=6）
        if current.weekday() < 5:
            date_list.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    return date_list

# ============== 函数：数据清洗 ==============
def clean_factor_data(df_long, logger, batch_info=""):
    """清洗因子数据，处理无穷大值和缺失值"""
    if df_long.empty:
        return df_long
    
    # 统计原始数据中的特殊值
    inf_count = df_long['factor_value'].isin([float('inf'), float('-inf')]).sum()
    nan_count = df_long['factor_value'].isna().sum()
    
    if inf_count > 0 or nan_count > 0:
        logger.info(f"🔧 {batch_info} 数据清洗: inf值={inf_count}, nan值={nan_count}")
    
    # 将inf值替换为None（MySQL中存储为NULL）
    df_long['factor_value'] = df_long['factor_value'].replace([float('inf'), float('-inf')], None)
    
    # 确保关键字段不为空
    df_cleaned = df_long.dropna(subset=['order_book_id', 'date', 'factor_name'])
    
    # 记录清洗后的数据量
    if len(df_cleaned) != len(df_long):
        logger.info(f"🧹 {batch_info} 清洗后数据量: {len(df_cleaned)}/{len(df_long)}")
    
    return df_cleaned

# ============== 函数：检查数据库中已存在的日期 ==============
def get_existing_dates(db, start_date, end_date):
    """获取数据库中已存在的日期"""
    try:
        existing_dates = pd.read_sql(
            f"SELECT DISTINCT date FROM {table_name} WHERE date BETWEEN '{start_date}' AND '{end_date}'",
            con=db.engine
        )['date'].tolist()
        return [str(date) for date in existing_dates]
    except Exception as e:
        logging.warning(f"获取已存在日期时出错: {e}")
        return []

# ============== 函数：单日数据获取 ==============
def fetch_single_day_factors(db, target_date, batch_size=100, max_retries=3):
    """获取单日的因子数据"""
    logger = logging.getLogger(__name__)
    
    # 检查该日期是否已存在
    existing_dates = get_existing_dates(db, target_date, target_date)
    if target_date in existing_dates:
        logger.info(f"📅 {target_date} 数据已存在，跳过")
        return True
    
    try:
        # 获取因子名和股票列表
        factor_list = rqdatac.get_all_factor_names(type=factor_type)
        all_stocks = rqdatac.all_instruments(type='CS', market='cn', date=target_date)['order_book_id'].tolist()
        
        logger.info(f"📅 开始获取 {target_date} 的数据，共 {len(all_stocks)} 只股票")
        
        total_inserted = 0
        
        for i in range(0, len(all_stocks), batch_size):
            stock_batch = all_stocks[i:i+batch_size]
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    df = rqdatac.get_factor(order_book_ids=stock_batch,
                                          factor=factor_list,
                                          start_date=target_date,
                                          end_date=target_date,
                                          expect_df=True)
                    
                    if df is None or df.empty:
                        logger.warning(f"⚠️ {target_date} batch {i//batch_size+1} 返回空数据")
                        break
                    
                    # 转换为长表（order_book_id, date, factor_name, factor_value）
                    df_long = df.reset_index().melt(id_vars=['order_book_id', 'date'],
                                                   var_name='factor_name',
                                                   value_name='factor_value')
                    
                    # 数据清洗：处理无穷大值和缺失值
                    if not df_long.empty:
                        batch_info = f"{target_date} batch {i//batch_size+1}/{len(all_stocks)//batch_size+1}"
                        df_cleaned = clean_factor_data(df_long, logger, batch_info)
                        
                        # 插入数据库
                        if not df_cleaned.empty:
                            df_cleaned.to_sql(table_name, con=db.engine, if_exists='append', index=False)
                            total_inserted += len(df_cleaned)
                            logger.info(f"✅ {batch_info} 插入 {len(df_cleaned)} 行数据")
                    
                    # 添加短暂延迟避免API限制
                    time.sleep(0.1)
                    break
                    
                except Exception as e:
                    retry_count += 1
                    error_msg = str(e)
                    
                    # 特殊处理MySQL相关错误
                    if "inf cannot be used with MySQL" in error_msg:
                        logger.error(f"❌ {target_date} batch {i//batch_size+1} 数据包含无穷大值，跳过此批次")
                        break  # 跳过此批次，不重试
                    elif "MySQL server has gone away" in error_msg:
                        logger.warning(f"⚠️ {target_date} batch {i//batch_size+1} MySQL连接断开，等待重连")
                        time.sleep(5)  # 等待更长时间重连
                    else:
                        logger.warning(f"⚠️ {target_date} batch {i//batch_size+1} 第 {retry_count} 次重试，错误: {e}")
                    
                    if retry_count >= max_retries:
                        logger.error(f"❌ {target_date} batch {i//batch_size+1} 重试 {max_retries} 次后失败")
                        break
                    time.sleep(2)  # 重试前等待
        
        logger.info(f"🎉 {target_date} 数据获取完成，共插入 {total_inserted} 行数据")
        return True
        
    except Exception as e:
        logger.error(f"❌ {target_date} 数据获取失败: {e}")
        return False

# ============== 函数：拉取因子数据（带日期轮询） ==============
def fetch_and_insert_factors(db, start_date, end_date, batch_size=100):
    """主要的因子数据获取函数，支持日期轮询"""
    logger = setup_logging()
    logger.info(f"🚀 开始获取因子数据，时间范围: {start_date} 到 {end_date}")
    
    create_alpha101_table(db)
    
    # 获取所有交易日
    trading_dates = get_date_range(start_date, end_date)
    logger.info(f"📊 共需处理 {len(trading_dates)} 个交易日")
    
    success_count = 0
    failed_dates = []
    
    for i, date in enumerate(trading_dates, 1):
        logger.info(f"📈 进度: {i}/{len(trading_dates)} - 处理日期: {date}")
        
        if fetch_single_day_factors(db, date, batch_size):
            success_count += 1
        else:
            failed_dates.append(date)
        
        # 每日处理完成后稍作休息
        time.sleep(1)
    
    # 输出最终统计
    logger.info(f"🏁 数据获取完成！成功: {success_count}/{len(trading_dates)} 天")
    if failed_dates:
        logger.warning(f"⚠️ 失败的日期: {failed_dates}")
    
    return success_count, failed_dates

# ============== 函数：重新获取失败的日期 ==============
def retry_failed_dates(db, failed_dates, batch_size=100):
    """重新获取失败的日期数据"""
    if not failed_dates:
        return
    
    logger = logging.getLogger(__name__)
    logger.info(f"🔄 开始重新获取失败的日期: {failed_dates}")
    
    for date in failed_dates:
        logger.info(f"🔄 重新获取 {date}")
        if fetch_single_day_factors(db, date, batch_size):
            logger.info(f"✅ {date} 重新获取成功")
        else:
            logger.error(f"❌ {date} 重新获取仍然失败")

# ============== 主程序 ==============
if __name__ == "__main__":
    # 初始化 rqdatac
    rqdatac.init()  # 替换成你的 rqdatac 账号
    
    # 初始化数据库连接
    db = DataBase_Position()
    
    # 设置日期范围
    start_date = "2020-01-01"
    end_date = "2025-09-19"
    
    # 设置批处理大小
    batch_size = 100
    
    try:
        # 执行数据获取
        success_count, failed_dates = fetch_and_insert_factors(db, start_date, end_date, batch_size)
        
        # 如果有失败的日期，询问是否重试
        if failed_dates:
            print(f"\n⚠️ 发现 {len(failed_dates)} 个失败的日期，是否重试？")
            retry_choice = input("输入 'y' 重试，其他键跳过: ").lower().strip()
            if retry_choice == 'y':
                retry_failed_dates(db, failed_dates, batch_size)
        
        print(f"\n🎉 程序执行完成！")
        
    except Exception as e:
        logging.error(f"❌ 程序执行出错: {e}")
        print(f"❌ 程序执行出错: {e}")
    
    finally:
        # 关闭数据库连接
        if hasattr(db, 'conn'):
            db.conn.close()
        print("📝 数据库连接已关闭")
