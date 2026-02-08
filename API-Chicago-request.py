import os
import time
import pandas as pd
from sodapy import Socrata
# 导入 text 用于 SQL 查询
from sqlalchemy import create_engine, text

# ... (省略中间代码)



from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 从环境变量获取配置
SOCRATA_DOMAIN = os.getenv("SOCRATA_DOMAIN")
DATASET_ID = os.getenv("SOCRATA_DATASET_ID")
APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

TIDB_USER = os.getenv("TIDB_USER")
TIDB_PASSWORD = os.getenv("TIDB_PASSWORD")
TIDB_HOST = os.getenv("TIDB_HOST")
TIDB_PORT = os.getenv("TIDB_PORT")
TIDB_DB_NAME = os.getenv("TIDB_DB_NAME")
CA_PATH = os.getenv("TID_CA_PATH")

# 构建连接字符串 (使用 pymysql 驱动)
# 注意：TiDB Cloud Serverless 建议在连接字符串中包含 SSL 配置
conn_str = f"mysql+pymysql://{TIDB_USER}:{TIDB_PASSWORD}@{TIDB_HOST}:{TIDB_PORT}/{TIDB_DB_NAME}?ssl_ca={CA_PATH}"

client = Socrata(SOCRATA_DOMAIN, APP_TOKEN, timeout=60)
engine = create_engine(conn_str)

def fetch_and_save_all():
    # 每次拉取的大小
    limit = 10000 
    
    # 抓取过去 10 年 (2015 - 2024)
    for year in range(2015, 2025):
        print(f"\n🚀 --- 检查年份: {year} ---")
        offset = 0
        total_year_records = 0
        
        # --- 断点续传逻辑 ---
        try:
            with engine.connect() as conn:
                # 统计该年份已存在的行数
                query = text("SELECT COUNT(*) FROM chicago_crimes WHERE YEAR = :year")
                offset = conn.execute(query, {"year": str(year)}).scalar() or 0
        except Exception:
            offset = 0
            
        if offset > 0:
            print(f"🔄 发现断点：该年份已存在 {offset} 条记录，将从此处继续抓取...")
            total_year_records = offset

        retry_count = 0

        
        while True:
            # SoQL 筛选
            where_clause = f"date >= '{year}-01-01T00:00:00' and date <= '{year}-12-31T23:59:59'"
            
            try:
                # 1. API 拉取
                results = client.get(
                    DATASET_ID, 
                    where=where_clause, 
                    limit=limit, 
                    offset=offset, 
                    order="date ASC"
                )
                
                if not results:
                    break # 该年抓完
                
                # 2. 清洗
                batch_df = pd.DataFrame.from_records(results)
                
                # 重要：移除包含字典的字段（如 'location'），否则会报 "dict can not be used as parameter"
                # 这些复杂字段 SQL 无法直接处理
                if 'location' in batch_df.columns:
                    batch_df = batch_df.drop(columns=['location'])
                
                batch_df.columns = [col.upper() for col in batch_df.columns]
                if 'DATE' in batch_df.columns:
                    batch_df['DATE'] = pd.to_datetime(batch_df['DATE'])
                
                # 3. 写入 TiDB
                batch_df.to_sql("chicago_crimes", engine, if_exists='append', index=False, chunksize=1000)

                
                records_in_batch = len(results)
                offset += records_in_batch
                total_year_records += records_in_batch
                print(f"✅ 进度: {year} 年已存入 {offset} 条记录")
                
                # 如果拉取的数量少于 limit，说明这一年也抓完了
                if records_in_batch < limit:
                    break
                    
            except Exception as e:
                retry_count += 1
                print(f"❌ 出错 (年份 {year}, 偏移量 {offset}): {e}")
                
                if retry_count > 10:
                    print("🚫 错误尝试超过 10 次，停止脚本运行。请检查数据库连接或网络配置。")
                    exit(1)
                
                print(f"⚠️ 第 {retry_count} 次重试... 等待 2 秒")
                time.sleep(2)
                continue
                
            # 成功抓取一次后重置重试计数
            retry_count = 0 

                
        print(f"✨ {year} 年抓取完毕，共计 {total_year_records} 条")

if __name__ == "__main__":
    fetch_and_save_all()