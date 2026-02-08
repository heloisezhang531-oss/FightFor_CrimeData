import os
import time
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 目前FBI API Key不可用

# 加载环境变量
load_dotenv()

# --- 配置信息 ---
# User requested to use SOCRATA_APP_TOKEN
FBI_API_KEY = os.getenv("FBI_API_KEY") 
BASE_URL = "https://api.usa.gov/crime/fbi/sapi" 
# Chicago Police Department ORI
CHICAGO_ORI = "IL0160200"

# 数据库配置
TIDB_USER = os.getenv("TIDB_USER")
TIDB_PASSWORD = os.getenv("TIDB_PASSWORD")
TIDB_HOST = os.getenv("TIDB_HOST")
TIDB_PORT = os.getenv("TIDB_PORT")
TIDB_DB_NAME = os.getenv("TIDB_DB_NAME")
CA_PATH = os.getenv("TID_CA_PATH")

# 构建连接字符串
conn_str = f"mysql+pymysql://{TIDB_USER}:{TIDB_PASSWORD}@{TIDB_HOST}:{TIDB_PORT}/{TIDB_DB_NAME}?ssl_ca={CA_PATH}"
engine = create_engine(conn_str)

# NIBRS 表列表 (根据 NIBRS 数据结构)
# 注意: FBI API 的 endpoint 可能不完全对应这些表名，需要根据实际API文档调整
# 这里假设存在类似 /incident, /offense 等 endpoint，或者通过 fetch_nibrs_data 内部逻辑处理
NIBRS_TABLES = [
    "nibrs_incident",
    "nibrs_offense", 
    "nibrs_victim", 
    "nibrs_offender", 
    "nibrs_arrestee",
    "nibrs_property",
]

def get_headers():
    return {
        "User-Agent": "Mozilla/5.0",
        "Authorization": f"Basic {FBI_API_KEY}" if FBI_API_KEY else None
        # 注意：data.gov key 通常通过 url param 'api_key' 传递，或者 header 'X-Api-Key'
        # 如果是 data.gov, 通常是 ?api_key=XYZ
    }

def fetch_and_save_nibrs():
    # 每次请求的参数配置 (如果支持分页)
    limit = 10000
    
    # 遍历年份
    for year in range(2015, 2025):
        print(f"\n🚀 --- 检查年份: {year} ---")
        
        # 遍历每张表
        for table in NIBRS_TABLES:
            print(f"  📂 处理表: {table}")
            
            # --- 断点续传检查 ---
            offset = 0
            try:
                with engine.connect() as conn:
                    # 检查该表、该年份已有的记录数
                    # 注意: 需要确保数据库中已有该表，否则 count 会报错，这里加个简单的 try-except 忽略表不存在的情况
                    query = text(f"SELECT COUNT(*) FROM {table} WHERE data_year = :year")
                    offset = conn.execute(query, {"year": year}).scalar() or 0
            except Exception as e:
                # 表可能不存在，或者没有 data_year 字段
                # print(f"    (断点检查跳过: {e})")
                offset = 0

            if offset > 0:
                print(f"    🔄 {table} 发现断点：已存在 {offset} 条记录，尝试继续...")

            retry_count = 0
            
            while True:
                # 构造 API 请求
                # 注意: 这里是假设的 endpoint 结构，FBI CDE API 结构比较复杂，可能需要根据实际情况调整 url
                # 如果 API 不支持直接 table access，可能需要调用 summarized endpoint
                # 下面代码尝试使用 generic 的 endpoint 结构
                
                # 示例 URL 结构 (需验证): 
                # https://api.usa.gov/crime/fbi/cde/agency/IL0160200/nibrs/incident?year=2024&...
                # 实际 FBI API 往往需要 api_key 参数
                
                api_url = f"{BASE_URL}/agency/{CHICAGO_ORI}/{table}"
                params = {
                    "api_key": FBI_API_KEY,
                    "year": year,
                    "limit": limit,
                    "offset": offset,
                    # "page": ... (如果 API 使用 page 而不是 offset)
                }

                try:
                    # 1. 发送请求
                    response = requests.get(api_url, params=params, headers=get_headers())
                    
                    if response.status_code != 200:
                        print(f"    ❌ API 请求失败 [{response.status_code}]: {response.text[:100]}")
                        break # 跳过该表/该年，或者重试
                    
                    data = response.json()
                    results = data.get('results', []) # 假设返回结构中有 results 字段
                    
                    if not results:
                        print(f"    ✅ {table} {year} 年无更多数据")
                        break
                    
                    # 2. 清洗数据
                    df = pd.DataFrame(results)
                    
                    # 统一大写列名
                    df.columns = [col.upper() for col in df.columns]
                    
                    # 确保有一个 DATA_YEAR 字段用于断点续传 (如果 API 没返回，手动加上)
                    if 'DATA_YEAR' not in df.columns:
                        df['DATA_YEAR'] = year
                        
                     # 处理复杂字段 (转字符串或丢弃)
                    for col in df.columns:
                        if df[col].dtype == 'object':
                             # 简单的将 list/dict 转为 string 存储，或者直接 drop
                             df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)

                    # 3. 写入 TiDB
                    df.to_sql(table, engine, if_exists='append', index=False, chunksize=1000)
                    
                    records_count = len(results)
                    offset += records_count
                    print(f"    💾 已存入 {records_count} 条 (Total: {offset})")
                    
                    if records_count < limit:
                        break # 数据取完了
                        
                except Exception as e:
                    retry_count += 1
                    print(f"    ❌ 出错: {e}")
                    if retry_count > 5:
                        print("    🚫 重试次数过多，跳过当前表/年份")
                        break
                    time.sleep(2)
                    continue
                
                # 成功后重置 retry
                retry_count = 0

if __name__ == "__main__":
    if not FBI_API_KEY:
        print("⚠️ 警告: 未检测到 FBI_API_KEY，请在 .env 文件中配置。")
        # exit(1) # 可以选择退出，或者尝试无 key 访问 (通常受限)
    
    fetch_and_save_nibrs()