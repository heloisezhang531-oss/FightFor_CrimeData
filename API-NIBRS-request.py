import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# --- 配置信息 ---
# Local Data Directory
DATA_DIR = r"D:\NUS\IT5006\project\FBI data"

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
NIBRS_TABLES = [
    "nibrs_weapon",
    "nibrs_criminal_act",
    "nibrs_victim_offense",
    "nibrs_victim_offender_rel",
    "nibrs_victim_injury",
    "nibrs_victim_circumstances",
    "nibrs_victim",
    "nibrs_suspected_drug",
    "nibrs_suspect_using",
    "nibrs_property_desc",
    "nibrs_property",
    "nibrs_offense",
    "nibrs_offender",
    "nibrs_incident",
    "nibrs_month",
    "nibrs_bias_motivation",
    "nibrs_arrestee_weapon",
    "nibrs_arrestee",
    "cde_agencies",
    "agency_participation",
    "nibrs_age",
    "nibrs_arrest_type",
    "nibrs_assignment_type",
    "nibrs_bias_list",
    "nibrs_circumstances",
    "nibrs_cleared_except",
    "nibrs_criminal_act_type",
    "nibrs_drug_measure",
    "nibrs_drug_measure_type",
    "nibrs_ethnicity",
    "nibrs_injury",
    "nibrs_justifiable_force",
    "nibrs_location_type",
    "nibrs_offense_type",
    "nibrs_prop_desc_type",
    "nibrs_prop_loss_type",
    "nibrs_relationship",
    "nibrs_suspected_drug_type",
    "nibrs_using_list",
    "nibrs_victim_type",
    "nibrs_weapon_type"
]

from sqlalchemy import create_engine, text, inspect

# ... (imports remain the same)

def process_and_upload_local_data():
    """
    遍历本地文件夹 (IL-2015 到 IL-2024)，读取 CSV 文件并上传到 TiDB。
    """
    inspector = inspect(engine)

    # 遍历年份
    for year in range(2015, 2025):
        year_folder_name = f"IL-{year}"
        year_dir_path = os.path.join(DATA_DIR, year_folder_name)
        
        print(f"\n🚀 --- 处理年份: {year} (文件夹: {year_folder_name}) ---")
        
        if not os.path.exists(year_dir_path):
             print(f"    ⚠️ 文件夹不存在: {year_dir_path}，跳过该年份。")
             continue

        # 遍历每张表
        for table in NIBRS_TABLES:
            csv_filename = f"{table}.csv"
            csv_file_path = os.path.join(year_dir_path, csv_filename)
            
            print(f"  📂 处理表: {table} (文件: {csv_filename})")
            
            if not os.path.exists(csv_file_path):
                print(f"    ⚠️ 文件不存在: {csv_file_path}，跳过。")
                continue
            
            # Check table existence via Inspector
            table_exists = False
            db_columns = []
            try:
                # Refresh inspector implies just calling checks, but inspector object might cache? 
                # Safer to verify existence directly or rely on engine.
                if inspector.has_table(table):
                    table_exists = True
                    # Get columns
                    columns_info = inspector.get_columns(table)
                    db_columns = [col['name'] for col in columns_info]
                    # print(f"    📋 Table found. Columns: {db_columns}")
                else:
                    # print(f"    🆕 Table {table} does not exist. Will create.")
                    table_exists = False
            except Exception as e:
                print(f"    ⚠️ Error checking table {table}: {e}")
                # Fallback to assuming not exists or connection issue
                table_exists = False

            try:
                # 1. 读取 CSV 数据
                chunk_size = 10000
                total_records = 0
                
                if os.path.getsize(csv_file_path) < 100:
                     df_peek = pd.read_csv(csv_file_path, nrows=1)
                     if df_peek.empty:
                         print(f"    ⚠️ 文件为空或无数据，跳过。")
                         continue

                for chunk_idx, df in enumerate(pd.read_csv(csv_file_path, chunksize=chunk_size)):
                    
                    # 2. 清洗数据
                    # 统一转小写
                    df.columns = [col.lower() for col in df.columns]
                    
                    # 确保有一个 data_year 字段
                    if 'data_year' not in df.columns:
                        df['data_year'] = year

                    df_final = df
                    
                    if table_exists:
                        # 过滤列：只保留 DB 中存在的列 (严格模式)
                        valid_columns = []
                        db_col_set = set([c.lower() for c in db_columns])
                        
                        for col in df.columns:
                            if col.lower() in db_col_set:
                                valid_columns.append(col)
                        
                        df_final = df[valid_columns].copy()
                        
                        # DEBUG
                        if chunk_idx == 0:
                            dropped = set(df.columns) - set(valid_columns)
                            if dropped:
                                print(f"    ℹ️ (表已存在) 丢弃 CSV 中多余的列: {dropped}")
                    else:
                        if chunk_idx == 0:
                            print(f"    🆕 (表不存在) 准备数据用于新建表...")

                     # 处理复杂字段
                    for col in df_final.columns:
                        if df_final[col].dtype == 'object':
                             df_final[col] = df_final[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)

                    # 3. 写入 TiDB
                    start_time = time.time()
                    try:
                        # if_exists='append': works for both new (creates) and existing.
                        df_final.to_sql(table, engine, if_exists='append', index=False, chunksize=1000)
                        cost = time.time() - start_time
                        
                        records_count = len(df_final)
                        total_records += records_count
                        print(f"    💾 Chunk {chunk_idx+1}: resource saved {records_count} records ({cost:.2f}s)")
                        
                    except Exception as sql_err:
                        print(f"    ❌ 写入数据库失败 (Chunk {chunk_idx+1}): {sql_err}")
                        break

                print(f"    ✅ {table} {year} 完成，共处理 {total_records} 条记录。")

            except Exception as e:
                print(f"    ❌ 读取或处理文件失败: {e}")
                continue

if __name__ == "__main__":
    if not os.path.exists(DATA_DIR):
         print(f"❌ 错误: 数据目录不存在 -> {DATA_DIR}")
         print("请确认路径是否正确。")
    else:
        process_and_upload_local_data()