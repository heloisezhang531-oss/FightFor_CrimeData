import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

TIDB_USER = os.getenv("TIDB_USER")
TIDB_PASSWORD = os.getenv("TIDB_PASSWORD")
TIDB_HOST = os.getenv("TIDB_HOST")
TIDB_PORT = os.getenv("TIDB_PORT")
CA_PATH = os.getenv("TID_CA_PATH")

TIDB_DB_NAME = os.getenv("TIDB_DB_NAME") or "Chicago_data"

# 连接到指定数据库
url = f"mysql+pymysql://{TIDB_USER}:{TIDB_PASSWORD}@{TIDB_HOST}:{TIDB_PORT}/{TIDB_DB_NAME}?ssl_ca={CA_PATH}"
engine = create_engine(url)

with engine.connect() as conn:
    # 1. 检查数据库列表
    result = conn.execute(text("SHOW DATABASES"))
    print("--- 1. 可用数据库 ---")
    for row in result:
        print(f"- {row[0]}")
    
    # 2. 检查数据表
    print(f"\n--- 2. {TIDB_DB_NAME} 中的表 ---")
    tables = conn.execute(text("SHOW TABLES"))
    has_table = False
    for row in tables:
        print(f"- {row[0]}")
        if row[0] == 'chicago_crimes':
            has_table = True
            
    if has_table:
        # 3. 统计总行数与重复项
        print("\n--- 3. 数据统计 ---")
        stats = conn.execute(text("""
            SELECT 
                COUNT(*) as total, 
                COUNT(DISTINCT ID) as unique_ids
            FROM chicago_crimes
        """)).mappings().first()
        
        total = stats['total']
        unique = stats['unique_ids']
        duplicates = total - unique
        
        print(f"总记录数: {total}")
        print(f"唯一 ID 数: {unique}")
        print(f"重复记录数: {duplicates}")
        
        if duplicates > 0:
            print(f"\n⚠️ 发现 {duplicates} 条重复数据！")
            confirm = input("是否执行清理（保留唯一记录）? (y/n): ")
            if confirm.lower() == 'y':
                print("正在清理重复项，请稍候...")
                # 方案 C：分批次插入 + 禁用 strict mode
                # 大数据量 (300万行) 一次性 INSERT ... GROUP BY 容易导致 TiDB Serverless 超时 (Lost connection)
                # 我们改为分批次搬运数据
                conn.execute(text("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))"))
                
                conn.execute(text("DROP TABLE IF EXISTS chicago_crimes_temp"))
                conn.execute(text("CREATE TABLE chicago_crimes_temp LIKE chicago_crimes"))
                
                # 获取总行数，用于进度条
                total_rows = conn.execute(text("SELECT COUNT(*) FROM chicago_crimes")).scalar()
                batch_size = 50000
                print(f"开始分批处理，每批 {batch_size} 条，预计 {total_rows // batch_size + 1} 批...")
                
                
                # 修正后的策略：
                # 1. 创建 temp 表
                # 2. 给 temp 表的 ID 加上 UNIQUE 索引 (如果原表没有)
                # 3. 分批 INSERT IGNORE 由于 ID 重复会被忽略

                
                try:
                    conn.execute(text("ALTER TABLE chicago_crimes_temp ADD UNIQUE INDEX idx_id_unique (ID)"))
                except Exception as e:
                    print("索引可能已存在，跳过创建")

                offset = 0
                while True:
                    # 使用 INSERT IGNORE ... SELECT ... LIMIT ...
                    # 注意：TiDB/MySQL 的 INSERT IGNORE ... SELECT ... LIMIT 有时不支持 OFFSET
                    # 更稳妥的是：每次读一批，在 Python 里不做处理，直接 INSERT IGNORE 到库里? 不，那样网络开销大。
                    
                    # 让我们尝试纯 SQL 的分批：
                    # INSERT IGNORE INTO temp SELECT * FROM source LIMIT 50000 OFFSET 0;
                    result = conn.execute(text(f"""
                        INSERT IGNORE INTO chicago_crimes_temp 
                        SELECT * FROM chicago_crimes LIMIT {batch_size} OFFSET {offset}
                    """))
                    conn.commit() # 每批提交
                    
                    rows = result.rowcount
                    offset += batch_size
                    print(f"已处理偏移量 {offset}...")
                    
                    if rows == 0:
                        break

                conn.execute(text("DROP TABLE chicago_crimes"))
                conn.execute(text("ALTER TABLE chicago_crimes_temp RENAME TO chicago_crimes"))
                
                conn.commit()



                print("✅ 清理完成！")
        
        # 4. 查看最新存入的 5 条数据
        print(f"\n--- 4. 最新 5 条记录预览 ---")
        preview = conn.execute(text("SELECT ID, DATE, PRIMARY_TYPE FROM chicago_crimes ORDER BY DATE DESC LIMIT 5"))
        for row in preview:
            print(f"ID: {row[0]}, 时间: {row[1]}, 类型: {row[2]}")

    else:
        print("\n❌ 尚未发现 'chicago_crimes' 表，请确认拉取脚本是否已开始写入。")
