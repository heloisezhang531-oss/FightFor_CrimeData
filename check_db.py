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
                # 使用临时表去重方案（大数据量下比 DELETE JOIN 更快更稳）
                conn.execute(text("CREATE TABLE chicago_crimes_temp LIKE chicago_crimes"))
                conn.execute(text("INSERT INTO chicago_crimes_temp SELECT * FROM chicago_crimes GROUP BY ID"))
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
