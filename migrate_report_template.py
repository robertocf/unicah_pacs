"""
Migration: Add formatting columns to report_templates_app
Run: python migrate_report_template.py
"""
from config import db, app

CHECK_SQL = """
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'report_templates_app'
"""

COLUMNS = [
    ("font_family", "VARCHAR(100) DEFAULT 'Times New Roman'"),
    ("font_size",   "VARCHAR(10)  DEFAULT '3'"),
    ("line_spacing","VARCHAR(10)  DEFAULT '1.2'"),
    ("text_align",  "VARCHAR(20)  DEFAULT 'left'"),
]

with app.app_context():
    with db.engine.connect() as conn:
        result = conn.execute(db.text(CHECK_SQL))
        existing = {row[0] for row in result}
        print("Colunas existentes:", existing)

        for col_name, col_def in COLUMNS:
            if col_name in existing:
                print(f"  SKIP (já existe): {col_name}")
            else:
                sql = f"ALTER TABLE report_templates_app ADD COLUMN {col_name} {col_def}"
                conn.execute(db.text(sql))
                print(f"  CRIADO: {col_name}")

        conn.commit()
    print("Migração concluída!")
