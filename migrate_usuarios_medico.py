"""
Migration: Add medical columns to users_app
Run: python migrate_usuarios_medico.py
"""
from config import db, app

CHECK_SQL = """
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'users_app'
"""

COLUMNS = [
    ("is_medico", "BOOLEAN DEFAULT FALSE"),
    ("crm",       "VARCHAR(50)"),
    ("conselho",  "VARCHAR(50)"),
    ("estado",    "VARCHAR(2)"),
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
                sql = f"ALTER TABLE users_app ADD COLUMN {col_name} {col_def}"
                conn.execute(db.text(sql))
                print(f"  CRIADO: {col_name}")

        conn.commit()
    print("Migração concluída!")
