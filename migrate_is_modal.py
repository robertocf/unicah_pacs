from config import app, db
from sqlalchemy import text

def migrate():
    with app.app_context():
        try:
            print("Iniciando migração: adicionando coluna 'is_modal' à tabela 'system_updates_app'...")
            
            # Verificar se a coluna já existe
            check_sql = """
                SELECT count(*) 
                FROM information_schema.columns 
                WHERE table_name='system_updates_app' AND column_name='is_modal';
            """
            result = db.session.execute(text(check_sql)).fetchone()
            
            if result[0] == 0:
                print("Coluna 'is_modal' não encontrada. Adicionando...")
                add_sql = "ALTER TABLE system_updates_app ADD COLUMN is_modal BOOLEAN DEFAULT FALSE;"
                db.session.execute(text(add_sql))
                db.session.commit()
                print("Coluna adicionada com sucesso!")
            else:
                print("Coluna 'is_modal' já existe. Pulando...")
            
            print("Migração concluída!")
        except Exception as e:
            db.session.rollback()
            print(f"Erro durante a migração: {e}")

if __name__ == "__main__":
    migrate()
