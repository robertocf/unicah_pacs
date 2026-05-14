from config import app, db
from models.SystemAnnouncement import SystemSettings, SystemUpdate

def init_db():
    with app.app_context():
        # Cria as tabelas se não existirem
        db.create_all()
        
        # Verifica se já existe uma configuração
        settings = SystemSettings.query.get(1)
        if not settings:
            settings = SystemSettings(pk=1, maintenance_mode=False, maintenance_message="O sistema passará por uma atualização programada hoje às 22:00.")
            db.session.add(settings)
            
        # Adiciona alguns dados iniciais se a tabela de updates estiver vazia
        if not SystemUpdate.query.first():
            updates = [
                SystemUpdate(category='improvement', content='Adicionado opção de Layout de Laudo.'),
                SystemUpdate(category='improvement', content='Adicionado função de modelo de laudo'),
                SystemUpdate(category='improvement', content='Adicionado opção de auto-texto'),
                SystemUpdate(category='release_note', content='Adicionado novas funcionadades para área médica'),
                SystemUpdate(category='release_note', content='Criação de menu Lixeira'),
                SystemUpdate(category='release_note', content='Alterado opção de deletar estudo'),
            ]
            db.session.add_all(updates)
            
        db.session.commit()
        print("Tabelas de avisos e alertas inicializadas com sucesso!")

if __name__ == '__main__':
    init_db()
