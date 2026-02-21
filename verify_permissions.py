import sys
import os

# Adiciona o diretório raiz ao path para importar os módulos do app
sys.path.append(os.getcwd())

from config import app, db
from models.RolePermission import RolePermission, update_role_permissions, get_permissions_by_role
from services.permissions import get_user_permissions

class MockUser:
    def __init__(self, role):
        self.role = role
        self.user_id = 'test_user'
        self.name = 'Test User'

def verify():
    with app.app_context():
        # 1. Testar se a tabela é criada/está acessível
        print("Verificando acesso ao banco...")
        try:
            RolePermission.query.first()
            print("OK.")
        except Exception as e:
            print(f"Erro: Tabela pode não existir ainda. Erro: {e}")
            print("Tentando criar tabelas...")
            db.create_all()
            print("Tabelas criadas.")

        # 2. Testar fallback hardcoded para admin (sem dados no banco)
        print("\nTestando fallback para admin (banco vazio)...")
        RolePermission.query.delete()
        db.session.commit()
        
        admin_user = MockUser('admin')
        perms = get_user_permissions(admin_user)
        if perms.get('visualizar_relatorios') == True:
            print("OK: Admin manteve permissões fixas.")
        else:
            print("FALHA: Fallback do admin não funcionou.")

        # 3. Testar atualização dinâmica
        print("\nTestando atualização dinâmica para 'medico'...")
        role = 'medico'
        test_perms = ['visualizar_estudos', 'visualizar_relatorios']
        
        success, msg = update_role_permissions(role, test_perms)
        if success:
            print("OK: Permissões atualizadas no banco.")
            
            medico_user = MockUser('medico')
            current_perms = get_user_permissions(medico_user)
            
            if current_perms.get('visualizar_relatorios') == True and current_perms.get('acessar_gerencial') == False:
                print("SUCESSO: Permissões dinâmicas aplicadas corretamente!")
            else:
                print(f"FALHA: Permissões não correspondem ao esperado. {current_perms}")
        else:
            print(f"FALHA ao atualizar banco: {msg}")

        # Limpar teste
        RolePermission.query.delete()
        db.session.commit()
        print("\nTeste concluído.")

if __name__ == "__main__":
    verify()
