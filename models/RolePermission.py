from config import db

class RolePermission(db.Model):
    __tablename__ = 'role_permissions_app'
    pk = db.Column(db.Integer, primary_key=True)
    role_name = db.Column(db.String(255), nullable=False)
    permission_key = db.Column(db.String(255), nullable=False)

    __table_args__ = (
        db.UniqueConstraint('role_name', 'permission_key', name='_role_permission_uc'),
    )

def get_permissions_by_role(role_name):
    """Retorna uma lista de chaves de permissão para um determinado grupo."""
    perms = RolePermission.query.filter_by(role_name=role_name).all()
    return [p.permission_key for p in perms]

def update_role_permissions(role_name, permission_keys):
    """Atualiza as permissões de um grupo, removendo as antigas e adicionando as novas."""
    try:
        # Remover permissões existentes para este papel
        RolePermission.query.filter_by(role_name=role_name).delete()
        
        # Adicionar novas permissões
        for key in permission_keys:
            new_perm = RolePermission(role_name=role_name, permission_key=key)
            db.session.add(new_perm)
            
        db.session.commit()
        return True, "Permissões atualizadas com sucesso"
    except Exception as e:
        db.session.rollback()
        return False, str(e)
