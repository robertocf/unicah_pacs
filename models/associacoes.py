from config import db

class UserCompany(db.Model):
    __tablename__ = 'user_company'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users_app.pk'))
    company_id = db.Column(db.Integer, db.ForeignKey('companies.pk'))

def get_user_companies(user_id):
    """Retorna todas as empresas associadas a um usuário."""
    return UserCompany.query.filter_by(user_id=user_id).all()

def save_user_companies(user_id, company_ids):
    """Salva ou atualiza as associações de um usuário com empresas."""
    # Remove associações existentes
    UserCompany.query.filter_by(user_id=user_id).delete()
    
    # Cria novas associações
    for company_id in company_ids:
        association = UserCompany(user_id=user_id, company_id=company_id)
        db.session.add(association)
    
    try:
        db.session.commit()
        return True, "Associações salvas com sucesso!"
    except Exception as e:
        db.session.rollback()
        return False, str(e)

def delete_user_companies(user_id):
    """Remove todas as associações de um usuário."""
    try:
        UserCompany.query.filter_by(user_id=user_id).delete()
        db.session.commit()
        return True, "Associações removidas com sucesso!"
    except Exception as e:
        db.session.rollback()
        return False, str(e)