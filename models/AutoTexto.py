from config import db

class AutoTexto(db.Model):
    __tablename__ = 'auto_textos'
    pk = db.Column(db.Integer, primary_key=True)
    codigo = db.Column(db.String(50), nullable=False)
    texto = db.Column(db.Text, nullable=False)
    modalidade = db.Column(db.String(50), nullable=False)
    user_pk = db.Column(db.Integer, db.ForeignKey('users_app.pk'), nullable=False)

    def to_dict(self):
        return {
            'pk': self.pk,
            'codigo': self.codigo,
            'texto': self.texto,
            'modalidade': self.modalidade,
            'user_pk': self.user_pk
        }
