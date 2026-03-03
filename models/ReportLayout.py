from config import db

class ReportLayout(db.Model):
    __tablename__ = 'report_layouts'
    pk = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(255), nullable=False)
    cabecalho = db.Column(db.Text, nullable=True)
    rodape = db.Column(db.Text, nullable=True)
    font_family = db.Column(db.String(100), default="'Times New Roman', Times, serif")
    font_size = db.Column(db.String(20), default="14pt")
    is_default = db.Column(db.Boolean, default=False)

    def to_dict(self):
        return {
            'pk': self.pk,
            'nome': self.nome,
            'cabecalho': self.cabecalho,
            'rodape': self.rodape,
            'font_family': self.font_family,
            'font_size': self.font_size,
            'is_default': self.is_default
        }
