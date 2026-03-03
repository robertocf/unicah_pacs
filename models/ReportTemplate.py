from config import db

class ReportTemplate(db.Model):
    __tablename__ = 'report_templates_app'
    pk = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(255), nullable=False)
    conteudo = db.Column(db.Text, nullable=False)
    font_family = db.Column(db.String(100), nullable=True, default='Times New Roman')
    font_size = db.Column(db.String(10), nullable=True, default='3')
    line_spacing = db.Column(db.String(10), nullable=True, default='1.2')
    text_align = db.Column(db.String(20), nullable=True, default='left')

    def to_dict(self):
        return {
            'pk': self.pk,
            'nome': self.nome,
            'conteudo': self.conteudo,
            'font_family': self.font_family or 'Times New Roman',
            'font_size': self.font_size or '3',
            'line_spacing': self.line_spacing or '1.2',
            'text_align': self.text_align or 'left',
        }
