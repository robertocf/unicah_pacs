from config import db
from datetime import datetime

class SystemSettings(db.Model):
    __tablename__ = 'system_settings_app'
    pk = db.Column(db.Integer, primary_key=True)
    maintenance_mode = db.Column(db.Boolean, default=False)
    maintenance_message = db.Column(db.String(500), default="O sistema está em manutenção no momento.")
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class SystemUpdate(db.Model):
    __tablename__ = 'system_updates_app'
    pk = db.Column(db.Integer, primary_key=True)
    category = db.Column(db.String(50), nullable=False)  # 'improvement' ou 'release_note'
    content = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    active = db.Column(db.Boolean, default=True)
    is_modal = db.Column(db.Boolean, default=False)

    def to_dict(self):
        return {
            'pk': self.pk,
            'category': self.category,
            'content': self.content,
            'created_at': self.created_at.strftime('%d/%m/%Y %H:%M'),
            'active': self.active,
            'is_modal': self.is_modal
        }
