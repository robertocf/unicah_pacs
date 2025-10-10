from flask_login import UserMixin
from config import db

class User(UserMixin, db.Model):
    __tablename__ = 'users_app'
    pk = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.String(255), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(255))
    role = db.Column(db.String(255))
    active = db.Column(db.Boolean, default=True)

    def get_id(self):
        return str(self.pk)