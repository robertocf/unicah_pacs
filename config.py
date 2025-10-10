from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

SERVER_IP='10.2.0.10'
SERVER_DB='pacsdb'
SERVER_USER='postgres'
SERVER_PASS='roberto'
SERVER_PORT='5432'

NGINX_AUTH_USER='suporte_image'
NGINX_AUTH_PASSWORD='$apr1$PefDLttp$C.smY/9DZ9PB4ZYaRmria0'

app = Flask(__name__)
app.config['SECRET_KEY'] = 'ohhr6T8UmvdC4Ws8Gn1q1pEZ2B5YfF8qDeag9nfe1ojeXVa6OzPb0W7BCVWrIAJgS66XmTrRWiaPzbmEi3uC7zsQKruYS1Q5u9a36GcJCfx2w1jTSAbWW8joG5jkvp53lHA5g93i0452LO4wQRJU8bhDAlYxRhiCMZhEYIkuEjqkpqCQnYcE4BASv6DDMPZv'
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{SERVER_USER}:{SERVER_PASS}@{SERVER_IP}/{SERVER_DB}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['NGINX_AUTH_USER'] = {NGINX_AUTH_USER}
app.config['NGINX_AUTH_PASSWORD'] = {NGINX_AUTH_PASSWORD}

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'