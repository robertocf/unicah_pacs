from config import app, db
from sqlalchemy import text

def migrate():
    with app.app_context():
        try:
            # Check if column already exists
            result = db.session.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='study' AND column_name='doctor_id';"))
            if result.fetchone():
                print("Column 'doctor_id' already exists in 'study' table.")
                return

            # Add column
            db.session.execute(text("ALTER TABLE study ADD COLUMN doctor_id INTEGER REFERENCES users_app(pk);"))
            db.session.commit()
            print("Successfully added 'doctor_id' column to 'study' table.")
        except Exception as e:
            db.session.rollback()
            print(f"Error during migration: {e}")

if __name__ == "__main__":
    migrate()
