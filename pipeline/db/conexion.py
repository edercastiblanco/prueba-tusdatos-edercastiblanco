import os

from dotenv import load_dotenv
from sqlalchemy import create_engine


def build_engine():
    """Construye la conexion SQLAlchemy usando variables de entorno."""
    load_dotenv()
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    return create_engine(db_url)
