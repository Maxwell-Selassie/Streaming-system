from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Goal - Test connection to Postgres database
load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

def get_engine():
    """Test connection to Postgres database"""
    url = (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    return create_engine(url)

if __name__ == "__main__":
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT version();"))
        conn.commit()
