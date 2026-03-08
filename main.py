import logging
import sys 
from pathlib import Path
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

def setup_logging(run_id: str) -> None:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_format = (
        f"%(asctime)s | run={run_id} | "
        f"%(levelname)s | %(name)s | %(message)s"
    )
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                log_dir / f"consumer_{run_id}.log"
            )
        ]
    )


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
    import uuid 
    run_id = str(uuid.uuid4())[:8]
    setup_logging(run_id)

    logger = logging.getLogger(__name__)
    logger.info(f"Starting Kafka pipeline - run_id: {run_id}")

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from src.consumer import run_consumer

    try:
        engine = get_engine()
        logger.info("Database engine created")
        run_consumer(engine)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)