from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
import os

SQLALCHEMY_DATABASE_URL = os.environ["DB_URL"]

engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_size=5, connect_args={
                       "options": "-c timezone=utc"}, pool_pre_ping=True,
                       pool_recycle=600)

SessionLocal = sessionmaker(
    autocommit=False, autoflush=True, bind=engine)

meta = MetaData(bind=engine)
MetaData.reflect(meta)

Base = declarative_base()


def get_db():
    db = scoped_session(SessionLocal)
    try:
        yield db
    finally:
        db.close()
