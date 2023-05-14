from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from app.utils.database import Base


class Movie(Base):
    __tablename__ = "movies"

    tconst = Column(String, primary_key=True)
    titleType = Column(String, nullable=False)
    primaryTitle = Column(String, nullable=False)
    runtimeMinutes = Column(Integer, nullable=False)
    genres = Column(String, nullable=False)
    rating = relationship("Rating", viewonly=True)


class Rating(Base):
    __tablename__ = "ratings"

    tconst = Column(String, ForeignKey("movies.tconst"), primary_key=True)
    averageRating = Column(Float, nullable=False)
    numVotes = Column(Integer, nullable=False)
    movie = relationship("Movie", viewonly=True)
