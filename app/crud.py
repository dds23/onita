import sqlalchemy as sa
from app.models import Movie, Rating
from app.schemas import MovieSchema, RatingSchema
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from app.utils.common import create_file
from sqlalchemy.sql import func
from sqlalchemy import update, and_


def upload_movie(movie: MovieSchema, db: Session, commit: bool = False):
    data_dict = dict(movie)
    stmt = insert(Movie).values(data_dict)
    del data_dict['tconst']
    stmt = stmt.on_conflict_do_update(
        index_elements=[Movie.tconst], set_=data_dict)
    db.execute(stmt)
    if commit:
        db.commit()


def upload_rating(rating: RatingSchema, db: Session, commit: bool = False):
    data_dict = dict(rating)
    stmt = insert(Rating).values(data_dict)
    del data_dict['tconst']
    stmt = stmt.on_conflict_do_update(
        index_elements=[Rating.tconst], set_=data_dict)
    db.execute(stmt)
    if commit:
        db.commit()


def get_top_n_movies(db: Session, limit):
    return db.query(Movie.tconst, Movie.primaryTitle, Movie.runtimeMinutes, Movie.genres).order_by(Movie.runtimeMinutes.desc()).limit(limit).all()


def get_top_rated_movies(db: Session, average_rating: float):
    return db.query(Rating.tconst, Movie.primaryTitle, Movie.genres, Rating.averageRating).join(Movie).filter(Rating.averageRating > average_rating).order_by(Rating.averageRating.desc()).all()


def get_genre_movies_with_subtotals(db: Session):
    p1 = sa.select(Movie.genres.label('genre'), func.sum(Rating.numVotes).label(
        'total_votes')).select_from(Movie).join(Rating).group_by(Movie.genres).cte('res')

    p2 = sa.select(Movie.tconst.label('id'), p1.c.genre, Movie.primaryTitle.label(
        'title'), p1.c.total_votes).select_from(p1).join(Movie, p1.c.genre == Movie.genres).cte('obj')

    final = sa.select(p2.c.genre, p2.c.title, Rating.numVotes.label('votes'), p2.c.total_votes).select_from(
        p2).join(Rating, p2.c.id == Rating.tconst).order_by(p2.c.genre, p2.c.title, Rating.numVotes).subquery()

    result = db.query(final).all()
    data = []
    val_list = []
    prev_val = result[0][3]
    val_list.append(prev_val)
    for row in result:
        val = row[3]
        if val != prev_val:
            val_list.append(val)
            data.append({'genre': '', 'title': 'TOTAL', 'votes': prev_val})
            prev_val = val
        temp = dict(row)
        del temp['total_votes']
        data.append(temp)

    response = create_file(data, val_list)
    return response


def update_runtime(documentary_time: int, animation_time: int, all_the_rest_time: int, db: Session, commit: bool = False):
    stmt1 = update(Movie).where(Movie.genres == 'Documentary').values(
        runtimeMinutes=Movie.runtimeMinutes+documentary_time)
    stmt2 = update(Movie).where(Movie.genres == 'Animation').values(
        runtimeMinutes=Movie.runtimeMinutes+animation_time)
    stmt3 = update(Movie).where(and_(Movie.genres != 'Documentary', Movie.genres !=
                                     'Animation')).values(runtimeMinutes=Movie.runtimeMinutes+all_the_rest_time)
    db.execute(stmt1)
    db.execute(stmt2)
    db.execute(stmt3)

    if commit:
        db.commit()
