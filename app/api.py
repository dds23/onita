from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.utils.database import get_db
from app.utils import upload
from app.utils.common import CustomException
from fastapi import Depends, File, UploadFile, HTTPException, status
from enum import Enum
from starlette.responses import FileResponse
from . import crud
from .schemas import MovieSchema


router = APIRouter()


class TableName(str, Enum):
    movies = "movies"
    ratings = "ratings"


class FileName(str, Enum):
    Movie_template = "Movie_template"
    Rating_template = "Rating_template"


@router.get("/api/v1/template", description='Download template for data upload')
def get_template(file_name: FileName):
    return FileResponse(path='app/templates/' + file_name + '.csv', media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', filename=file_name + '.csv')


@router.post("/api/v1/upload", description='Upload data to table from template')
def upload_template(table_name: TableName, file: UploadFile = File(...), db: Session = Depends(get_db)):
    return upload.upload_file(file.file, db=db, table_name=table_name)


@router.get("/api/v1/longest-duration-movies")
def get_top_n_movies(limit: int = 10, db: Session = Depends(get_db)):
    return crud.get_top_n_movies(db, limit)


@router.post("/api/v1/new-movie")
def upload_movie(movie: MovieSchema, db: Session = Depends(get_db)):
    try:
        crud.upload_movie(movie, db, commit=True)
        db.commit()
        return "Success"
    except HTTPException as http_exception:
        raise CustomException(
            status_code=http_exception.status_code, message=http_exception.detail)
    except Exception as e:
        message = str(e).replace('\n', ' ')
        raise CustomException(status_code=status.HTTP_400_BAD_REQUEST,
                              message=message+" Could not upload movie")


@router.get("/api/v1/top-rated-movies")
def get_top_rated_movies(averageRating: float = 6.0, db: Session = Depends(get_db)):
    return crud.get_top_rated_movies(db, averageRating)


@router.get("/api/v1/genre-movies-with-subtotals")
def get_genre_movies_with_subtotals(db: Session = Depends(get_db)):
    return crud.get_genre_movies_with_subtotals(db)


@router.post("/api/v1/update-runtime-minutes")
def update_runtimes(documentary_time: int = 15, animation_time: int = 30, all_the_rest_time: int = 45, db: Session = Depends(get_db)):
    try:
        crud.update_runtime(documentary_time, animation_time,
                            all_the_rest_time, db, commit=True)
        return "Runtimes successfully updated"
    except HTTPException as http_exception:
        raise CustomException(
            status_code=http_exception.status_code, message=http_exception.detail)
    except Exception as e:
        message = str(e).replace('\n', ' ')
        raise CustomException(status_code=status.HTTP_400_BAD_REQUEST,
                              message=message+" Could not update runtimes")
