from app import crud
from app.models import Movie, Rating
from app.schemas import MovieSchema, RatingSchema
from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from enum import Enum
from app.utils.common import CustomException, get_chunk_size, divide_chunks, parallelize
import pandas as pd
import os


class TableName(str, Enum):
    movies = "movies"
    ratings = "ratings"


class_map = {
    "movies": MovieSchema,
    "ratings": RatingSchema,
}

upload_function_map = {
    "movies": crud.upload_movie,
    "ratings": crud.upload_rating
}


get_class_by_tablename = {
    "movies": Movie,
    "ratings": Rating
}


column_names_mapping = {
    "movies": 'Movie',
    "ratings": 'Rating'
}

column_check_list = ['tconst', 'primaryTitle']


def validate_data(data):
    for column_name in data.columns:
        if column_name in column_check_list:
            column_to_get = column_name
            column_name_list = [column_name]
            if column_to_get == 'primaryTitle':
                column_name_list = [column_to_get, 'titleType']
            repeat_list = data[data.duplicated(
                column_name_list)][column_to_get].unique().tolist()
            repeat_list = repeat_list[:min(3, len(repeat_list))]
            if repeat_list != []:
                len_check = (1 if len(repeat_list) > 1 else 0)
                detail = ', '.join([col.capitalize() for col in column_name_list]) + ('s ' if len_check else ' ') + ', '.join(repeat_list) + \
                    (' have' if len_check else ' has') + \
                    ' multiple occurrences' + '. Only 1 occurrence is allowed to upload'
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


def validate_template(data, table_name):
    file_name = 'app/templates/' + \
        column_names_mapping[table_name] + '_template.csv'
    file = pd.read_csv(file_name, nrows=0)
    column_list = list(file.columns)
    data_column_list = list(data.columns)
    if column_list != data_column_list:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail='Wrong template uploaded. Upload a template of ' + table_name)


def file_handling(file, table_name):
    try:
        data = pd.read_csv(file)
        str_value = os.environ["STR_VALUE"]
        data.fillna(str_value, inplace=True)
    except Exception as e:
        raise CustomException(
            status_code=status.HTTP_400_BAD_REQUEST, message="File format should be csv")

    validate_data(data)
    validate_template(data, table_name)
    return data


def read_and_upload_data(data: pd.DataFrame, table_name: str, db: Session):
    for index, row in data.iterrows():
        try:
            upload_function_map[table_name](
                class_map[table_name](**row), db=db)

        except HTTPException as http_exception:
            raise CustomException(
                status_code=http_exception.status_code, message=http_exception.detail + ' [row ' + str(index+1)+']')
        except Exception as e:
            message = str(e).replace('\n', ' ')
            raise CustomException(
                status_code=status.HTTP_400_BAD_REQUEST, message=" ".join(message.split()) + ' [row ' + str(index+1)+']')

    db.commit()


def upload_data(args, **kwargs):
    [table_name, db] = args
    for key, value in kwargs.items():
        data = value
    read_and_upload_data(data, table_name, db)


def make_input_list(data):
    input_list = []
    for i in range(len(data)):
        input_list.append({str(i): data[i]})
    return input_list


def upload_file(file, db: Session, table_name: TableName):

    data = file_handling(file=file, table_name=table_name)
    chunk_size = get_chunk_size(len(data.index))
    data = list(divide_chunks(data, chunk_size))
    data = make_input_list(data)
    parallelize(upload_data, table_name, db, data)
    return "File successfully uploaded"
