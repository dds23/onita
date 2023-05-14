import os
import openpyxl
import pandas as pd
from tqdm import tqdm
from io import BytesIO
from typing import List
from openpyxl.styles import Font
from fastapi import HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel, validator
from tempfile import NamedTemporaryFile
from fastapi.responses import StreamingResponse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed


class CustomModel(BaseModel):
    class Config:
        orm_mode = True

    @validator(
        'tconst',
        'titleType',
        'primaryTitle',
        'runtimeMinutes',
        'genres',
        'averageRating',
        'numVotes',
        pre=True,
        check_fields=False
    )
    def valid_type(cls, value, field):
        if field.name == 'runtimeMinutes' or field.name == 'numVotes':
            check_field = int
        elif field.name == 'averageRating':
            check_field = float
        else:
            check_field = str

        type_dict = {'str': 'string', 'int': 'integer', 'float': 'float'}

        str_value = os.environ["STR_VALUE"]
        if not isinstance(value, check_field) or value == str_value:
            if value == str_value:
                detail = " cannot be empty"
            else:
                detail = " should be of type " + \
                    type_dict[eval(str(check_field).split(" ")[-1][:-1])]

            raise HTTPException(status_code=400, detail=str(cls).split(
                ".")[-1][:-2] + ' field ' + field.name + detail)

        return value


class CustomException(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message


def update_values_in_table(input_row: classmethod, db_row: classmethod, db: Session):
    input_row_dict = input_row.__dict__
    db_row_dict = db_row.__dict__

    for key, value in input_row_dict.items():
        if value != 'NA' and db_row_dict[key] != value:
            setattr(db_row, key, value)


def modify_file(handle: BytesIO):
    wb = openpyxl.load_workbook(handle)
    sheet = wb.active

    heading_list = ['Genre', 'primaryTitle', 'numVotes']

    for i in range(len(heading_list)):
        sheet.cell(row=1, column=i+1,
                   value=heading_list[i]).font = Font(bold=True)

    dimensions = {}
    for row in sheet.rows:
        flag = False
        for cell in row:
            if cell.value:
                dimensions[cell.column_letter] = max(
                    (dimensions.get(cell.column_letter, 0), len(str(cell.value))))
                if flag:
                    flag = False
                    cell.font = Font(bold=True)
                if cell.value == 'TOTAL':
                    flag = True
                    cell.font = Font(bold=True)
    for col, value in dimensions.items():
        sheet.column_dimensions[col].width = value + 2

    with NamedTemporaryFile() as tmp:
        wb.save(tmp.name)
        handle = BytesIO(tmp.read())
    return handle


def create_file(data: List, val_list: List):
    df = pd.DataFrame(data)
    filename = "genre-movies-with-subtotals.xlsx"
    handle = BytesIO()
    df.to_excel(handle, index=False)
    handle = modify_file(handle)
    handle.seek(0)
    response = StreamingResponse(
        content=handle, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response.headers["Content-Disposition"] = "attachment; filename=" + filename
    return response


def get_chunk_size(data_len):
    max_workers = 50
    # chunk_size = data_len // max_workers
    chunk_size = data_len
    default_chunk_size = 500
    if chunk_size <= default_chunk_size:
        chunk_size = default_chunk_size
        if data_len // chunk_size <= 1:
            chunk_size = 100
    return chunk_size


def divide_chunks(data, n):
    for i in range(0, len(data), n):
        yield data[i:i + n]


def parallelize(fn, *args, max_workers=50, pool_type='Thread'):
    iterable = args[2]
    max_ = len(iterable)
    result_list = []
    pool = ThreadPoolExecutor if pool_type == 'Thread' else ProcessPoolExecutor

    if pool_type == 'Loop':
        for obj in tqdm(iterable, desc=f"Iterating using {pool_type}: {fn.__name__}.."):
            result = fn(**obj)
            result_list.append(result)
        return result_list

    with pool(max_workers=max_workers) as executor:
        with tqdm(total=max_, desc=f"parallelizing using {pool_type}: {fn.__name__}..") as p_bar:
            tasks = (
                executor.submit(fn, args[:2], **obj)
                for obj in iterable
            )

            for task in as_completed(tasks):
                result_list.append(task.result())
                p_bar.update(1)

    return result_list
