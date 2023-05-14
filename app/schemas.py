from app.utils.common import CustomModel


class MovieSchema(CustomModel):
    tconst: str
    titleType: str
    primaryTitle: str
    runtimeMinutes: int
    genres: str


class RatingSchema(CustomModel):
    tconst: str
    averageRating: float
    numVotes: int
