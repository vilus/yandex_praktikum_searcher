from enum import Enum
from typing import Optional

from pydantic import BaseModel, ValidationError, validator
from flask import Flask, jsonify, abort, request
from elasticsearch import Elasticsearch, exceptions as es_exceptions

ES_URL = '127.0.0.1:9200'
ES_MOVIES_INDEX = 'movies'

API_PREFIX = '/api'
app = Flask(__name__)
app.config['DEBUG'] = True
app.config['JSON_SORT_KEYS'] = False

QS_PARAMS_VALIDATION_ERROR = 422


class MoviesSortEnum(str, Enum):
    id = 'id'
    title = 'title'
    imdb_rating = 'imdb_rating'


class MoviesSortOrderEnum(str, Enum):
    asc = 'asc'
    desc = 'desc'


class MoviesQSParamsModel(BaseModel):
    limit: int
    page: int
    sort: MoviesSortEnum
    sort_order: MoviesSortOrderEnum
    search: str

    @validator('limit', 'page')
    def validate_for_pagination(cls, v):
        if v <= 0:
            raise ValidationError('must be more than 0')
        return v


class ShortMovieModel(BaseModel):
    id: str
    title: str
    imdb_rating: Optional[float] = None

    @classmethod
    def from_es_hit(cls, es_hit):
        """
        TODO: !
        :param es_hit:
        :return:
        """
        _source = es_hit['_source']
        return cls(
            id=_source['id'],
            title=_source['title'],
            imdb_rating=_source['imdb_rating'],
        )


def _get_movies_qs_params(params: dict):
    return {
        'limit': params.get('limit', 50),
        'page': params.get('page', 1),
        'sort': params.get('sort', MoviesSortEnum.id),
        'sort_order': params.get('sort_order', MoviesSortOrderEnum.asc),
        'search': params.get('search', ''),  # TODO: with default or required param?
    }


def _get_es_search_body_param(query_value: str):
    if not query_value:
        return {}  # TODO: check docs about wildcard

    return {
        'query': {
            'multi_match': {
                'query': query_value,
                'fuzziness': 'auto',
                'fields': [
                    'title^5',
                    'description^4',
                    'genre^3',
                    'actors_names^3',
                    'writers_names^2',
                    'director'
                ]
            }
        }
    }


def _get_es_filter_path_param():
    return [
        'hits.hits._source.{}'.format(field) for field in ShortMovieModel.__fields__.keys()
    ]  # or just `_source=['id', 'title', 'imdb_rating']` in search?


def _get_es_sort_param(field, sort_order):
    sort_field = {
        'title': 'title.raw'
    }.get(field, field)
    return f'{sort_field}:{sort_order}'


@app.route(API_PREFIX + '/movies/')
@app.route(API_PREFIX + '/movies')
def movies():
    try:
        qs_params = MoviesQSParamsModel(**_get_movies_qs_params(request.args))
    except ValidationError as exc:
        return {'detail': exc.errors()}, QS_PARAMS_VALIDATION_ERROR

    with Elasticsearch(hosts=ES_URL) as es:  # TODO: extract
        es_movies = es.search(  # TODO: check docs about exceptions
            index=ES_MOVIES_INDEX,
            body=_get_es_search_body_param(qs_params.search),
            filter_path=_get_es_filter_path_param(),
            from_=(qs_params.page - 1)*qs_params.limit,
            size=qs_params.limit,
            sort=_get_es_sort_param(qs_params.sort, qs_params.sort_order),
        )

    return jsonify([
        ShortMovieModel.from_es_hit(hit).dict() for hit in es_movies['hits']['hits']
    ])


@app.route(API_PREFIX + '/movies/<movie_id>')
def movie(movie_id: str):
    with Elasticsearch(hosts=ES_URL) as es:  # TODO: extract
        try:
            es_movie = es.get(index=ES_MOVIES_INDEX, id=movie_id)['_source']
        except es_exceptions.NotFoundError:
            abort(404)

    # TODO: add schema validation

    return jsonify(  # TODO: rewrite with {}
        id=es_movie['id'],
        title=es_movie['title'],
        description=es_movie['description'],
        imdb_rating=es_movie['imdb_rating'],
        writers=es_movie['writers'],
        actors=[{'id': int(m['id']), 'name': m['name']} for m in es_movie['actors']],  # do catch ValueError?
        genre=es_movie['genre'],
        director=es_movie['director']
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
