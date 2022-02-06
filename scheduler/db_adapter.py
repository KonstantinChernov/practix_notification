import backoff
from psycopg2.extras import RealDictCursor
import psycopg2
import datetime
from abc import ABC


class AbstractDBAdapter(ABC):
    def __init__(self, dsn: dict,
                 chunk_size: int):
        self.dsn = dsn
        self.chunk_size = chunk_size

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError)
    def get_db_connection(self):
        dsn_string = ' '.join([f'{key}={value}' for key, value in self.dsn.items()])
        pg_conn = psycopg2.connect(dsn=dsn_string, cursor_factory=RealDictCursor)
        return pg_conn


class AuthDBAdapter(AbstractDBAdapter):

    def get_users_info(self, offset: int) -> str:
        SQL = f'''
        SELECT u.email, u.login
        FROM User u
        OFFSET {offset}
        LIMIT {self.chunk_size}
        '''
        return SQL


class FilmWorkDBAdapter(AbstractDBAdapter):

    @staticmethod
    def get_sql_for_m2m_person(table_name: str) -> str:
        with_name = table_name.split('_')[2]
        as_name = ''.join([word[0] for word in table_name.split('_')])
        SQL = f'''
        {with_name} as (
        SELECT m.id, 
               string_agg(CAST(a.id AS TEXT), ',') ids,
               string_agg(a.first_name || ' ' || a.last_name, ',') persons_names
        FROM movies_filmwork m
            LEFT JOIN {table_name} {as_name} on m.id = {as_name}.filmwork_id 
            LEFT JOIN movies_person a on {as_name}.person_id = a.id
        GROUP BY m.id
        )
        '''
        return SQL

    def get_base_filmwork_sql(self) -> str:
        with_actors_sql = self.get_sql_for_m2m_person('movies_filmwork_actors')
        with_writers_sql = self.get_sql_for_m2m_person('movies_filmwork_writers')
        with_directors_sql = self.get_sql_for_m2m_person('movies_filmwork_directors')
        SQL = f'''
        WITH genres as (
            SELECT m.id, 
                   string_agg(CAST(g.id AS TEXT), ',') ids,
                   string_agg(g.title, ',') titles
            FROM movies_filmwork m
                LEFT JOIN movies_filmwork_genres mfg on m.id = mfg.filmwork_id 
                LEFT JOIN movies_genre g on mfg.genre_id = g.id
            GROUP BY m.id
        ),
        {with_actors_sql},
        {with_directors_sql},
        {with_writers_sql}

        SELECT 
               fm.created_at,
               fm.updated_at,
               fm.id,
               fm.rating,
               fm.type,
               genres.ids genres_ids,
               genres.titles genres_titles,
               fm.title,
               fm.description,
               actors.ids actors_ids,
               actors.persons_names actors_names,
               writers.ids writers_ids,
               writers.persons_names writers_names,
               directors.ids directors_ids,
               directors.persons_names directors_names
        FROM movies_filmwork fm
            LEFT JOIN genres ON genres.id = fm.id
            LEFT JOIN actors ON actors.id = fm.id
            LEFT JOIN writers ON writers.id = fm.id
            LEFT JOIN directors ON directors.id = fm.id
        '''
        return SQL

    def get_top_filmworks(self) -> str:
        one_week_ago_datetime = str(datetime.datetime.now() - datetime.timedelta(days=7))
        SQL = f'''
        {self.get_base_filmwork_sql()}
        WHERE fm.created_at > {one_week_ago_datetime}
        ORDER BY fm.rating DESC 
        LIMIT 10
        '''
        return SQL

    def get_new_films_by_genre(self, genre: str) -> str:
        one_week_ago_datetime = str(datetime.datetime.now() - datetime.timedelta(days=7))
        SQL = f'''
        {self.get_base_filmwork_sql()}
        WHERE fm.dt_create > {one_week_ago_datetime},
        {genre} IN genres_titles
        ORDER BY fm.rating DESC 
        LIMIT 10
        '''
        return SQL

    def get_updated_filmworks_by_ids_sql(self, filmworks_ids: str) -> str:
        SQL = f'''
        {self.get_base_filmwork_sql()}
        WHERE fm.id in ({filmworks_ids})
        '''
        return SQL
