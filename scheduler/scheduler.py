import asyncio

import backoff
import psycopg2
from apscheduler.schedulers.background import BackgroundScheduler
from db_adapter import AuthDBAdapter, FilmWorkDBAdapter
from decouple import config
from notify_grpc.send_event import EventSender
from mongo_adapter import MongoAdapter
from pymongo import MongoClient
from models import Favorite
from collections import Counter


class MailEventGenerator:
    def __init__(self,
                 auth_db_adapter: AuthDBAdapter,
                 filmwork_db_adapter: FilmWorkDBAdapter,
                 event_sender: EventSender,
                 mongo_adapter: MongoAdapter
                 ):
        self.auth_db_adapter = auth_db_adapter
        self.filmwork_db_adapter = filmwork_db_adapter
        self.event_sender = event_sender
        self.mongo_adapter = mongo_adapter

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError)
    def generate_common_week_mails(self):
        """
        Генерирует еженедельные ивенты с новым фильмами с самым большим рейтингом для каждого пользователя.
        """
        auth_pg_conn = self.auth_db_adapter.get_db_connection()
        try:
            filmwork_pg_conn = self.filmwork_db_adapter.get_db_connection()
            try:
                postgres_cur = filmwork_pg_conn.cursor()
                postgres_cur.execute(self.filmwork_db_adapter.get_top_filmworks())
                top_films = postgres_cur.fetchall
            finally:
                filmwork_pg_conn.close()
            if top_films:
                offset = 0
                postgres_cur = auth_pg_conn.cursor()
                postgres_cur.execute(self.auth_db_adapter.get_users_info(offset=0))
                user_info = postgres_cur.fetchall()
                transformed_films = self.__get_transformed_films(top_films)
                event_code = config('COMMON_WEEK_QUEUE')
                while user_info:
                    recievers_emails = [info['email'] for info in user_info]
                    self.event_sender.send_event(context={'receivers_emails': recievers_emails,
                                                          'films': transformed_films},
                                                 event_code=event_code,
                                                 request_id_header="")
                    offset += self.auth_db_adapter.chunk_size
                    postgres_cur.execute(self.auth_db_adapter.get_users_info(offset=offset))
                    user_info = postgres_cur.fetchall()
        finally:
            auth_pg_conn.close()

    @backoff.on_exception(backoff.expo, ConnectionError, psycopg2.OperationalError)
    def generate_personal_week_mails(self):
        """
        Генерирует еженедельные ивенты с новыми фильмами в любимом жанре каждого пользователя.
        """
        auth_pg_conn = self.auth_db_adapter.get_db_connection()
        try:
            postgres_cur = auth_pg_conn.cursor()
            postgres_cur.execute(self.auth_db_adapter.get_users_info(offset=0))
            user_info = postgres_cur.fetchall()
            event_code = config('WEEK_PERSONAL_USERS_QUEUE')
            offset = 0
            while user_info:
                for info in user_info:
                    favourites = self.mongo_adapter.get_objects_from_db(model=Favorite,
                                                                        query={"user_login": info['login']})
                    if favourites:
                        filmwork_pg_conn = self.filmwork_db_adapter.get_db_connection
                        favourite_films_ids = ",".join([fav['film_id'] for fav in favourites])
                        try:
                            postgres_cur = filmwork_pg_conn.cursor()
                            postgres_cur.execute(self.filmwork_db_adapter.get_updated_filmworks_by_ids_sql(favourite_films_ids))
                            favourite_films = postgres_cur.fetchall
                            films_genres = [{'film_id': film['film_id'],
                                             'genre': film['genres_titles'][0]} for film in favourite_films]
                            genres_counter = Counter(film['genre'] for film in films_genres)
                            favourite_genre = max(genres_counter, key=genres_counter.get)
                            personal_films = postgres_cur.execute(self.filmwork_db_adapter.get_new_films_by_genre(genre=favourite_genre))
                        finally:
                            filmwork_pg_conn.close()
                        self.event_sender.send_event(context={'email': info['email'],
                                                              'films': self.__get_transformed_films(personal_films)},
                                                     event_code=event_code,
                                                     request_id_header="")
                        offset += self.auth_db_adapter.chunk_size
                        postgres_cur.execute(self.auth_db_adapter.get_users_info(offset=offset))
                        user_info = postgres_cur.fetchall()
        finally:
            auth_pg_conn.close()

    @staticmethod
    def __get_transformed_films(top_films: list):
        transformed_films = [
            {'title': film['title'],
             'rating': film['rating'],
             'description': film['description'],
             'genres': film['genres_titles'],
             'directors': film['directors_names']
             } for film in top_films
        ]
        return transformed_films


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    auth_input_dsn = {'dbname': config('AUTH_POSTGRES_DB'),
                      'user': config('AUTH_POSTGRES_USER'),
                      'password': config('AUTH_POSTGRES_PASSWORD'),
                      'host': config('AUTH_POSTGRES_HOST') or 'localhost',
                      'port': config('AUTH_POSTGRES_PORT') or '5432'
                      }
    filmwork_input_dsn = {'dbname': config('FILMWORK_POSTGRES_DB'),
                          'user': config('FILMWORK_POSTGRES_USER'),
                          'password': config('FILMWORK_POSTGRES_PASSWORD'),
                          'host': config('FILMWORK_POSTGRES_HOST') or 'localhost',
                          'port': config('FILMWORK_POSTGRES_PORT') or '5432'
                          }
    input_auth_db_adapter = AuthDBAdapter(dsn=auth_input_dsn,
                                          chunk_size=config('CHUNK_SIZE'))
    input_filmwork_db_adapter = FilmWorkDBAdapter(dsn=auth_input_dsn,
                                                  chunk_size=config('CHUNK_SIZE'))

    mongo_client = MongoClient(config('MONGO_HOST'), config('MONGO_PORT'))
    input_mongo_adapter = MongoAdapter(mongo=mongo_client)
    input_event_sender = EventSender(loop=loop)
    event_generator = MailEventGenerator(
                                         auth_db_adapter=input_auth_db_adapter,
                                         filmwork_db_adapter=input_filmwork_db_adapter,
                                         event_sender=input_event_sender,
                                         mongo_adapter=input_mongo_adapter)
    sched = BackgroundScheduler(daemon=False)
    sched.add_job(event_generator.generate_common_week_mails, 'interval', weeks=1)
    sched.add_job(event_generator.generate_personal_week_mails, 'interval', weeks=1)
    sched.start()
