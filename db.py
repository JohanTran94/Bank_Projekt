import psycopg2

# Singleton to reuse the same connection across instances
class Db:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Db, cls).__new__(cls)
            cls._instance.conn = cls._create_conn()
        return cls._instance

    @staticmethod
    def _create_conn():
        return psycopg2.connect(
            dbname='bank_projekt',
            user='postgres',
            password='Jason.Chen241194',
            host='localhost',
            port='5544'
        )

    def get_conn(self):
        return self.conn
