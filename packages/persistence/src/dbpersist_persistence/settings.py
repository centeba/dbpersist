from dbpersist_common.settings import CommonSettings


class Settings(CommonSettings):
    persist_database_url: str = "postgresql+asyncpg://dbpersist:dbpersist@localhost:5442/persist"
    db_pool_size: int = 10
    db_max_overflow: int = 10
