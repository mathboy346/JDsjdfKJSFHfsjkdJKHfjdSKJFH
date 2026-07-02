from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost/filmydb"
    redis_url: str = "redis://localhost:6379/0"
    daily_cutoff_minutes: int = 200
    cache_ttl: int = 60  # seconds

    @property
    def async_database_url(self) -> str:
        """Heroku supplies postgres:// — asyncpg needs the postgresql+asyncpg:// dialect prefix."""
        url = self.database_url
        if url.startswith("postgres://"):
            return "postgresql+asyncpg://" + url[len("postgres://"):]
        if url.startswith("postgresql://") and "+asyncpg" not in url:
            return "postgresql+asyncpg://" + url[len("postgresql://"):]
        return url


@lru_cache
def get_settings() -> Settings:
    return Settings()
