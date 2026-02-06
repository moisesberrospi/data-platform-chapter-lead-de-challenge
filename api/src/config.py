from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    POSTGRES_DB: str = "challenge"
    POSTGRES_USER: str = "challenge"
    POSTGRES_PASSWORD: str = "challenge"
    POSTGRES_HOST: str = "db"
    POSTGRES_PORT: int = 5432

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )


settings = Settings()
