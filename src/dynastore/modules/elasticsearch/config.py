import os
from pydantic_settings import BaseSettings

class ElasticsearchConfig(BaseSettings):
    host: str = "localhost"
    port: int = 9200
    scheme: str = "http"
    username: str | None = None
    password: str | None = None
    verify_certs: bool = True
    index_prefix: str = "dynastore"

    @property
    def url(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}"

    class Config:
        env_prefix = "ES_"

config = ElasticsearchConfig()
