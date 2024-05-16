import dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ("conf",)


def find_dotenv(name) -> str:
    """Finds dotenv absolute paths.

    Returns
    -------
    dotenv : str
        Absolute dotenv path.
    """
    return dotenv.find_dotenv(name)


class EnvFile(BaseSettings):
    """Specifies the environment file to use."""

    env_file: str = ".env"


class SparkSettings(BaseSettings):
    """Application general information."""

    tempdir: str = "s3://kuonaspark"

    model_config = SettingsConfigDict(env_prefix="SPARK_")


def get_dotenv() -> str:
    """Returns dotenv filename."""
    return find_dotenv(EnvFile().env_file)


class Settings:
    """Returns shared settings.

    Use get_* methods to retrieve settings.
    Environment file is set dynamically depending on the "env_file"
    environment variable.
    """

    def __init__(self) -> None:
        self._env_file = get_dotenv()

    def get_spark_settings(self) -> SparkSettings:
        """Returns token settings."""
        return SparkSettings(_env_file=self._env_file)


conf: Settings = Settings()
