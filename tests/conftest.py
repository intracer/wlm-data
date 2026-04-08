import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .master("local")
               .appName("wlm-tests")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()
