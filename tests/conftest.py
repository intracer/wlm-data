import os
import pytest
from pyspark.sql import SparkSession

# Delta Lake JARs are stored in the ivy2 cache after `pip install delta-spark`.
# We load them explicitly via spark.jars to avoid triggering Maven resolution at test time.
_IVY_CACHE = os.path.expanduser("~/.ivy2/cache")
_DELTA_JAR = os.path.join(_IVY_CACHE, "io.delta", "delta-spark_2.12", "jars", "delta-spark_2.12-3.2.0.jar")
_STORAGE_JAR = os.path.join(_IVY_CACHE, "io.delta", "delta-storage", "jars", "delta-storage-3.2.0.jar")


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .master("local")
               .appName("wlm-tests")
               .config("spark.ui.enabled", "false")
               .config("spark.jars", f"{_DELTA_JAR},{_STORAGE_JAR}")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .getOrCreate())
    yield session
    session.stop()
