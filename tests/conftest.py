import os
import pytest
from pyspark.sql import SparkSession

# Resolve Delta Lake JARs from the ivy2 cache (populated when delta-spark pip package is used
# with configure_spark_with_delta_pip or spark.jars.packages on a networked run).
# We use spark.jars with explicit paths to avoid Maven resolution at test time.
_IVY_CACHE = os.path.expanduser("~/.ivy2/cache")
_DELTA_JAR = os.path.join(_IVY_CACHE, "io.delta", "delta-spark_2.12", "jars", "delta-spark_2.12-3.2.0.jar")
_STORAGE_JAR = os.path.join(_IVY_CACHE, "io.delta", "delta-storage", "jars", "delta-storage-3.2.0.jar")
_DELTA_JARS = ",".join([_DELTA_JAR, _STORAGE_JAR])


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .master("local")
               .appName("wlm-tests")
               .config("spark.ui.enabled", "false")
               .config("spark.jars", _DELTA_JARS)
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .getOrCreate())
    yield session
    session.stop()
