import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("wagerflow-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()