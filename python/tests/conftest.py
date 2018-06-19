"""
### CODE OWNERS: Shea Parkes, Kyle Baird

### OBJECTIVE:
    Customize unit testing environment.

### DEVELOPER NOTES:
    Will be auto-found by py.test.
    Used below to define session-scoped fixtures

"""
# pylint: disable=redefined-outer-name
import pytest

import prm.spark.cluster
from prm.spark.app import SparkApp
from prm.spark.shared import TESTING_APP_PARAMS

#==============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
#==============================================================================


@pytest.fixture(scope='session')
def spark_cluster(request):
    """Create a local spark cluster to be shared among the tests in this directory."""
    superman = prm.spark.cluster.SparkCluster(**TESTING_APP_PARAMS)
    superman.start_cluster()

    def _cleanup_cluster():
        """Cleanup function for when we are finished."""
        superman.stop_cluster()
    request.addfinalizer(_cleanup_cluster)

    return superman


@pytest.fixture(scope='module')
def spark_app(request, spark_cluster):
    """Make a SparkApp instance for testing."""
    try:
        SparkApp.implant_cluster(spark_cluster)
    except AssertionError:
        # Testing cluster has likely already been implanted
        pass
    testing_app = SparkApp(
        request.module.__name__,
        bypass_instance_cache=True,
        spark_sql_shuffle_partitions=TESTING_APP_PARAMS["spark_sql_shuffle_partitions"],
        allow_local_io=TESTING_APP_PARAMS["allow_local_io"],
    )

    def _cleanup_app():
        """Cleanup function for when we are finished."""
        testing_app.stop_app()
    request.addfinalizer(_cleanup_app)

    return testing_app
