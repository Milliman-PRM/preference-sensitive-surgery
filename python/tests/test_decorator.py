"""
### CODE OWNERS: Umang Gupta, Pierre Cornell

### OBJECTIVE:
  Test the decoration of preference-sensitive surgeries

### DEVELOPER NOTES:
  <none>
"""
# Disable some design quirks required by pytest
# pylint: disable=redefined-outer-name

from pathlib import Path

import pytest
import pyspark.sql.functions as spark_funcs

import pref_sens_surg.decorator
from prm.spark.io_txt import build_structtype_from_csv

try:
    _PATH_THIS_FILE = Path(__file__).parent
except NameError:
    _PARTS = list(Path(pref_sens_surg.decorator.__file__).parent.parts)
    _PARTS[-1] = "tests"
    _PATH_THIS_FILE = Path(*_PARTS)  # pylint: disable=redefined-variable-type

PATH_MOCK_SCHEMAS = _PATH_THIS_FILE / "mock_schemas"
PATH_MOCK_DATA = _PATH_THIS_FILE / "mock_data"

# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


@pytest.fixture
def mock_schemas():
    """Schemas for testing data to play with"""
    return {
        path_.stem: build_structtype_from_csv(path_)
        for path_ in PATH_MOCK_SCHEMAS.glob("*.csv")
    }


@pytest.fixture
def mock_dataframes(spark_app, mock_schemas):
    """Testing data to play with"""
    return {
        path_.stem: spark_app.session.read.csv(
            str(path_),
            schema=mock_schemas[path_.stem],
            sep=",",
            header=True,
            mode="FAILFAST",
        )
        for path_ in PATH_MOCK_DATA.glob("*.csv")
    }


def test_pss(mock_dataframes):
    """Test the avoidable claim identification"""
    test_instance = pref_sens_surg.decorator.PSSDecorator()
    actual_result = test_instance.calc_decorator(
        dfs_input=mock_dataframes, dfs_refs=mock_dataframes
    )
    actual_result.cache()
    expected_result = (
        mock_dataframes["ccs_results"]
        .select(
            "sequencenumber",
            *[
                spark_funcs.col(col).alias("expected_" + col)
                for col in mock_dataframes["ccs_results"].columns
                if col.startswith("ccs")
            ]
        )
        .cache()
    )
    assert actual_result.count() == expected_result.count()

    compare = expected_result.join(
        actual_result, test_instance.key_fields, "left_outer"
    ).cache()
    compare_fields = {
        exp_col: exp_col[len("expected_") :]
        for exp_col in expected_result.columns
        if exp_col.startswith("expected_")
    }
    failures = list()
    failure_rows = set()
    for expected_column, actual_column in compare_fields.items():
        misses = compare.filter(compare[expected_column] != compare[actual_column])
        n_misses = misses.count()
        if n_misses != 0:
            failures.append(actual_column)
            failure_rows = failure_rows | set(
                [row["sequencenumber"] for row in misses.collect()]
            )
    assert (
        not failures
    ), "Unexpected values in '{}' column(s) for sequencenumbers '{}'".format(
        failures, failure_rows
    )
