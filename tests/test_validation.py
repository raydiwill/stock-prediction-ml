import great_expectations as gx
import pandas as pd
import pytest

from stock_prediction_ml.data_validation.validation import (
    add_dataframe_asset,
    add_pandas_datasource,
    build_expectation_suite,
    get_batch,
    get_batch_definition,
    get_context,
    validate_batch,
)


@pytest.fixture
def fake_stock_data():
    return {
        "date": ["2023-01-01", "2023-01-02"],
        "symbol": ["AAPL", "GOOGL"],
        "open": [150.0, 2800.0],
        "high": [155.0, 2850.0],
        "low": [149.0, 2790.0],
        "close": [154.0, 2840.0],
        "volume": [1000000, 1500000],
        "adj_close": [154.0, 2840.0],
    }


def test_get_context_return_gx_context():
    context = get_context()
    assert isinstance(
        context,
        gx.data_context.data_context.ephemeral_data_context.EphemeralDataContext,
    )


def test_add_pandas_datasource_returns_pandas_datasource():
    context = get_context()
    datasource = add_pandas_datasource(context)
    assert isinstance(
        datasource, gx.datasource.fluent.pandas_datasource.PandasDatasource
    )


def test_add_dataframe_asset_returns_dataframe_asset():
    context = get_context()
    datasource = add_pandas_datasource(context)
    data_asset = add_dataframe_asset(datasource)
    assert isinstance(data_asset, gx.datasource.fluent.pandas_datasource.DataFrameAsset)


def test_get_batch_returns_batch(tmp_path, fake_stock_data):
    context = get_context()
    datasource = add_pandas_datasource(context)
    data_asset = add_dataframe_asset(datasource)
    batch_definition = get_batch_definition(data_asset)

    # Use fake_stock_data directly (it's already the dict)
    df = pd.DataFrame(fake_stock_data)

    parquet_path = tmp_path / "sample.parquet"
    df.to_parquet(parquet_path)

    batch = get_batch(parquet_path, batch_definition)
    assert isinstance(batch, gx.datasource.fluent.interfaces.Batch)


def test_get_batch_definition_returns_batch_definition():
    context = get_context()
    datasource = add_pandas_datasource(context)
    data_asset = add_dataframe_asset(datasource)
    batch_definition = get_batch_definition(data_asset)
    assert isinstance(batch_definition, gx.core.batch_definition.BatchDefinition)


def test_build_expectation_suite_returns_expectation_suite():
    suite = build_expectation_suite()
    assert isinstance(suite, gx.core.expectation_suite.ExpectationSuite)


@pytest.mark.parametrize(
    "expectation",
    [
        "expect_column_pair_values_a_to_be_greater_than_b",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_be_of_type",
        "expect_column_values_to_not_be_null",
        "expect_compound_columns_to_be_unique",
        "expect_table_columns_to_match_set",
        "expect_table_row_count_to_be_between",
    ],
)
def test_suite_has_expected_expectations(expectation):
    suite = build_expectation_suite()
    expectation_types = [
        expectation.expectation_type for expectation in suite.expectations
    ]

    assert expectation in expectation_types


def test_validate_batch_returns_validation_result(tmp_path, fake_stock_data):
    context = get_context()
    datasource = add_pandas_datasource(context)
    data_asset = add_dataframe_asset(datasource)
    batch_definition = get_batch_definition(data_asset)

    # Use fake_stock_data directly (it's already the dict)
    df = pd.DataFrame(fake_stock_data)

    parquet_path = tmp_path / "sample.parquet"
    df.to_parquet(parquet_path)

    batch = get_batch(parquet_path, batch_definition)
    suite = build_expectation_suite()
    validation_result = validate_batch(batch, suite)

    assert isinstance(
        validation_result,
        gx.core.expectation_validation_result.ExpectationSuiteValidationResult,
    )
