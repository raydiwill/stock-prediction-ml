import argparse
import logging
from pathlib import Path

import great_expectations as gx
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_context():
    return gx.get_context()


def add_pandas_datasource(context, name: str = "pandas_datasource"):
    return context.sources.add_pandas(name=name)


def add_dataframe_asset(datasource, name: str = "data_asset"):
    return datasource.add_dataframe_asset(name=name)


def get_batch_definition(data_asset, batch_definition_name: str = "batch_definition"):
    return data_asset.add_batch_definition_whole_dataframe(name=batch_definition_name)


def get_batch(parquet_path: Path, batch_definition):
    df = pd.read_parquet(parquet_path)

    batch_parameters = {"dataframe": df}
    batch = batch_definition.get_batch(batch_data=batch_parameters)

    return batch


def build_expectation_suite(name: str = "stock_data_expectation_suite"):
    suite = gx.ExpectationSuite(name=name)

    # --- Schema: exact columns present (order-insensitive) ---
    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=[
                "date",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "adj_close",
            ]
        )
    )

    # --- Basic completeness ---
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="date"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="symbol")
    )
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="open"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="close"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="volume")
    )

    # --- Data types (Pandas dtypes shown; adjust if your backend differs) ---
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="date", type_="datetime64[ns]"
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="symbol", type_="object")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="open", type_="float64")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="high", type_="float64")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="low", type_="float64")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="close", type_="float64")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="adj_close", type_="float64"
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="volume", type_="int64")
    )

    # --- Logical price relationships ---
    suite.add_expectation(
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="high", column_B="low"
        )
    )

    # --- Uniqueness & row count ---
    suite.add_expectation(
        gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=["date", "symbol"])
    )
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))

    # --- Allowed symbols
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="symbol",
            value_set=["AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA"],
        )
    )

    return suite


def save_suite(context, suite):
    try:
        context.save_expectation_suite(suite)
        logger.info(f"Expectation suite '{suite.name}' saved successfully.")
    except Exception:
        context.suites.add(suite)
        logger.info(f"Expectation suite '{suite.name}' added successfully.")


def validate_batch(batch, suite):
    validation_result = batch.validate(suite)
    logger.info("Validation completed!")
    return validation_result


def main():
    parser = argparse.ArgumentParser(
        description="Validate stock data using Great Expectations."
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/raw/stock_data.parquet"),
        help="Path to the input Parquet file containing stock data.",
    )

    parser.add_argument(
        "--suite-name",
        type=str,
        default="stock_data_expectation_suite",
        help="Name of the expectation suite to use or create.",
    )

    args = parser.parse_args()

    context = get_context()
    datasource = add_pandas_datasource(context)
    data_asset = add_dataframe_asset(datasource)
    batch_definition = get_batch_definition(data_asset)
    batch = get_batch(args.input, batch_definition)
    suite = build_expectation_suite(args.suite_name)
    save_suite(context, suite)
    validation_result = validate_batch(batch, suite)

    logger.info(
        "Successful expectations: %d / %d",
        sum(1 for r in validation_result["results"] if r["success"]),
        len(validation_result["results"]),
    )


if __name__ == "__main__":
    main()
