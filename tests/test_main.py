from stock_prediction_ml.main import main


def test_main():
    result = main()
    assert result is None
