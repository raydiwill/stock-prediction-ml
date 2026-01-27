from feast import Entity, ValueType


stock = Entity(
    name="stock_symbol",
    description="Stock's ticker symbol (E.g AAPL, TSLA, etc)",
    value_type=ValueType.STRING,
)
