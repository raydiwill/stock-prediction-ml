from feast import Entity
from feast.types import String


stock = Entity(
    name="stock_symbol",
    description="Stock's ticker symbol (E.g AAPL, TSLA, etc)",
    value_type=String,
)
