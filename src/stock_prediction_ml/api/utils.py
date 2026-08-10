from datetime import date, timedelta


def next_trading_day(d: date) -> date:
    """Return the next trading day after ``d``, skipping weekends.

    Note:
        Does not account for market holidays.
    """
    next_day = d + timedelta(days=1)

    if next_day.weekday() == 5:
        next_day += timedelta(days=2)
    elif next_day.weekday() == 6:
        next_day += timedelta(days=1)

    return next_day


def check_dependencies(model: bool, feast: bool) -> dict:
    dependencies = {
        "MODEL": model is not None,
        "FEAST": feast is not None
    }

    all_loaded = all([model, feast])
    missing = [name for name, loaded in dependencies.items() if loaded is False]

    return {
        "all_loaded": all_loaded,
        "missing_dependencies": missing,
        "dependencies": dependencies,
    }
