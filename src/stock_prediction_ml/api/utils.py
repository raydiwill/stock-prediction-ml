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
