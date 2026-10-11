__version__ = "0.1.1"


def __getattr__(name: str) -> object:
    # Submodules are imported on first access so that lerna can locate
    # ccflow_s3.config without importing ccflow
    from importlib import import_module

    namespace = globals()
    for submodule in ("base",):
        module = import_module(f"{__name__}.{submodule}")
        namespace.update({n: getattr(module, n) for n in module.__all__})
    if name in namespace:
        return namespace[name]
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
