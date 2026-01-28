import ccdexplorer_schema_parser as _csp

try:
    Schema = _csp.Schema
except AttributeError:
    # Some builds don't re-export Schema in __init__.py.
    from ccdexplorer_schema_parser.Schema import Schema as _Schema

    _csp.Schema = _Schema  # type: ignore[attr-defined]
    Schema = _Schema

__all__ = ["Schema"]
