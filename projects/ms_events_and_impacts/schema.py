from ccdexplorer.schema_parser import Schema as _UpstreamSchema


class Schema(_UpstreamSchema):
    """Project-local Schema shim so poly check can locate it."""

    pass
