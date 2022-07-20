from ..tools.catalog import WrappedCatalogBase

class WrappedAnalysis(WrappedCatalogBase):
    KEY_MAP = {"samples": "sample", "groups": "group"}
