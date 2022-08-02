"""
A module to deal with fully-processed xastools-like spectra
"""
from ..tools.catalog import WrappedCatalogBase
from tiled.queries import Key
from functools import reduce


class WrappedAnalysis(WrappedCatalogBase):
    KEY_MAP = {"samples": "scaninfo.sample", "groups": "scaninfo.group",
               "edges": "scaninfo.element", "loadid": "scaninfo.loadid",
               "scans": "scaninfo.scan"}

    def get_subcatalogs(self, groups=True, samples=True, edges=True):
        return self._get_subcatalogs(groups=groups, samples=samples, edges=edges)

    def list_meta_key_vals(self, key):
        keys = key.split('.')
        vals = set()
        for h in self._catalog.values():
            s = h.metadata
            for k in keys:
                s = s.get(k, None)
                if s is None:
                    break
            if s is not None:
                vals.add(s)
        return vals

    def filter(self, samples=None, groups=None, edges=None):
        return super().filter(samples=samples, groups=groups, edges=edges)

    def summarize(self):
        for h in self._catalog.values():
            scaninfo = h.metadata['scaninfo']
            print(f"Date: {scaninfo['date']}")
            print(f"Scan: {scaninfo['scan']}")
            print(f"Group: {scaninfo['group']}")
            print(f"Sample: {scaninfo['sample']}")

    def get_xas(self, subcatalogs=True):
        if subcatalogs:
            catalogs = self.get_subcatalogs()
            xas = [c.get_xas(False) for c in catalogs]
            return xas
        else:
            allxas = [v.to_xas() for v in self._catalog.values()]
            if len(allxas) > 1:
                xas = reduce(lambda x, y: x + y, allxas)
            else:
                xas = allxas[0]
            return xas
