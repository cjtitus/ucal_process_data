"""
A module to deal with fully-processed xastools-like spectra
"""

from ..tools.catalog import WrappedCatalogBase
from ..tools.utils import get_with_fallbacks
from tiled.queries import Key
from functools import reduce
import datetime


def subcatalog_input_transformer(arg):
    if arg is True:
        return {}
    elif arg is False or arg is None:
        return {"subcatalogs": False}
    else:
        return arg


class WrappedAnalysis(WrappedCatalogBase):
    KEY_MAP = {
        "samples": "scaninfo.sample",
        "groups": "scaninfo.group_md.name",
        "edges": "scaninfo.element",
        "loadid": "scaninfo.loadid",
        "scans": "scaninfo.scan",
        "date": "scaninfo.date",
    }

    def get_subcatalogs(self, groups=True, samples=True, edges=True, subcatalogs=True):
        """
        subcatalogs: bool. If False, don't get subcatalogs, but wrap the current catalog in a list,
        so that it can be treated as the output from get_subcatalogs
        """
        if subcatalogs:
            return self._get_subcatalogs(groups=groups, samples=samples, edges=edges)
        else:
            return [self]

    def list_meta_key_vals(self, key):
        keys = key.split(".")
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

    def filter_by_time(self, since, until):
        return self.search(Key(self.KEY_MAP["date"]) > since).search(
            Key(self.KEY_MAP["date"]) < until
        )

    def get_beamtime(self, since, until=None):
        """
        since : iso formatted date string
        until : optional, iso formatted date string
        """
        if until is None:
            startdate = datetime.datetime.fromisoformat(since)
            defaultdelta = datetime.timedelta(days=1)
            untildatetime = startdate + defaultdelta
            until = untildatetime.isoformat()
        beamtime_start_vals = (
            self.search(Key("scaninfo.beamtime_start") > since)
            .search(Key("scaninfo.beamtime_start") < until)
            .list_meta_key_vals("scaninfo.beamtime_start")
        )
        return self.filter_by_key("scaninfo.beamtime_start", beamtime_start_vals)

    def summarize(self):
        for h in self._catalog.values():
            scaninfo = h.metadata["scaninfo"]
            print(f"Date: {scaninfo['date']}")
            print(f"Scan: {scaninfo['scan']}")
            print(f"Group: {scaninfo['group_md']['name']}")
            print(f"Sample: {scaninfo['sample']} Edge: {scaninfo['element']}")

    def describe(self):
        desc_dict = {}
        for h in self._catalog.values():
            scaninfo = h.metadata["scaninfo"]
            scan = scaninfo["scan"]
            group = scaninfo["group_md"]["name"]
            sample = scaninfo["sample"]
            edge = scaninfo["element"]
            if group not in desc_dict:
                desc_dict[group] = {}
            group_dict = desc_dict[group]
            if sample not in group_dict:
                group_dict[sample] = {}
            sample_dict = group_dict[sample]
            if edge not in sample_dict:
                sample_dict[edge] = []
            edge_list = sample_dict[edge]
            edge_list.append(scan)
        for group, group_dict in desc_dict.items():
            print("-------------------------")
            print(f"Group: {group}")
            for sample, sample_dict in group_dict.items():
                print(f"Sample: {sample}")
                for edge, edge_list in sample_dict.items():
                    print(f"Edge: {edge}, Scans: {edge_list}")

    def get_xas(self, sample=None, subcatalogs=True, individual=False):
        """
        sample: simple pre-filter by a sample name
        subcatalogs: Bool or dictionary. If dictionary, passed as kwargs to
        get_subcatalogs so that default options can be modified
        """
        if sample is not None:
            catalog = self.filter_by_samples([sample])
            return catalog.get_xas(subcatalogs=subcatalogs, individual=individual)
        if individual:
            xas = [v.to_xas() for v in self._catalog.values()]
            return xas
        if subcatalogs is not False:
            catalogs = self.get_subcatalogs(**subcatalog_input_transformer(subcatalogs))
            xas = [c.get_xas(subcatalogs=False) for c in catalogs]
            return xas
        else:
            allxas = [v.to_xas() for v in self._catalog.values()]
            if len(allxas) > 1:
                xas = reduce(lambda x, y: x + y, allxas)
            else:
                xas = allxas[0]
            return xas
