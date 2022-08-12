from .run import summarize_run, db
from ..tes.process_routines import process_catalog
from ..tes.process_classes import is_run_processed
from .export import export_run_to_analysis_catalog
from databroker.queries import PartialUID
from ..tools.catalog import WrappedCatalogBase
import datetime


class WrappedDatabroker(WrappedCatalogBase):
    KEY_MAP = {"samples": "sample_args.sample_name.value", "groups": "group",
               "edges": "edge", "noise": "last_noise", "scantype": "scantype",
               "proposal": "proposal", "uid": "uid"}

    def __init__(self, catalog, prefilter=False):
        super().__init__(catalog)
        if prefilter:
            self._catalog = self._filter_by_stop()

    def get_subcatalogs(self, noise=True, groups=True, samples=True, edges=True, **kwargs):
        return self._get_subcatalogs(noise=noise, groups=groups,
                                     samples=samples, edges=edges, **kwargs)

    def _filter_by_stop(self):
        ok_uuids = []
        for n in range(len(self._catalog)):
            try:
                uuid, item = self._catalog.items_indexer[n]
                if item.metadata['stop']['exit_status'] == "success":
                    ok_uuids.append(uuid)
            except:
                pass
        return self._catalog.search(PartialUID(*ok_uuids))

    def filter_by_stop(self):
        catalog = self._filter_by_stop()
        return self.__class__(catalog)

    def list_meta_key_vals(self, key):
        keys = key.split('.')
        vals = set()
        for h in self._catalog.values():
            s = h.metadata['start']
            for k in keys:
                s = s.get(k, None)
                if s is None:
                    break
            if s is not None:
                vals.add(s)
        return vals

    def filter(self, stop=False, samples=None, groups=None, scantype=None, edges=None):
        if stop:
            catalog = self.filter_by_stop()
            return catalog.filter(stop=False, samples=samples, groups=groups,
                                  scantype=scantype, edges=edges)
        else:
            return super().filter(samples=samples, groups=groups,
                                  scantype=scantype, edges=edges)

    def get_noise_catalogs(self):
        return self._get_subcatalogs(noise=True)

    def describe(self):
        nruns = len(self._catalog)
        samples = self.list_samples()
        groups = self.list_groups()
        times = self.list_meta_key_vals("time")
        start = datetime.datetime.fromtimestamp(min(times)).isoformat()
        stop = datetime.datetime.fromtimestamp(max(times)).isoformat()
        print(f"Catalog contains {nruns} runs")
        print(f"Time range: {start} to {stop}")
        print(f"Contains groups {groups}")
        print(f"Contains sampes {samples}")

    def summarize(self):
        groupname = ""
        for n in range(len(self._catalog)):
            uid, run = self._catalog.items_indexer[n]
            group = run.metadata['start'].get('group', '')
            if group != groupname:
                groupname = group
                if groupname != "":
                    print(f"Group: {groupname}")
            print("-------------")
            print(f"uid: {uid[:9]}...")
            summarize_run(run)

    def export_to_analysis(self, **kwargs):
        for _, run in self._catalog.items():
            print(f"Exporting run {run.metadata['start']['scan_id']}")
            export_run_to_analysis_catalog(run, **kwargs)

    def process_tes(self):
        process_catalog(self._catalog)

    def check_processed(self):
        for n in range(len(self._catalog)):
            uid, run = self._catalog.items_indexer[n]
            print(f"uid: {uid[:9]}...")
            print(f"TES processed: {is_run_processed(run)}")


wdb = WrappedDatabroker(db)
