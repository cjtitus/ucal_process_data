from databroker.queries import TimeRange, FullText, PartialUID, In, Key
from .run import summarize_run
import collections

def iterfy(x):
    if not isinstance(x, str) and isinstance(x, collections.abc.Iterable):
        return x
    else:
        return [x]

def filter_catalog(catalog, stop=True, sample=None, group=None, scantype=None, edge=None):
    if stop:
        catalog = filter_by_stop(catalog)
    if sample is not None:
        catalog = filter_by_sample(catalog, sample)
    if group is not None:
        catalog = filter_by_group(catalog, group)
    if scantype is not None:
        catalog = filter_by_scantype(catalog, scantype)
    if edge is not None:
        catalog = filter_by_edge(catalog, edge)
    return catalog

def filter_by_stop(catalog):
    ok_uuids = []
    for n in range(len(catalog)):
        try:
            uuid, item = catalog.items_indexer[n]
            if item.metadata['stop']['exit_status'] == "success":
                ok_uuids.append(uuid)
        except:
            pass
    return catalog.search(PartialUID(*ok_uuids))


def filter_by_sample(catalog, samplename):
    return catalog.search(In("sample_args.sample_name.value", iterfy(samplename)))


def filter_by_group(catalog, groupname):
    return catalog.search(In("group", iterfy(groupname)))


def filter_by_scantype(catalog, scantype):
    return catalog.search(In("scantype", iterfy(scantype)))


def filter_by_edge(catalog, edge):
    return catalog.search(In("edge", iterfy(edge)))


def list_groups(catalog):
    return list_start_key_vals(catalog, "group")


def list_samples(catalog):
    return list_start_key_vals(catalog, "sample_args", "sample_name", "value")


def list_edges(catalog):
    return list_start_key_vals(catalog, "edge")


def list_start_key_vals(catalog, *keys):
    vals = set()
    for h in catalog.values():
        s = h.metadata['start']
        for k in keys:
            s = s.get(k, None)
            if s is None:
                break
        if s is not None:
            vals.add(s)
    return vals


def summarize_catalog(catalog):
    groupname = ""
    for n in range(len(catalog)):
        uid, run = catalog.items_indexer[n]
        group = run.metadata['start'].get('group', '')
        if group != groupname:
            groupname = group
            if groupname != "":
                print(f"Group: {groupname}")
        print("-------------")
        print(f"uid: {uid[:9]}...")
        summarize_run(run)


def get_subcatalogs(catalog, groups=True, samples=True, edges=True):
    return _get_subcatalogs(catalog, groups=groups, samples=samples,
                            edges=edges)


def _get_subcatalogs(catalog, **kwargs):
    subcatalogs = []
    if kwargs.pop('groups', False):
        for g in list_groups(catalog):
            group_catalog = filter_by_group(catalog, g)
            subcatalogs += _get_subcatalogs(group_catalog, **kwargs)
        return subcatalogs
    if kwargs.pop('samples', False):
        for s in list_samples(catalog):
            sample_catalog = filter_by_sample(catalog, s)
            subcatalogs += _get_subcatalogs(sample_catalog, **kwargs)
        return subcatalogs
    if kwargs.pop('edges', False):
        for e in list_edges(catalog):
            edge_catalog = filter_by_edge(catalog, e)
            subcatalogs += _get_subcatalogs(edge_catalog, **kwargs)
        return subcatalogs
    return [catalog]
