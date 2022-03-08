from databroker.queries import TimeRange, FullText, PartialUID, RawMongo
from .run import summarize_run


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
    return catalog.search({"sample_args.sample_name.value": samplename})


def filter_by_group(catalog, groupname):
    return catalog.search({"group": groupname})


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
