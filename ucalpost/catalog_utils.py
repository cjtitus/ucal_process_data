from databroker.queries import TimeRange, FullText

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
 
