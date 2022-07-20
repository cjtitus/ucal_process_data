def get_xas_from_run(run, **kwargs):
    if hasattr(run, 'to_xas'):
        s = run.to_xas()
    else:
        data, header = get_data_and_header(run, **kwargs)
        s = XAS.from_data_header(data, header)
    return s


def get_xas_from_catalog(catalog, combine=True, **kwargs):
    xas_list = []
    for uid, run in catalog.items():
        xas_list.append(get_xas_from_run(run, **kwargs))
    if combine:
        return reduce(lambda x, y: x + y, xas_list)
    else:
        return xas_list


def export_run_to_yaml(run, folder=None, data_kwargs={}, export_kwargs={}, namefmt="{sample}_{element}_{scan}"):
    if folder is None:
        folder = get_proposal_directory(run)
    if not path.exists(folder):
        print(f"Making {folder}")
        os.makedirs(folder)
    xas = get_xas_from_run(run, **data_kwargs)
    exportXASToYaml(xas, folder, namefmt=namefmt, **export_kwargs)


def export_catalog_to_yaml(catalog, **kwargs):
    for _, run in catalog.items():
        export_run_to_yaml(run, **kwargs)
