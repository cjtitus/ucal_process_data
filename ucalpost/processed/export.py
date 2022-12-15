from xastools.io.exportXAS import exportXASToYaml, exportXASToSSRL
import datetime
from ..tools.utils import iterfy


def xas_to_directory(xas):
    date = datetime.datetime.fromtimestamp(xas.scaninfo['time'])
    cycle = xas.scaninfo['cycle']
    proposal = xas.scaninfo['proposal']
    basepath = f"/nsls2/data/sst/legacy/ucal/proposals/{date.year}-{cycle}/pass-{proposal}/ucal/{date.year}{date.month:02d}{date.day:02d}_processed"
    return basepath


def export_catalog_to_yaml(catalog, folder=None, namefmt=None, subcatalogs=True, **export_kwargs):
    """
    norm : If present, a column to normalize by
    offsetMono : If True, shift mono
    """

    xaslist = catalog.get_xas(subcatalogs=subcatalogs)

    for xas in iterfy(xaslist):
        if folder is None:
            folder = xas_to_directory(xas)
        if namefmt is None:
            if subcatalogs:
                namefmt = "{sample}_{element}_coadded.yaml"
            else:
                namefmt = "{sample}_{element}_{scan}.yaml"
        exportXASToYaml(xas, folder, namefmt=namefmt, **export_kwargs)


def export_catalog_to_ssrl(catalog, folder=None, namefmt=None, subcatalogs=True, **export_kwargs):
    xaslist = catalog.get_xas(subcatalogs=subcatalogs)

    for xas in iterfy(xaslist):
        if folder is None:
            folder = xas_to_directory(xas)
        if namefmt is None:
            if subcatalogs:
                namefmt = "{sample}_{element}_coadded.dat"
            else:
                namefmt = "{sample}_{element}_{scan}.dat"
        exportXASToSSRL(xas, folder, namefmt=namefmt, **export_kwargs)
