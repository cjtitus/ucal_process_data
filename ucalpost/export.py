from .process_classes import scandata_from_run
from xastools.utils import roiMaster, roiDefaults
from xastools.io import exportXASToYaml
from xastools.xas import XAS
from .run import get_proposal_directory
import os
from os import path
import datetime
import numpy as np
from functools import reduce

def convert_names(name):
    name_conversions = {"en_energy_setpoint": "MONO", "en_energy": "ENERGY_ENC", "ucal_I400_i0up": "I0", "ucal_I400_ref": "REF", "ucal_I400_sc": "SC", "tes_tfy": "tfy"}
    return name_conversions.get(name, name)

def get_run_header(run):
    metadata = {}
    scaninfo = {}
    scaninfo['sample'] = run.start['sample_args']['sample_name']['value']
    scaninfo['loadid'] = run.start['sample_args']['sample_id']['value']
    scaninfo['scan'] = run.start['scan_id']
    scaninfo['date'] = datetime.datetime.fromtimestamp(run.start['time']).isoformat()
    scaninfo['command'] = run.start.get('command', run.start.get('plan_name', None))
    scaninfo['element'] = run.start.get('element', run.start.get('edge', None))
    scaninfo['motor'] = convert_names(run.start['motors'][0])
    motors = {}
    motors['exslit'] = run.baseline.data['Exit Slit of Mono Vertical Gap'].data[0]
    motors['manipx'] = run.baseline.data['Manipulator_x'].data[0]
    motors['manipy'] = run.baseline.data['Manipulator_y'].data[0]
    motors['manipz'] = run.baseline.data['Manipulator_z'].data[0]
    motors['manipr'] = run.baseline.data['Manipulator_r'].data[0]
    motors['samplex'] = run.baseline.data['Manipulator_sx'].data[0]
    motors['sampley'] = run.baseline.data['Manipulator_sy'].data[0]
    motors['samplez'] = run.baseline.data['Manipulator_sz'].data[0]
    motors['sampler'] = run.baseline.data['Manipulator_sr'].data[0]
    motors['tesz'] = run.baseline.data['tesz'].data[0]
    metadata['scaninfo'] = scaninfo
    metadata['motors'] = motors
    metadata['channelinfo'] = {}
    return metadata


def get_run_data(run):
    
    natural_order = ["Seconds", "MONO", "ENERGY_ENC", "I0", "I1", "REF", "SC", "tfy"]
    exposure = float(run.primary.config['ucal_I400']['ucal_I400_exposure_sp'][0])
    columns = []
    datadict = {}
    for key in run.primary.data:
        newkey = convert_names(key)
        datadict[newkey] = run.primary.data[key].data
    if 'Seconds' not in datadict:
        datadict['Seconds'] = np.zeros_like(datadict[newkey]) + exposure
    for k in natural_order:
        if k in datadict.keys():
            columns.append(k)
    for k in datadict.keys():
        if k not in columns:
            columns.append(k)
    data = [datadict[k] for k in columns]
    return columns, data


def get_tes_data(run, rois, channels=None):
    """
    rois : dictionary of {roi_name: (llim, ulim)}
    """
    d = scandata_from_run(run)
    tes_data = []
    tes_keys = []
    for roi in rois:
        llim, ulim = rois[roi]
        tes_keys.append(roi)
        y, x = d.getScan1d(llim, ulim, channels=channels)
        tes_data.append(y)
    return tes_keys, tes_data


def get_data_and_header(run, infer_rois=True, rois=[], channels=None):
    """
    rois : mixed list of (llim, ulim, roi_name) tuples or roi_names to add to data
    """
    header = get_run_header(run)
    columns, run_data = get_run_data(run)
    _rois = {}
    if infer_rois:
        _rois['tfy'] = roiMaster['tfy']
        e = header['scaninfo'].get('element', '').lower()
        if e in roiDefaults:
            for k in roiDefaults[e]:
                _rois[k] = roiMaster[k]
    for roi in rois:
        if isinstance(roi, str):
            _rois[roi] = roiMaster[roi]
        else:
            _rois[roi[2]] = (roi[0], roi[1])
    tes_keys, tes_data = get_tes_data(run, _rois, channels=channels)
    for i, k in enumerate(tes_keys):
        if k in columns:
            update_idx = columns.index(k)
            run_data[update_idx] = tes_data[i]
        else:
            columns.append(k)
            run_data.append(tes_data[i])
    data = np.vstack(run_data).T
    header['channelinfo']['cols'] = columns
    header['channelinfo']['weights'] = {}
    header['channelinfo']['offsets'] = {}
    return data, header


def get_xas_from_run(run, **kwargs):
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

def export_run(run, folder=None, data_kwargs={}, export_kwargs={}):
    if folder is None:
        folder = get_proposal_directory(run)
    if not path.exists(folder):
        print(f"Making {folder}")
        os.makedirs(folder)
    xas = get_xas_from_run(run, **data_kwargs)
    exportXASToYaml(xas, folder, **export_kwargs)

def export_catalog(catalog, **kwargs):
    for _, run in catalog.items():
        export_run(run, **kwargs)
