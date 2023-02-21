from ..tes.process_classes import scandata_from_run
from xastools.utils import roiMaster, roiDefaults
from xastools.io import exportXASToYaml
from xastools.xas import XAS
from .run import get_proposal_directory
import os
from tiled.queries import Key
from os import path
import datetime
import numpy as np
from functools import reduce
from httpx import HTTPStatusError
from time import sleep

"""
Module to export processed data to analysis catalog
"""

ANALYSIS_CATALOG = None

def convert_names(name):
    name_conversions = {"en_energy_setpoint": "MONO", "en_energy": "ENERGY_ENC", "ucal_I400_i0up": "I0", "ucal_I400_ref": "REF", "ucal_I400_sc": "SC", "ucal_i400_i0up": "I0", "ucal_i400_ref": "REF", "ucal_i400_sc": "SC", "tes_tfy": "tfy", 'ucal_i0up': 'I0', 'ucal_ref': 'REF', 'ucal_sc': 'SC'}
    return name_conversions.get(name, name)


def get_with_fallbacks(thing, *possible_names, default=None):
    for name in possible_names:
        if isinstance(name, (list, tuple)):
            for subname in name:
                if subname in thing:
                    thing = thing[subname]
                    found_thing = True
                else:
                    found_thing = False
            if found_thing:
                return thing
        elif name in thing:
            return thing[name]
    return default


def get_run_header(run):
    metadata = {}
    scaninfo = {}
    scaninfo['sample'] = run.start['sample_md']['name']
    scaninfo['loadid'] = run.start['sample_md']['sample_id']
    scaninfo['scan'] = run.start['scan_id']
    scaninfo['date'] = datetime.datetime.fromtimestamp(run.start['time']).isoformat()
    scaninfo['command'] = get_with_fallbacks(run.start, 'command', 'plan_name', default=None)
    scaninfo['element'] = get_with_fallbacks(run.start, 'element', 'edge', default=None)
    scaninfo['motor'] = convert_names(run.start['motors'][0])
    scankeys = ['time', 'users', 'proposal', 'cycle', 'saf', 'group_md', 'beamtime_uid', 'beamtime_start', 'repeat']
    for k in scankeys:
        if k in run.start:
            scaninfo[k] = run.start[k]
    if 'ref_args' in run.start:
        scaninfo['ref_edge'] = run.start['ref_args']['i0up_multimesh_sample_sample_name']['value']
        scaninfo['ref_id'] = run.start['ref_args']['i0up_multimesh_sample_sample_id']['value']
    scaninfo['raw_uid'] = run.start['uid']
    motors = {}
    baseline = run.baseline.data
    motors['exslit'] = get_with_fallbacks(baseline, 'Exit Slit of Mono Vertical Gap')[0]
    motors['manipx'] = get_with_fallbacks(baseline, 'manip_x', 'Manipulator_x')[0]
    motors['manipy'] = get_with_fallbacks(baseline, 'manip_y', 'Manipulator_y')[0]
    motors['manipz'] = get_with_fallbacks(baseline, 'manip_z', 'Manipulator_z')[0]
    motors['manipr'] = get_with_fallbacks(baseline, 'manip_r', 'Manipulator_r')[0]
    motors['samplex'] = get_with_fallbacks(baseline, 'manip_sx', 'Manipulator_sx')[0]
    motors['sampley'] = get_with_fallbacks(baseline, 'manip_sy', 'Manipulator_sy')[0]
    motors['samplez'] = get_with_fallbacks(baseline, 'manip_sz', 'Manipulator_sz')[0]
    motors['sampler'] = get_with_fallbacks(baseline, 'manip_sr', 'Manipulator_sr')[0]
    motors['tesz'] = get_with_fallbacks(baseline, 'tesz')[0]
    metadata['scaninfo'] = scaninfo
    metadata['motors'] = motors
    metadata['channelinfo'] = {}
    return metadata


def get_run_data(run):

    natural_order = ["Seconds", "MONO", "ENERGY_ENC", "I0", "I1", "REF", "SC",
                     "tfy"]
    config = run.primary.descriptors[0]['configuration']
    exposure = get_with_fallbacks(config, ['ucal_i0up', 'data', 'ucal_i0up_exposure_time'],
                                  ['ucal_sc', 'data', 'ucal_sc_exposure_time'],
                                  ['ucal_i400_i0up', 'data', 'ucal_i400_i0up_exposure_time'],
                                  ['ucal_i400_sc', 'data', 'ucal_i400_sc_exposure_time'],
                                  ['ucal_I400', 'data', 'ucal_I400_exposure_sp'])
    exposure = float(exposure)
    columns = []
    datadict = {}
    for key in run.primary.data:
        newkey = convert_names(key)
        datadict[newkey] = run.primary.data[key][:]
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
    header['channelinfo']['rois'] = {}
    for key, roi in _rois.items():
        header['channelinfo']['rois'][key] = roi
    return data, header


def export_run_to_analysis_catalog(run, infer_rois=True, rois=[], channels=None, check_existing=True):
    global ANALYSIS_CATALOG
    if ANALYSIS_CATALOG is None:
        from tiled.client import from_profile
        c = from_profile('nsls2')['ucal']['sandbox']
        ANALYSIS_CATALOG = c
    if check_existing:
        uid = run.metadata['start']['uid']
        if len(ANALYSIS_CATALOG.search(Key("scaninfo.raw_uid") == uid)) > 0:
            print("Data associated with this run is already in the catalog")
            return

    data, header = get_data_and_header(run, infer_rois=infer_rois, rois=rois,
                                       channels=channels)

    try:
        new_uid = ANALYSIS_CATALOG.write_array(data, metadata=header,
                                               specs=['nistxas'])
    except HTTPStatusError:
        print("Got an HTTP Error, will sleep and retry once")
        sleep(1)
        new_uid = ANALYSIS_CATALOG.write_array(data, metadata=header,
                                               specs=['nistxas'])
    return new_uid
