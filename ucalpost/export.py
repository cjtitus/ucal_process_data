from .process_classes import scandata_from_run
from xastools.utils import roiMaster, roiDefaults


def get_run_header(run):
    metadata = {}
    scaninfo = {}
    scaninfo['sample'] = run.start['sample_args']['sample_name']['value']
    scaninfo['loadid'] = run.start['sample_args']['sample_id']['value']
    scaninfo['scan'] = run.start['scan_id']
    scaninfo['date'] = datetime.datetime.fromtimestamp(run.start['time']).isoformat()
    scaninfo['command'] = run.start.get('command', run.start.get('plan_name', None))
    scaninfo['element'] = run.start.get('element', run.start.get('edge', None))
    scaninfo['motor'] = run.start['motors'][0]
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
    metadata['channelinfo'] = []
    return metadata


def get_run_data(run):
    name_conversions = {"en_energy_setpoint": "MONO", "en_energy": "ENERGY_ENC", "ucal_I400_i0up": "I0", "ucal_I400_ref": "REF", "ucal_I400_sc": "SC", "tes_tfy": "testfy"}
    natural_order = ["Seconds", "MONO", "ENERGY_ENC", "I0", "I1", "REF", "SC", "testfy"]
    exposure = float(run.primary.config['ucal_I400']['ucal_I400_exposure_sp'][0])
    columns = []
    datadict = {}
    for key in run.primary.data:
        newkey = name_conversions.get(key, key)
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
        e = header['scaninfo']['element']
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
    data = np.vstack(run_data)
    header['channelinfo']['columns'] = columns
    header['channelinfo']['weights'] = {}
    header['channelinfo']['offsets'] = {}
    return data, header


