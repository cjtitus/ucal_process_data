from tiled.client import from_profile
from os import path
from databroker.queries import TimeRange
import datetime

db = from_profile("ucal")


def get_config_dict(run):
    return run.primary.descriptors[0]['configuration']['tes']['data']


def get_filename(run):
    filename = get_config_dict(run)['tes_filename']
    return filename


def get_logname(run):
    config = get_config_dict(run)
    state_str = 'calibration' if config['tes_cal_flag'] else 'scan'
    scan_num = config['tes_scan_num']
    filebase = path.dirname(get_filename(run))
    filename = f"{state_str}{scan_num:0=4d}.json"
    return path.join(filebase, "logs", filename)


def get_save_directory(run):
    basename = "/nsls2/data/sst1/legacy/ucal/processed"
    timestamp = run.metadata['start']['time']
    date = datetime.datetime.fromtimestamp(timestamp)
    return path.join(basename, f"{date.year}/{date.month:02d}/{date.day:02d}")


def get_tes_state(run):
    config = get_config_dict(run)
    if "tes_scan_str" in config:
        state = config['tes_scan_str']
    else:
        state_str = 'CAL' if config['tes_cal_flag'] else 'SCAN'
        scan_num = config['tes_scan_num']
        state = f"{state_str}{scan_num}"
    return state


def get_line_names(cal_run):
    return ["ck", "nk", "ok", "fela", "felb", "nila", "cula"]


def get_cal(run):
    return db['ec72ac48']


def getRunFromStop(doc):
    run_uuid = doc['run_start']
    run = db[run_uuid]
    return run


def get_samplename(run):
    return run.start['sample_args']['sample_name']['value']


def summarize_run(run):
    scanid = run.metadata['start']['scan_id']
    sample = get_samplename(run)
    scantype = run.metadata['start']['scantype']
    if scantype == 'xas':
        edge = run.metadata['start']['edge']
    print(f"Scan {scanid}")
    print(f"Sample name: {sample}")
    if scantype == 'xas':
        edge = run.metadata['start']['edge']
        print(f"Scantype: {scantype}, edge: {edge}")
    else:
        print(f"Scantype: {scantype}")

#tes_runs = db.search(TimeRange(since="2022-01-26", until="2022-01-28"))
#sample_runs = tes_runs.search({"sample_args": {"$exists": True}})
#run = sample_runs[-1]
