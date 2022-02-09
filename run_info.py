from tiled.client import from_profile
from os import path
from databroker.queries import TimeRange
from analysis_classes import RawData, CalibrationInfo
import datetime

db = from_profile("ucal")

def get_filename(run):
    basename = "/nsls2/users/ctitus/data"
    filename = run.primary.descriptors[0]['configuration']['tes']['data']['tes_filename']
    return path.join(basename, *filename.split("/")[-3:])


def get_logname(run):
    num = run.primary.descriptors[0]['configuration']['tes']["data"]['tes_scan_num']
    state = "cal" if run.primary.descriptors[0]['configuration']['tes']["data"]['tes_cal_flag'] else "scan"
    filebase = path.dirname(get_filename(run))
    filename = f"{state}{num:0=4d}.json"
    return path.join(filebase, "logs", filename)


def get_save_directory(run):
    basename = "/nsls2/data/sst1/legacy/ucal/raw"
    timestamp = run.metadata['start']['timestamp']
    date = datetime.datetime.fromtimestamp(timestamp)
    return path.join(basename, f"{date.year}/{date.month:02d}/{date.day:02d}")


def get_cal_state(cal_run):
    state = cal_run.primary.descriptors[0]['configuration']['tes']['data']['tes_scan_num']
    return f"CAL{state}"


def get_line_names(cal_run):
    return ["CKAlpha", "NKAlpha", "OKAlpha", "FeLAlpha", "NiLAlpha", 'CuLAlpha']


def get_cal(run):
    return db['ec72ac48']


def getAnalysisObjects(run):
    off_filename = get_filename(run)
    # scan_filename = get_logname(run)
    cal = get_cal(run)
    cal_state = get_cal_state(cal)
    line_names = get_line_names(cal)
    savedir = get_save_directory(run)
    rd = RawData(off_filename, savedir=savedir)
    calinfo = CalibrationInfo(cal_state, line_names)
    return rd, calinfo


def getRunFromStop(doc):
    run_uuid = doc['run_start']
    run = db[run_uuid]
    
tes_runs = db.search(TimeRange(since="2022-01-26", until="2022-01-28"))
sample_runs = tes_runs.search({"sample_args": {"$exists": True}})
run = sample_runs[-1]
