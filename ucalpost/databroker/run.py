# from tiled.client import from_profile
from os import path
from databroker.queries import TimeRange
import datetime
import numpy as np


def get_noise(run, catalog):
    noise_uid = get_config_dict(run)["tes_noise_uid"]
    return catalog[noise_uid]


def get_projectors(run, catalog):
    projectors_uid = get_config_dict(run)["tes_projector_uid"]
    return catalog[projectors_uid]


def get_config_dict(run):
    return run.primary.descriptors[0]["configuration"]["tes"]["data"]


def get_filename(run, convert_local=True):
    filename = get_config_dict(run)["tes_filename"]
    if convert_local:
        raw_directory = get_raw_directory(filename)
        a, name = path.split(filename)
        _, runNumber = path.split(a)
        filename = path.join(raw_directory, runNumber, name)
    return filename


def get_logname(run):
    config = get_config_dict(run)
    state_str = "calibration" if config["tes_cal_flag"] else "scan"
    scan_num = config["tes_scan_num"]
    filebase = path.dirname(get_filename(run))
    filename = f"{state_str}{scan_num:0=4d}.json"
    return path.join(filebase, "logs", filename)


def get_save_directory(run):
    basename = "/nsls2/data/sst/legacy/ucal/processed"
    timestamp = run.metadata["start"]["time"]
    date = datetime.datetime.fromtimestamp(timestamp)
    return path.join(basename, f"{date.year}/{date.month:02d}/{date.day:02d}")


def get_raw_directory(filename):
    date = path.basename(filename).split("_")[0]
    year = date[:4]
    month = date[4:6]
    day = date[6:]
    basename = "/nsls2/data/sst/legacy/ucal/raw"
    return path.join(basename, f"{year}/{month}/{day}")


def get_proposal_directory(run):
    passid = run.metadata["start"]["proposal"]
    date = datetime.datetime.fromtimestamp(
        run.metadata["start"].get("beamtime_start", run.metadata["start"]["time"])
    )
    cycle = run.metadata["start"]["cycle"]
    dirname = f"/nsls2/data/sst/legacy/ucal/proposals/{date.year}-{cycle}/pass-{passid}/ucal/{date.year}{date.month:02}{date.day:02}"
    return dirname


def get_tes_state(run):
    config = get_config_dict(run)
    if "tes_scan_str" in config:
        state = config["tes_scan_str"]
    else:
        state_str = "CAL" if config["tes_cal_flag"] else "SCAN"
        scan_num = config["tes_scan_num"]
        state = f"{state_str}{scan_num}"
    return state


def get_line_names(cal_run):
    if "cal_lines" in cal_run.start:
        return cal_run.start["cal_lines"]
    samplename = get_samplename(cal_run)
    if samplename == "mixv1":
        energy = cal_run.start.get("calibration_energy", 980)
        line_energies = np.array([300, 400, 525, 715, 840, 930, 950, 1020])
        line_names = np.array(
            ["ck", "nk", "ok", "fela", "nila", "cula", "culb", "znla"]
        )
        return list(line_names[line_energies < energy])
    else:
        return ["ck", "nk", "ok", "fela", "nila", "cula"]


def get_cal_id(run, default=None):
    cal_id = run.start["last_cal"]
    if cal_id is None:
        return default
    return cal_id


def get_cal(run, catalog):
    return catalog[run.start["last_cal"]]


def getRunFromStop(doc, catalog):
    run_uuid = doc["run_start"]
    run = catalog[run_uuid]
    return run


def get_group(run):
    if "group_name" in run.start:
        return run.start["group_name"]
    elif "group_md" in run.start:
        return run.start["group_md"]["name"]
    else:
        return "None"


def get_samplename(run):
    if "sample_name" in run.start:
        return run.start["sample_name"]
    elif "sample_md" in run.start:
        return run.start["sample_md"]["name"]
    elif "sample_args" in run.start:
        return run.start["sample_args"]["sample_name"]["value"]
    else:
        return "None"


def get_sampleid(run):
    if "sample_id" in run.start:
        return run.start["sample_id"]
    elif "sample_md" in run.start:
        return run.start["sample_md"]["sample_id"]
    elif "sample_args" in run.start:
        return run.start["sample_args"]["sample_id"]["value"]
    else:
        return "None"


def summarize_run(run):
    scanid = run.metadata["start"]["scan_id"]
    sample = get_samplename(run)
    scantype = run.start.get("scantype", "None")

    print(f"Scan {scanid}")
    if "group_md" in run.start:
        print(f"Group: {run.start['group_md']['name']}")
    elif "group" in run.start:
        print(f"Group: {run.start['group']}")
    print(f"Sample name: {sample}")
    if scantype == "xas":
        edge = run.start.get("edge", "Not recorded")
        print(f"Scantype: {scantype}, edge: {edge}")
    else:
        print(f"Scantype: {scantype}")
    if "last_cal" in run.start:
        print(f"Calibration: {run.start['last_cal']!s:.8}")


# tes_runs = db.search(TimeRange(since="2022-01-26", until="2022-01-28"))
# sample_runs = tes_runs.search({"sample_args": {"$exists": True}})
# run = sample_runs[-1]
