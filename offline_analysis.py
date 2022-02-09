from run_info import (getRunFromStop, get_filename, get_cal, get_tes_state,
                      get_line_names, get_save_directory)
from analysis_classes import RawData, CalibrationInfo
from analysis_routines import process, save_tes_arrays


# Need to do caching of open dataset
# Just re-do drift_correct when new data comes in?
# Just re-do calibration when new calibration data comes in?
# Need to save data
def getAnalysisObjects(run):
    off_filename = get_filename(run)
    # scan_filename = get_logname(run)
    cal = get_cal(run)
    cal_state = get_tes_state(cal)
    line_names = get_line_names(cal)
    rd = RawData(off_filename)
    calinfo = CalibrationInfo(cal_state, line_names)
    return rd, calinfo


def run_analyis(stop):
    run = getRunFromStop(stop)
    rd, calinfo = getAnalysisObjects(run)
    process(rd, calinfo)
    state = get_tes_state(run)
    savedir = get_save_directory(run)
    save_tes_arrays(rd, savedir, state)
