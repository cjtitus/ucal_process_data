from run_info import getAnalysisObjects, getRunFromStop
from analysis_routines import process


# Need to do caching of open dataset
# Just re-do drift_correct when new data comes in?
# Just re-do calibration when new calibration data comes in?
# Need to save data
def run_analyis(stop):
    run = getRunFromStop(stop)
    rd, calinfo = getAnalysisObjects(run)
    process(rd, calinfo)
