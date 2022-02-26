from run_info import (getRunFromStop, get_filename, get_cal, get_tes_state,
                      get_line_names, get_save_directory)
from analysis_classes import RawData, CalibrationInfo
from analysis_routines import process, save_tes_arrays
from bluesky.callbacks.zmq import RemoteDispatcher


# Need to do caching of open dataset
# Just re-do drift_correct when new data comes in?
# Just re-do calibration when new calibration data comes in?
# Need to save data
# Only works when cal and data are in same file

class AnalysisLoader:
    def __init__(self):
        self.off_filename = None
        self.rd = None
        
    def getAnalysisObjects(self, run, cal=None):
        off_filename = get_filename(run)
        if self.rd is None:
            self.rd = RawData(off_filename)
            self.off_filename = off_filename
        elif off_filename != self.off_filename:
            self.rd = RawData(off_filename)
            self.off_filename = off_filename
        else:
            self.rd.refresh()
            # scan_filename = get_logname(run)
        if cal is None:
            cal = get_cal(run)
        cal_state = get_tes_state(cal)
        line_names = get_line_names(cal)
        calinfo = CalibrationInfo(cal_state, line_names)
        return self.rd, calinfo


def run_analysis(run, loader=None, cal=None):
    if loader is None:
        loader = AnalysisLoader()
    rd, calinfo = loader.getAnalysisObjects(run, cal)
    state = get_tes_state(run)
    print(f"Processing {rd.off_filename}, state: {state}")
    process(rd, calinfo)
    savedir = get_save_directory(run)
    print(f"Saving TES Arrays to {savedir}")
    save_tes_arrays(rd, savedir, state)


def getDocumentHandler():
    loader = AnalysisLoader()    
    def _handler(name, doc):
        if name == "stop":
            #run_analysis(stop)
            run = getRunFromStop(stop)
            run_analysis(run, loader)
        else:
            print(name)
    return _handler

if __name__ == "__main__":
    d = RemoteDispatcher('localhost:5578')
    d.subscribe(getDocumentHandler())
    print("Ready for documents, starting handler")
    d.start()