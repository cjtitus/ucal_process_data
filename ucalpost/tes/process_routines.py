from ..databroker.run import (get_filename, get_cal, get_tes_state,
                              get_line_names, get_save_directory)
from .raw_classes import RawData, CalibrationInfo
from .raw_routines import process, save_tes_arrays
from .process_classes import get_analyzed_filename


# Need to do caching of open dataset
# Just re-do drift_correct when new data comes in?
# Just re-do calibration when new calibration data comes in?
# Need to save data
# Only works when cal and data are in same file

class AnalysisLoader:
    def __init__(self):
        self.off_filename = None
        self.rd = None
        self.ci = None

    def getAnalysisObjects(self, run, cal=None):
        off_filename = get_filename(run)
        state = get_tes_state(run)
        savefile = get_analyzed_filename(run)
        if self.rd is None:
            self.rd = RawData(off_filename, state, savefile)
            self.off_filename = off_filename
        elif off_filename != self.off_filename:
            self.rd = RawData(off_filename, state, savefile)
            self.off_filename = off_filename
        else:
            self.rd.update(state, savefile)
        if cal is None:
            if run.start.get('scantype', None) == 'calibration':
                cal = run
            else:
                cal = get_cal(run)
        cal_savedir = get_save_directory(cal)
        cal_state = get_tes_state(cal)
        line_names = get_line_names(cal)
        cal_filename = get_filename(cal)
        cal_savefile = get_analyzed_filename(cal)

        if self.ci is None:
            self.ci = CalibrationInfo(cal_filename, cal_state, cal_savefile,
                                      cal_savedir, line_names, data=self.rd.data)
            self.cal_filename = cal_filename
        elif cal_filename != self.cal_filename:
            self.ci = CalibrationInfo(cal_filename, cal_state, cal_savefile,
                                      cal_savedir, line_names, data=self.rd.data)
            self.cal_filename = cal_filename
        else:
            self.ci.update(cal_state, cal_savefile, cal_savedir, line_names)
        return self.rd, self.ci


def process_run(run, loader=None, cal=None, redo=False, overwrite=False, **kwargs):
    if loader is None:
        loader = AnalysisLoader()
    rd, calinfo = loader.getAnalysisObjects(run, cal)
    print(f"Processing {rd.off_filename}, state: {rd.state}")
    process(rd, calinfo, redo=redo, overwrite=overwrite, **kwargs)
    print(f"Saving TES Arrays to {rd.savefile}")
    save_tes_arrays(rd, overwrite=overwrite)


def process_catalog(catalog, **kwargs):
    loader = AnalysisLoader()
    for run in catalog.values():
        try:
            process_run(run, loader, **kwargs)
        except Error as e:
            print("Problem processing {run}")
            raise e
