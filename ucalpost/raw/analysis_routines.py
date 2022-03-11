import mass

import os
import numpy as np

from .calibration import summarize_calibration


# Understand how to intelligently re-drift-correct as data comes in
def _drift_correct(data):
    data.learnDriftCorrection()


def drift_correct(rd):
    """
    rd : A RawData object
    """
    if not rd.driftCorrected:
        print("Drift Correcting")
        _drift_correct(rd.data)
        rd.load_ds()
        rd.driftCorrected = True
    else:
        print("Drift Correction already done")


# Need to determine when to re-calibrate
def calibrate(rd, calinfo, redo=False, rms_cutoff=2):
    """
    rd : A RawData object
    calinfo : a CalibrationInfo object
    """
    if not rd.calibrated:
        print("Calibrating")
        calinfo.calibrate(redo=redo, rms_cutoff=rms_cutoff)
        rd.data.calibrationLoadFromHDF5Simple(calinfo.cal_file)
        rd.calibrated = True
    else:
        print("Calibration already present")


def process(rd, calinfo, redo=False, rms_cutoff=2):
    # cal transfer doesn't use dc anyway yet
    # drift_correct(rd)
    calibrate(rd, calinfo, redo=redo, rms_cutoff=2)
    summarize_calibration(calinfo, redo=redo)


def save_tes_arrays(rd, overwrite=False):
    savefile = rd.savefile
    state = rd.state
    savedir = os.path.dirname(savefile)
    if not os.path.exists(savedir):
        os.makedirs(savedir)
    if os.path.exists(savefile) and not overwrite:
        print(f"Not overwriting {savefile}")
        return

    timestamps = []
    energies = []
    channels = []
    for ds in rd.data.values():
        try:
            uns, es = ds.getAttr(["unixnano", "energy"], state)
        except:
            print(f"{ds.channum} failed")
            ds.markBad("Failed to get energy")
        ch = np.zeros_like(uns) + ds.channum
        timestamps.append(uns)
        energies.append(es)
        channels.append(ch)
    ts_arr = np.concatenate(timestamps)
    en_arr = np.concatenate(energies)
    ch_arr = np.concatenate(channels)
    sort_idx = np.argsort(ts_arr)
    print(f"Saving {savefile}")
    np.savez(savefile,
             timestamps=ts_arr[sort_idx],
             energies=en_arr[sort_idx],
             channels=ch_arr[sort_idx])
