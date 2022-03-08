import mass
import mass.off
import os
import numpy as np
import matplotlib.pyplot as plt


# Horrible monkeypatch
def ds_learnCalibrationPlanFromEnergiesAndPeaks(self, attr, states, ph_fwhm, line_names):
    peak_ph_vals, _peak_heights = mass.algorithms.find_local_maxima(self.getAttr(attr, indsOrStates=states), ph_fwhm)
    _name_e, energies_out, opt_assignments = mass.algorithms.find_opt_assignment(peak_ph_vals, line_names, maxacc=0.1)

    self.calibrationPlanInit(attr)
    for ph, name in zip(opt_assignments, _name_e):
        self.calibrationPlanAddPoint(ph, name, states=states)


mass.off.Channel.learnCalibrationPlanFromEnergiesAndPeaks = ds_learnCalibrationPlanFromEnergiesAndPeaks


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


def _calibrate(data, ds, cal_state, line_names, fv="filtValueDC"):
    data.setDefaultBinsize(0.2)
    # ds.plotHist(np.arange(0,30000,10), fv, states=None)

    ds.learnCalibrationPlanFromEnergiesAndPeaks(attr=fv, ph_fwhm=50,
                                                states=cal_state,
                                                line_names=line_names)

    ds.calibrateFollowingPlan(fv, overwriteRecipe=True, dlo=20, dhi=25)
    # ds.diagnoseCalibration()

    data.alignToReferenceChannel(ds, fv, np.arange(0, 20000,  10))
    data.calibrateFollowingPlan(fv, dlo=20, dhi=25, overwriteRecipe=True)


# Need to determine when to re-calibrate
def calibrate(rd, calinfo):
    """
    rd : A RawData object
    calinfo : a CalibrationInfo object
    """
    if not rd.calibrated:
        print("Calibrating")
        calinfo.calibrate()
        rd.data.calibrationLoadFromHDF5Simple(calinfo.cal_file)
        rd.calibrated = True
    else:
        print("Calibration already present")


def process(rd, calinfo):
    # cal transfer doesn't use dc anyway yet
    # drift_correct(rd)
    calibrate(rd, calinfo)


def save_tes_arrays(rd, savedir, state):
    if not os.path.exists(savedir):
        os.makedirs(savedir)
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
    savefile = os.path.join(savedir, f"tes_{state}")
    print(f"Saving {savefile}")
    np.savez(savefile,
             timestamps=ts_arr[sort_idx],
             energies=en_arr[sort_idx],
             channels=ch_arr[sort_idx])
