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


def summarize_calibration(calinfo):
    savedir = calinfo.savefile[:-4] + '_summary'
    if not os.path.exists(savedir):
        os.mkdirs(savedir)
    
    nstack = 7
    for n, chan in enumerate(calinfo.data):
        ds = calinfo.data[chan]
        energies = ds.getAttr("energy", calinfo.state)
        bins = np.arange(200, 1000, 1)
        # work in progress
        if n%nstack == 0:
            if n != 0:
                ax.legend()
            fig = plt.figure()
            ax = fig.add_subplot(111)
            ax.set_xlabel("Emission energy (eV)")
            ax.set_ylabel("Counts")
            fig.title("Stacked calibration")
        c, e = caldata.getEmission(200, 1000, channels=[chan])
        ax.plot(e, c, label=f"Chan {chan}")
    ax.legend()
