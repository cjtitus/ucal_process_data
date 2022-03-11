import mass
from mass.calibration.algorithms import line_names_and_energies
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

    data.alignToReferenceChannel(ds, fv, np.arange(1000, 27000,  10))
    data.calibrateFollowingPlan(fv, dlo=20, dhi=25, overwriteRecipe=True)


# Need to determine when to re-calibrate
def calibrate(rd, calinfo, redo=False):
    """
    rd : A RawData object
    calinfo : a CalibrationInfo object
    """
    if not rd.calibrated:
        print("Calibrating")
        calinfo.calibrate(redo=redo)
        rd.data.calibrationLoadFromHDF5Simple(calinfo.cal_file)
        rd.calibrated = True
    else:
        print("Calibration already present")


def process(rd, calinfo, redo=False):
    # cal transfer doesn't use dc anyway yet
    # drift_correct(rd)
    calibrate(rd, calinfo, redo=redo)


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
    lines = line_names_and_energies(calinfo.line_names)
    nstack = 7
    naxes = len(calinfo.line_names)
    for n, chan in enumerate(calinfo.data):
        ds = calinfo.data[chan]
        energies = ds.getAttr("energy", calinfo.state)
        bins = np.arange(200, 1000, 1)
        centers = 0.5*(bins[1:] + bins[:-1])
        counts = np.histogram(energies, bins)
        # work in progress
        if n % nstack == 0:
            if n != 0:
                filename = f"cal_{firstchan}_to_{chan}.png"
                savename = os.path.join(savedir, filename)
                fig.save(savename)
            fig = plt.figure(figsize=(2*naxes, 4))
            fig.subplots_adjust(wspace=0)
            axlist = fig.subplots(1, naxes, sharey=True)
            for i in range(naxes):
                name = lines[0][i]
                energy = lines[1][i]
                axlist[i].set_ylim(energy - 20, energy + 20)
                axlist[i].set_title(name)
            fig.title("Stacked calibration")
            firstchan = chan
        for ax in axlist:
            ax.plot(centers, counts, label=f"Chan {chan}")
        ax.legend()
