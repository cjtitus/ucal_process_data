import mass
import os
from os import path
import numpy as np
import matplotlib.pyplot as plt
import h5py

cal_line_master = {'ck': 278.21, 'nk': 392.25, 'tila': 452, 'ok': 524.45, 'fell': 614.84,
                   'coll': 675.98, 'fk': 677, 'fela': 705.01, 'felb': 717.45,
                   'cola': 775.31, 'colb': 790.21, 'nill': 742.3, 'nila': 848.85,
                   'nilb': 866.11, 'cula': 926.98, 'culb': 947.52,
                   'znla': 1009.39, 'znlb': 1032.46}


def get_line_energies(line_names):
    """
    Takes a list of strings or floats, and returns the line energies in
    cal_line_master.
    """
    line_energies = [cal_line_master.get(n, n) for n in line_names]
    return line_energies


def ds_learnCalibrationPlanFromEnergiesAndPeaks(self, attr, states, ph_fwhm, line_names):
    peak_ph_vals, _peak_heights = mass.algorithms.find_local_maxima(self.getAttr(attr, indsOrStates=states), ph_fwhm)
    _name_e, energies_out, opt_assignments = mass.algorithms.find_opt_assignment(peak_ph_vals, line_names, maxacc=0.1)

    self.calibrationPlanInit(attr)
    for ph, name in zip(opt_assignments, _name_e):
        if type(name) == str:
            self.calibrationPlanAddPoint(ph, name, states=states)
        else:
            energy = name
            name = str(energy)
            self.calibrationPlanAddPoint(ph, name, states=states, energy=energy)

mass.off.Channel.learnCalibrationPlanFromEnergiesAndPeaks = ds_learnCalibrationPlanFromEnergiesAndPeaks


def data_calibrationLoadFromHDF5Simple(self, h5name):
    print(f"loading calibration from {h5name}")
    with h5py.File(h5name, "r") as h5:
        for channum_str in h5.keys():
            cal = mass.calibration.EnergyCalibration.load_from_hdf5(h5, channum_str)
            channum = int(channum_str)
            if channum in self:
                ds = self[channum]
                ds.recipes.add("energy", cal, ["filtValue"], overwrite=True)
    # set other channels bad
    for ds in self.values():
        if "energy" not in ds.recipes.keys():
            ds.markBad("no loaded calibration")

mass.off.ChannelGroup.calibrationLoadFromHDF5Simple = data_calibrationLoadFromHDF5Simple


def data_calibrationSaveToHDF5Simple(self, h5name):
    print(f"writing calibration to {h5name}")
    with h5py.File(h5name, "w") as h5:
        for ds in self.values():
            cal = ds.recipes["energy"].f
            cal.save_to_hdf5(h5, f"{ds.channum}")


mass.off.ChannelGroup.calibrationSaveToHDF5Simple = data_calibrationSaveToHDF5Simple


def find_poly_residual(cal_energies, opt_assignment, degree):
    x = np.insert(opt_assignment, 0, 0.0)
    y = np.insert(cal_energies, 0, 0.0)
    coeff = np.polyfit(x, y, degree)
    poly = np.poly1d(coeff)
    residual = poly(opt_assignment)-cal_energies
    residual_rms = np.sqrt(sum(np.square(residual))/len(cal_energies))
    return coeff, residual, residual_rms


def _calibrate(data, ds, cal_state, line_names, fv="filtValueDC", rms_cutoff=1):
    data.setDefaultBinsize(0.2)
    # ds.plotHist(np.arange(0,30000,10), fv, states=None)
    line_energies = get_line_energies(line_names)
    ds.learnCalibrationPlanFromEnergiesAndPeaks(attr=fv, ph_fwhm=50,
                                                states=cal_state,
                                                line_names=line_energies)

    ds.calibrateFollowingPlan(fv, overwriteRecipe=True, dlo=7, dhi=7)
    # ds.diagnoseCalibration()

    data.alignToReferenceChannel(ds, fv, np.arange(1000, 27000,  10))
    data.calibrateFollowingPlan(fv, dlo=20, dhi=25, overwriteRecipe=True)
    for ds in data:
        ecal = ds.recipes['energy'].f
        degree = min(len(ecal._ph) - 1, 4)
        _, _, rms = find_poly_residual(ecal._energies, ecal._ph, degree)
        if rms > rms_cutoff:
            msg = f"Failed calibration cut with RMS: {rms}, cutoff: {rms_cutoff}"
            print(msg)
            ds.markBad(msg)


def summarize_calibration(calinfo, redo=False):
    savedir = calinfo.savefile[:-4] + '_summary'
    if not os.path.exists(savedir):
        os.mkdirs(savedir)
    line_energies = get_line_energies(calinfo.line_names)
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
                if not os.path.exists(savename) or redo:
                    fig.save(savename)
                fig.close()
            fig = plt.figure(figsize=(2*naxes, 4))
            fig.subplots_adjust(wspace=0)
            axlist = fig.subplots(1, naxes, sharey=True)
            for i in range(naxes):
                name = calinfo.line_names[i]
                energy = line_energies[i]
                axlist[i].set_ylim(energy - 20, energy + 20)
                axlist[i].set_title(name)
            fig.title("Stacked calibration")
            firstchan = chan
        for ax in axlist:
            ax.plot(centers, counts, label=f"Chan {chan}")
        ax.legend()
