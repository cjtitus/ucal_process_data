import mass
from mass.calibration.algorithms import line_names_and_energies
import os
from os import path
import itertools
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


def find_opt_assignment(peak_positions, line_names, nextra=2, nincrement=3,
                        nextramax=8, rms_cutoff=1.0):
    """Tries to find an assignment of peaks to line names that is reasonably self consistent and smooth

    Args:
        peak_positions (np.array(dtype=float)): a list of peak locations in arb units,
            e.g. p_filt_value units
        line_names (list[str or float)]): a list of calibration lines either as number (which is
            energies in eV), or name to be looked up in STANDARD_FEATURES
        nextra (int): the algorithm starts with the first len(line_names) + nextra peak_positions
        nincrement (int): each the algorithm fails to find a satisfactory peak assignment, it uses
            nincrement more lines
        nextramax (int): the algorithm stops incrementint nextra past this value, instead
            failing with a ValueError saying "no peak assignment succeeded"
        rms_cutoff (float): an empirical number that determines if an assignment is good enough.
            The default number works reasonably well for NSLS-II data
    """
    name_e, e_e = line_names_and_energies(line_names)
    polyorder = 4
    n_sel_pp = len(line_names) + nextra  # number of peak_positions to use to line up to line_names
    nmax = len(line_names) + nextramax
    while True:
        sel_positions = np.asarray(peak_positions[:n_sel_pp], dtype="float")
        energies = np.asarray(e_e, dtype="float")
        assign = np.array(list(itertools.combinations(sel_positions, len(line_names))))
        assign.sort(axis=1)
        acc_est = []
        for n in range(assign.shape[0]):
            _, _, rms = find_poly_residual(energies, assign[n, :], polyorder)
            acc_est.append(rms)
        opt_assign_i = np.argmin(acc_est)
        acc = acc_est[opt_assign_i]
        opt_assign = assign[opt_assign_i]

        if acc > rms_cutoff:
            n_sel_pp += nincrement
            if n_sel_pp > nmax:
                print("no peak assignment succeeded: acc %g, rms_cutoff %g" %
                      (acc, rms_cutoff))
                return name_e, energies, list(opt_assign)
                # raise ValueError("no peak assignment succeeded: acc %g, rms_cutoff %g" %
                # (acc, rms_cutoff))
            else:
                continue
        else:
            return name_e, energies, list(opt_assign)


def ds_learnCalibrationPlanFromEnergiesAndPeaks(self, attr, states, ph_fwhm, line_names, assignment="nsls"):
    peak_ph_vals, _peak_heights = mass.algorithms.find_local_maxima(self.getAttr(attr, indsOrStates=states), ph_fwhm)
    if assignment == "nsls":
        _name_e, energies_out, opt_assignments = find_opt_assignment(peak_ph_vals,
                                                                     line_names, rms_cutoff=1)
    else:
        _name_e, energies_out, opt_assignments = mass.algorithms.find_opt_assignment(peak_ph_vals,
                                                                                     line_names, maxacc=0.1)

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


def _calibrate(data, cal_state, line_names, fv="filtValueDC", rms_cutoff=1, assignment="nsls"):
    data.setDefaultBinsize(0.2)
    # ds.plotHist(np.arange(0,30000,10), fv, states=None)
    line_energies = get_line_energies(line_names)
    # ds.diagnoseCalibration()
    for ds in data.values():
        try:
            ds.learnCalibrationPlanFromEnergiesAndPeaks(attr=fv, ph_fwhm=50,
                                                        states=cal_state,
                                                        line_names=line_energies,
                                                        assignment=assignment)
        except ValueError:
            print("Chan {ds.channum} failed peak assignment")
            ds.markBad("Failed peak assignment")

    #data.alignToReferenceChannel(ds, fv, np.arange(1000, 27000,  10))
    data.calibrateFollowingPlan(fv, dlo=7, dhi=7, overwriteRecipe=True)
    for ds in data.values():
        # ds.calibrateFollowingPlan(fv, overwriteRecipe=True, dlo=7, dhi=7)

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
