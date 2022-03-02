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
    #drift_correct(rd) 
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

############################################################
# Plotting stuff                                           #
############################################################
if False:
    data.plotHist(np.arange(0,1000,0.2), "energy", coAddStates=False)

    scan = DataScan.from_file(scan_filename)
    data.cutAdd("fepfy", lambda energy: np.logical_and(energy>690, energy<730))

    bin_edges_ev = np.arange(200, 800, 0.3)
    bin_edges_uns = np.array(scan.epoch_time_start_s+scan.epoch_time_end_s[-1:])*1e9
    t_per_bin = (bin_edges_uns[1:] - bin_edges_uns[:-1])/1e9
    coadded = 0

    for ds in data.values():
        uns, es = ds.getAttr(["unixnano", "energy"], "SCAN4")
        counts, _, _ = np.histogram2d(uns, es, bins=(bin_edges_uns, bin_edges_ev))
        coadded += counts

    xx, yy = np.meshgrid(scan.var_values, bin_edges_ev[1:])
    zz = coadded.T


    def cut(mgrid, egrid, cgrid, llim, ulim):
        """Cut a RIXS plane into a smaller energy range

        :param mgrid: 2-D mono array
        :param egrid: 2-D energy array
        :param cgrid: 2-D counts array
        :param llim: Lower energy range
        :param ulim: Upper energy range
        :returns: mgrid, egrid, cgrid between llim and ulim

        """
        energy = egrid[:, 0]
        lidx = np.argmin(np.abs(energy - llim))
        uidx = np.argmin(np.abs(energy - ulim))
        return mgrid[lidx:uidx, :], egrid[lidx:uidx, :], cgrid[lidx:uidx, :]


    def comboPlot(llim, ulim, title):
        x, y, z = cut(xx, yy, zz, llim, ulim)
        fig = plt.figure()
        ax1 = fig.add_subplot(211)
        ax1.plot(x[0, :], z.sum(axis=0)/t_per_bin)
        ax2 = fig.add_subplot(212)
        ax2.contourf(x, y, z, 30, cmap="viridis")
        ax2.set_xlabel("mono energy (eV)")
        ax2.set_ylabel("emission energy (eV)")
        ax1.set_xlim(690, 740)
        ax1.set_xticks([])
        ax1.set_ylabel("Counts per second")
        ax2.set_xlim(690, 740)
        fig.subplots_adjust(hspace=0)
        ax1.set_title(title)


    plt.figure()
    plt.contourf(xx, yy, zz, 30, cmap="viridis")
    plt.xlabel("mono energy (eV)")
    plt.ylabel("emission energy (eV)")

    comboPlot(500, 550, r"Oxygen K$_{\alpha}$ RIXS and IPFY")
    comboPlot(600, 640, r"Fe L$_l$ RIXS and PFY")




    comboPlot(690, 730, r"Fe $L_{\alpha,\beta}$ RIXS and PFY")
    t_per_bin = (bin_edges_uns[1:] - bin_edges_uns[:-1])/1e9

