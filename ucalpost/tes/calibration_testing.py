from ucalpost.databroker.run import get_filename, get_tes_state
from os.path import basename
from ucalpost.tes.loader import AnalysisLoader
import numpy as np
import matplotlib.pyplot as plt

plt.ion()


def match_run(run, date, num, state):
    fname = get_filename(run)
    rundate, runnum = basename(fname).split("_")[:2]
    if rundate == date and runnum == f"run{num:04d}":
        runstate = get_tes_state(run)
        if runstate == state:
            return True
    return False


def find_run(catalog, date, num, state):
    """
    catalog : a catalog of bluesky runs
    date : a string in YYYYMMDD format
    num : int, TES filename number
    state : str, "CAL0" or "RUN56" or something
    """
    for run in catalog.values():
        if match_run(run, date, num, state):
            return run
    return None


def get_calinfo(catalog, date, num, state):
    run = find_run(catalog, date, num, state)
    loader = AnalysisLoader()
    rd, ci = loader.getAnalysisObjects(run)
    return ci


def test_calibration(calinfo, rms_cutoff=0.2, **kwargs):
    """
    Calibrates data without saving anything.
    """
    attr = "filtValueDC" if calinfo.driftCorrected else "filtValue"

    calinfo.data.calibrate(
        calinfo.state, calinfo.line_names, fv=attr, rms_cutoff=rms_cutoff, **kwargs
    )


def plot_ds_histogram(ds, attr, state, bmin, bmax, axlist, step=1, legend=True):
    bins = np.arange(bmin, bmax, step)
    centers = 0.5 * (bins[1:] + bins[:-1])
    energies = ds.getAttr(attr, state)
    counts, _ = np.histogram(energies, bins)

    for ax in axlist:
        ax.plot(centers, counts, label=f"Chan {ds.channum}")
    if legend:
        ax.legend()
