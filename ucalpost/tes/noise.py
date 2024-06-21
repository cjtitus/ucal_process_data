from ..databroker.run import get_noise, get_projectors, get_filename
import mass
import os
from os.path import basename, join
from . import mass_addons
import matplotlib.pyplot as plt

plt.ion()


def get_noise_and_projectors(run, c):
    scantype = run.start.get("scantype", "None")
    if scantype == "projectors":
        projectors = run
    else:
        projectors = get_projectors(run, c)
    noise = get_noise(projectors, c)
    return noise, projectors


def get_noise_data(noise, projectors, invert=True):
    data = load_mass(noise, projectors, invert=invert)
    prep_data(data)
    return data


def plot_noise(data, savedir=None):
    fig = plt.figure()
    ax = fig.add_subplot()
    data.plot_noise(axis=ax, legend=False)

    if savedir is not None:
        if not os.path.exists(savedir):
            os.makedirs(savedir)
        savename = "_".join(basename(data.filenames[0]).split("_")[:-1] + ["noise.png"])
        savefile = join(savedir, savename)
        fig.savefig(savefile)
    return (fig, ax)


def load_mass(noise, projectors, invert=True):
    scan_fname = get_filename(projectors).replace(".off", ".ljh")
    noise_fname = get_filename(noise).replace(".off", ".ljh")

    available_chans = set(mass.ljh_util.ljh_get_channels_both(noise_fname, scan_fname))
    pulse_files = mass.ljh_util.ljh_chan_names(scan_fname, available_chans)
    noise_files = mass.ljh_util.ljh_chan_names(noise_fname, available_chans)
    pulse_h5name = "_".join(basename(scan_fname).split("_")[:-1] + ["mass.hdf5"])
    noise_h5name = "_".join(basename(noise_fname).split("_")[:-1] + ["mass.hdf5"])
    data = mass.TESGroup(
        filenames=pulse_files,
        noise_filenames=noise_files,
        max_chans=1000,
        overwrite_hdf5_file=False,
        hdf5_filename=pulse_h5name,
        hdf5_noisefilename=noise_h5name,
    )
    data.set_all_chan_good()
    if invert:
        for ds in data:
            ds.invert_data = True
    return data


def prep_data(data):
    data.summarize_data()
    global_cuts = {
        "peak_time_ms": (
            0,
            None,
        ),  # we have lots of saturated pulses that get cut by this
        "rise_time_ms": (10e-3, 0.332288),
        "postpeak_deriv": (None, 50),
        "pretrigger_rms": (None, 70),
        "min_value": (-4000, None),
    }
    cuts = mass.controller.AnalysisControl()
    cuts.cuts_prm.update(global_cuts)
    data.apply_cuts(cuts, clear=True)
    for ds in data:
        ds.compute_average_pulse(ds.good())
        ds.p_rel_time_min = (ds.p_timestamp[:] - ds.p_timestamp[0]) / 60
    data.compute_noise_spectra()
