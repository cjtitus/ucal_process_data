from ..databroker.run import get_noise, get_projectors, get_filename
import mass
import os
from . import mass_addons
import matplotlib.pyplot as plt

plt.ion()


def plot_noise(data):
    plt.figure()
    data.plot_noise()
    ax = plt.gca()
    ax.get_legend().remove()


def load_mass(projectors, c, invert=True):
    noise = get_noise(projectors, c)
    scan_fname = get_filename(projectors).replace(".off", ".ljh")
    noise_fname = get_filename(noise).replace(".off", ".ljh")

    available_chans = set(mass.ljh_util.ljh_get_channels_both(noise_fname, scan_fname))
    pulse_files = mass.ljh_util.ljh_chan_names(scan_fname, available_chans)
    noise_files = mass.ljh_util.ljh_chan_names(noise_fname, available_chans)
    pulse_h5name = "_".join(
        os.path.basename(scan_fname).split("_")[:-1] + ["mass.hdf5"]
    )
    noise_h5name = "_".join(
        os.path.basename(noise_fname).split("_")[:-1] + ["mass.hdf5"]
    )
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
