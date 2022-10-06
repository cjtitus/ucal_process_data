import mass
from mass.off import getOffFileListFromOneFile as getOffList
import os
from os import path
from ucalpost.tes.calibration import _calibrate
from ucalpost.databroker.run import get_tes_state, get_line_names, get_filename, get_save_directory
from ucalpost.tes.process_routines import get_analyzed_filename


class CatalogData:
    def __init__(self, cal_runs, data_runs, savenames=None):
        """
        off_filename : Full path to one .off file, from which the others will be found
        cal_states : states in the .off file that should be used for calibration
        data_states : states in the .off file that are data
        savenames: A dictionary mapping state names to savefile names
        """
        self.cal_runs = cal_runs
        self.data_runs = data_runs
        self.cal_states = [get_tes_state(cal) for cal in self.cal_runs]
        self.data_states = [get_tes_state(d) for d in self.data_runs]
        allruns = cal_runs + data_runs
        # For convenience later
        self.run_dict = {get_tes_state(r): r for r in allruns}
        self.off_filename = get_filename(allruns[0])
        if savenames is None:
            self.savenames = {}
            for run in allruns:
                savename = get_analyzed_filename(run)
                state = get_tes_state(run)
                self.savenames[state] = savename
        else:
            self.savenames = savenames
        self.data = mass.off.ChannelGroup(getOffList(self.off_filename)[:1000])
        self.ds = self.data.firstGoodChannel()

    @property
    def driftCorrected(self):
        try:
            return hasattr(self.ds, "filtValueDC")
        except:
            return False


def driftCorrect(catalog, states=None, redo=False):
    """
    catalog : A CatalogData instance
    states : Optional, a list of states to use for DC
    """
    if states is None:
        states = catalog.cal_states + catalog.data_states
    if not catalog.driftCorrected or redo:
        print("Drift Correcting")
        catalog.data.learnDriftCorrection(states=states)
    else:
        print("Drift Correction already done")


def makeStateCalibration(catalog, state, attr, rms_cutoff=0.2, save=True,
                         **kwargs):
    data = catalog.data
    run = catalog.run_dict[state]

    line_names = kwargs.get('line_names', get_line_names(run))
    savedir = get_save_directory(run)
    recipeName = f"energy_{state}"
    if 'savefile' not in kwargs:
        savebase = "_".join(path.basename(catalog.off_filename).split('_')[:-1])
        savefile = path.join(savedir, f"{savebase}_{state}_cal.hdf5")
    else:
        savefile = kwargs['savefile']
    _calibrate(data, state, line_names, fv=attr, rms_cutoff=rms_cutoff,
               recipeName=recipeName)
    if save:
        if not path.exists(path.dirname(savefile)):
            os.makedirs(path.dirname(savefile))
        data.calibrationSaveToHDF5Simple(savefile, recipeName=recipeName)


def loadStateCalibration(catalog, state, attr, **kwargs):
    run = catalog.run_dict[state]
    savedir = get_save_directory(run)
    if 'savefile' not in kwargs:
        savebase = "_".join(path.basename(catalog.off_filename).split('_')[:-1])
        savefile = path.join(savedir, f"{savebase}_{state}_cal.hdf5")
    else:
        savefile = kwargs['savefile']
    recipeName = f"energy_{state}"
    catalog.data.calibrationLoadFromHDF5Simple(savefile, recipeName)

def calibrate(catalog, states=None, attr=None, stateOptions={}, rms_cutoff=0.2,
              save=True, saveSummary=True):
    cal_md = {}
    if states is None:
        states = catalog.cal_states
    if attr is None:
        attr = 'filtValueDC' if catalog.driftCorrected else 'filtValue'
    cal_md['states'] = states
    cal_md['attr'] = attr
    for state in catalog.cal_states:
        opt = stateOptions.get(state, {})
        makeStateCalibration(catalog, state, attr, rms_cutoff, save, **opt)
    catalog.cal_md = cal_md


def summarize_calibration(catalog, state, savedir=None):
    if savedir is not None:
        if not os.path.exists(savedir):
            os.makedirs(savedir)


def get_cal_runs(noise_catalog):
    cal_list = []
    for run in noise_catalog._catalog.values():
        scantype = run.start.get('scantype', 'data')
        if scantype == 'calibration':
            cal_list.append(run)
    return cal_list


def get_data_runs(noise_catalog):
    data_list = []
    for run in noise_catalog._catalog.values():
        scantype = run.start.get('scantype', 'data')
        if scantype != 'calibration':
            data_list.append(run)
    return data_list


def get_savenames(noise_catalog):
    filenames = {}
    for run in noise_catalog._catalog.values():
        savename = get_analyzed_filename(run)
        state = get_tes_state(run)
        filenames[state] = savename
    return filenames


def get_catalog_data(noise_catalog):
    cal_runs = get_cal_runs(noise_catalog)
    data_runs = get_data_runs(noise_catalog)
    return CatalogData(cal_runs, data_runs)
