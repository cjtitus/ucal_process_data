import numpy as np
from os import path
import os
import mass
import mass.off
from mass.off import getOffFileListFromOneFile as getOffList
from .calibration import _calibrate
import h5py


class RawData:
    def __init__(self, off_filename, state, savefile, data=None):
        self.off_filename = off_filename
        self.calibrated = False
        self.attribute = "filtValueDC"
        self.state = state
        self.savefile = savefile
        self.load_data(data)
        self.load_ds()

    def load_data(self, data=None):
        if data is None:
            data = mass.off.ChannelGroup(getOffList(self.off_filename)[:1000],
                                         excludeStates=[])
        elif self.off_filename not in data.offFileNames:
            data = mass.off.ChannelGroup(getOffList(self.off_filename)[:1000],
                                         excludeStates=[])
        self.data = data

    def load_ds(self):
        self.ds = self.data.firstGoodChannel()

    def refresh(self):
        self.data.refreshFromFiles()

    def update(self, state, savefile):
        self.state = state
        self.savefile = savefile
        self.refresh()

    @property
    def driftCorrected(self):
        return hasattr(self.data, "filtValueDC")


class CalibrationInfo(RawData):
    def __init__(self, off_filename, state, savefile, savedir, line_names, **kwargs):
        self.line_names = line_names
        self.cal_file = None
        self.savedir = savedir
        super().__init__(off_filename, state, savefile, **kwargs)

    def update(self, state, savefile, savedir, line_names):
        super().update(state, savefile)
        self.savedir = savedir
        self.line_names = line_names

    def calibrate(self, savedir=None, redo=False):
        attr = "filtValueDC" if self.driftCorrected else "filtValue"
        if savedir is None:
            savedir = self.savedir

        if savedir is not None:
            savebase = "_".join(path.basename(self.off_filename).split('_')[:-1])
            savename = f"{savebase}_{self.state}_cal.hdf5"
            cal_file_name = path.join(savedir, savename)
        else:
            cal_file_name = None

        if cal_file_name is not None and path.exists(cal_file_name) and not redo:
            self.cal_file = cal_file_name
            self.calibrated = True
        else:
            _calibrate(self.data, self.state, self.line_names, fv=attr)
            if cal_file_name is not None:
                if not path.exists(path.dirname(cal_file_name)):
                    os.makedirs(path.dirname(cal_file_name))
                self.data.calibrationSaveToHDF5Simple(cal_file_name)
                self.cal_file = cal_file_name
            self.calibrated = True
