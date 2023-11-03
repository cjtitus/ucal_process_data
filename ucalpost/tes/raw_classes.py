import numpy as np
from os import path
import os
import mass
import mass.off
from mass.off import getOffFileListFromOneFile as getOffList
import h5py


class RawData:
    def __init__(self, off_filename, state, savefile, data=None):
        self.off_filename = off_filename
        self.attribute = "filtValueDC"
        self.state = state
        self.savefile = savefile
        self.load_data(data)
        self.load_ds()
        self._calibrated = False
        self._calmd = {}

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

    def getProcessMd(self):
        md = {"driftCorrected": self.driftCorrected,
              "calibration": self._calmd}
        return md

    @property
    def calibrated(self):
        try:
            return hasattr(self.ds, "energy") and self._calibrated
        except:
            return False

    @property
    def driftCorrected(self):
        try:
            return hasattr(self.ds, "filtValueDC")
        except:
            return False


class CalibrationInfo(RawData):
    def __init__(self, off_filename, state, savefile, savedir, line_names, **kwargs):
        super().__init__(off_filename, state, savefile, **kwargs)
        self.line_names = line_names
        self.cal_file = None
        self.savedir = savedir
        self.update_calibration()

    def update(self, state, savefile, savedir, line_names):
        super().update(state, savefile)
        self.savedir = savedir
        self.line_names = line_names
        self.update_calibration()

    def update_calibration(self, savedir=None):
        if savedir is None:
            savedir = self.savedir
        else:
            self.savedir = savedir
        if savedir is not None:
            savebase = "_".join(path.basename(self.off_filename).split('_')[:-1])
            savename = f"{savebase}_{self.state}_cal.hdf5"
            new_cal_file = path.join(savedir, savename)
            if new_cal_file != self.cal_file:
                self.cal_file = new_cal_file
                self._calibrated = False
        else:
            self.cal_file = None
            self._calibrated = False


