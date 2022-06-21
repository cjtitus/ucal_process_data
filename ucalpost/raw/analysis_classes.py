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
    def calibrated(self):
        try:
            return hasattr(self.ds, "energy")
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
        self.line_names = line_names
        self.cal_file = None
        self.savedir = savedir
        super().__init__(off_filename, state, savefile, **kwargs)

    def update(self, state, savefile, savedir, line_names):
        super().update(state, savefile)
        self.savedir = savedir
        self.line_names = line_names
