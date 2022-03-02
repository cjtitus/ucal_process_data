import numpy as np
from os import path
import mass
import mass.off
from mass.off import getOffFileListFromOneFile as getOffList
from .analysis_routines import _calibrate
import h5py

def data_calibrationLoadFromHDF5Simple(self, h5name):
    print(f"loading calibration from {h5name}")
    with h5py.File(h5name,"r") as h5:
        for channum_str in h5.keys():
            cal = mass.calibration.EnergyCalibration.load_from_hdf5(h5, channum_str)
            channum = int(channum_str)
            if channum in self:
                ds = self[channum]
                ds.recipes.add("energy", cal, ["filtValue"], overwrite=True)
    # set other channels bad
    for ds in self.values():
        if not "energy" in ds.recipes.keys():
            ds.markBad("no loaded calibration")
mass.off.ChannelGroup.calibrationLoadFromHDF5Simple = data_calibrationLoadFromHDF5Simple

def data_calibrationSaveToHDF5Simple(self, h5name):
    print(f"writing calibration to {h5name}")
    with h5py.File(h5name,"w") as h5:
        for ds in self.values():
            cal = cal=ds.recipes["energy"].f
            cal.save_to_hdf5(h5, f"{ds.channum}")

mass.off.ChannelGroup.calibrationSaveToHDF5Simple = data_calibrationSaveToHDF5Simple

class RawData:
    def __init__(self, off_filename):
        self.off_filename = off_filename
        self.calibrated = False
        self.driftCorrected = False
        self.attribute = "filtValueDC"
        self.load_data()
        self.load_ds()

    def load_data(self):
        data = mass.off.ChannelGroup(getOffList(self.off_filename)[:1000],
                                     excludeStates=[])
        self.data = data

    def load_ds(self):
        self.ds = self.data.firstGoodChannel()

    def refresh(self):
        self.data.refreshFromFiles()

class CalibrationInfo(RawData):
    def __init__(self, off_filename, savedir, cal_state, line_names):
        self.cal_state = cal_state
        self.line_names = line_names
        self.cal_file = None
        self.savedir=savedir
        super().__init__(off_filename)
            
    def calibrate(self, savedir=None, redo=False):
        attr = "filtValueDC" if self.driftCorrected else "filtValue"
        if savedir is None:
            savedir = self.savedir
                
        if savedir is not None:
            savebase = "_".join(path.basename(self.off_filename).split('_')[:-1]) + "cal.hdf5"
            savename = path.join(savedir, savebase)
            cal_file_name = savename
        else:
            cal_file_name = None
            
        if cal_file_name is not None and path.exists(cal_file_name) and not redo:
            self.cal_file = cal_file_name
            self.calibrated = True
        else:
            _calibrate(self.data, self.ds, self.cal_state, self.line_names, fv=attr)
            if cal_file_name is not None:
                self.data.calibrationSaveToHDF5Simple(savename)
                self.cal_file = cal_file_name
            self.calibrated = True

