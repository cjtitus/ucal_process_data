import mass
import mass.off
from mass.off import getOffFileListFromOneFile as getOffList


class RawData:
    def __init__(self, off_filename, savedir=None):
        self.off_filename = off_filename
        self.calibrated = False
        self.attribute = "filtValueDC"
        self.savedir = savedir
        self.load_data()
        self.load_ds()

    def load_data(self):
        data = mass.off.ChannelGroup(getOffList(self.off_filename)[:1000],
                                     excludeStates=[])
        self.data = data

    def load_ds(self):
        self.ds = self.data.firstGoodChannel()


class CalibrationInfo:
    def __init__(self, cal_state, line_names):
        self.cal_state = cal_state
        self.line_names = line_names
