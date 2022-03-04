import numpy as np
import os
import json
from .run_info import get_save_directory, get_tes_state, get_logname

class ProcessedData:
    def __init__(self, timestamps, energies, channels):
        self.timestamps = timestamps
        self.energies = energies
        self.channels = channels
        
    def index_between_times(self, start, stop):
        idx1 = np.searchsorted(self.timestamps, start)
        idx2 = np.searchsorted(self.timestamps, stop)
        return idx1, idx2
    
    def select_between_times(self, start, stop):
        idx1, idx2 = self.index_between_times(start, stop)
        return self.energies[idx1:idx2]
    
    def sum_roi_between_times(self, start, stop, llim, ulim):
        energies = self.select_between_times(start, stop)
        return np.sum((energies < ulim) & (energies > llim))
        
    def histogram_between_times(self, start, stop, e_bins):
        energies = self.select_between_times(start, stop)
        ehist, _ = np.histogram(energies, e_bins)
        return ehist
        
class LogData:
    def __init__(self, start_times, stop_times, motor_name, motor_vals):
        self.start_times = start_times
        self.stop_times = stop_times
        self.motor_name = motor_name
        self.motor_vals = motor_vals
    
class ScanData:
    def __init__(self, data, log):
        self.data = data
        self.log = log
        
    def getScan1d(self, llim, ulim):
        counts = np.zeros_like(self.log.start_times)
        for n in range(len(counts)):
            counts[n] = self.data.sum_roi_between_times(self.log.start_times[n], self.log.stop_times[n], llim, ulim)
        return counts, self.log.motor_vals
    
    def getScan2d(self, llim, ulim, eres=0.3):
        mono_list = self.log.motor_vals
        n_e_pts = int((ulim - llim)//eres)
        e_bins = np.linspace(llim, ulim, n_e_pts)
        e_centers = (e_bins[1:] + e_bins[:-1])/2

        mono_grid, energy_grid = np.meshgrid(mono_list, e_centers)
        counts = np.zeros_like(mono_grid)
        for n in range(len(mono_list)):
            counts[:, n] = self.data.histogram_between_times(self.log.start_times[n],
                                                             self.log.stop_times[n], e_bins)
        return counts, mono_grid, energy_grid

    def getEmission(self, llim, ulim, eres=0.3, strictTimebins=False):
        n_e_pts = int((ulim - llim)//eres)
        e_bins = np.linspace(llim, ulim, n_e_pts)
        e_centers = (e_bins[1:] + e_bins[:-1])/2
        emission = self.data.histogram_between_times(self.log.start_times[0], self.log.stop_times[-1], e_bins)
        return emission, e_centers
        
def data_from_file(filename):
    data = np.load(filename)
    timestamps = data['timestamps']*1e-9 # data is stored as nanoseconds
    energies = data['energies']
    channels = data['channels']
    return ProcessedData(timestamps, energies, channels)


def log_from_json(logname):
    with open(logname, 'r') as f:
        log = json.load(f)
    start_time = log['epoch_time_start_s']
    stop_time = log['epoch_time_end_s']
    motor_name = log['var_name']
    motor_vals = log['var_values']
    return LogData(start_time, stop_time, motor_name, motor_vals)

def scandata_from_run(run):
    filename = get_analyzed_filename(run)
    logname = get_logname(run)
    data = data_from_file(filename)
    log = log_from_json(logname)
    return ScanData(data, log)

def get_analyzed_filename(run):
    data_directory = get_save_directory(run)
    state = get_tes_state(run)
    filename = os.path.join(data_directory, f"tes_{state}.npz")
    return filename
    
def process_default(run):
    roi_keys = run.primary.descriptors[0]['object_keys']['tes']
    desc = run.primary.descriptors[0]['data_keys']
    rois = {roi: (desc[roi]['llim'], desc[roi]['ulim']) for roi in roi_keys}
    filename = get_analyzed_filename(run)
    logname = get_logname(run)
    if exists(filename):
        data = np.load(filename)
        
def get_analyzed_filename(run):
    data_directory = get_save_directory(run)
    state = get_tes_state(run)
    filename = os.path.join(data_directory, f"tes_{state}.npz")
    return filename
