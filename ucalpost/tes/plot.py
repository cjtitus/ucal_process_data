import matplotlib.pyplot as plt
import numpy as np
from xastools.utils import tailNorm, areaNorm, ppNorm
from xastools.rixstools import maskPFYRegion, maskRegion, makeTrap, makeBox
from ucalpost.tes.process_classes import (
    scandata_from_run,
    is_run_processed,
)


def getScan1d(catalog, llim, ulim, removeElastic=0, coadd=True):
    """
    Make sure everything in catalog has the same x-axis and length

    If removeElastic > 0, scans will be co-added
    """
    x = 0
    counts = []
    if removeElastic > 0:
        x, y, counts = getScan2d(
            catalog, llim, ulim, removeElastic=removeElastic, coadd=coadd
        )
        counts = np.sum(counts, axis=-2)
        x = x[0, :]
    else:
        for run in catalog.values():
            if is_run_processed(run):
                sd = scandata_from_run(run, logtype="run")
                y, x = sd.getScan1d(llim, ulim)
                counts.append(y)
        if coadd:
            counts = np.sum(counts, axis=0)
    return x, counts


def plotScan1d(catalog, llim, ulim, removeElastic=0, normType=None):
    fig = plt.figure()
    ax = fig.add_subplot()

    x, counts = getScan1d(catalog, llim, ulim, removeElastic, coadd=True)
    if normType == "tail":
        counts = tailNorm(counts)
    elif normType == "area":
        counts = areaNorm(counts, x)
    elif normType == "ppNorm":
        counts = ppNorm(counts)
    ax.plot(x, counts)
    return x, counts


def getScan2d(catalog, llim, ulim, eres=0.3, removeElastic=0, coadd=True):
    x = 0
    y = 0
    counts = []
    for run in catalog.values():
        if is_run_processed(run):
            sd = scandata_from_run(run, logtype="run")
            z, x, y = sd.getScan2d(llim, ulim, eres=eres)
            if removeElastic > 0:
                counts = maskElastic(x, y, counts, removeElastic)
            counts.append(z)
    if coadd:
        counts = np.sum(counts, axis=0)
    return x, y, counts


def plotScan2d(catalog, llim, ulim, removeElastic=0):
    x, y, counts = getScan2d(
        catalog, llim, ulim, removeElastic=removeElastic, coadd=True
    )
    plt.contourf(x, y, counts, 50)


def maskElastic(x, y, z, width):
    llim = min(np.min(x), np.min(y))
    ulim = max(np.max(x), np.max(y))
    region = makeTrap(llim, llim, ulim, ulim, width)
    zn = maskRegion({"x": x, "y": y, "z": z}, region)
    return zn


def get_slice(x, y, z, llim, ulim):
    xidx = (x[0, :] < ulim) & (x[0, :] > llim)
    zslice = np.sum(z[:, xidx], axis=1)
    return y[:, 0] - (llim + ulim) / 2.0, zslice
