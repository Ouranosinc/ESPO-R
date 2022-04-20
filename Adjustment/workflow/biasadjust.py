"""
Bias-adjustment
---------------
This module defines bias-adjustment function and other things surrounding it.
"""
from typing import Optional, Union

import xarray as xr
import xclim as xc
from xclim import sdba

from .config import parse_config


@parse_config
def train_qm(
    ref: xr.DataArray,
    hist: xr.DataArray,
    *,
    method: str = "DetrendedQuantileMapping",
    group: Union[sdba.Grouper, str, dict] = "time.dayofyear",
    kind: "+",
    train: dict = {'nquantiles': 15},
    adapt_freq: Optional[dict] = None,
    jitter_under: Optional[dict] = None,
    jitter_over: Optional[dict] = None
) -> xr.Dataset:
    """
    Train a bias-adjustment.

    Parameters
    ----------
    ref : xr.DataArray
      The target timeseries, on the reference period.
    hist : xr.DataArray
      The timeseries to adjust, on the reference period.
    method : str
      Name of the `sdba.TrainAdjust` method of xclim.
    group : str or sdba.Grouper
      Grouping information
    kind : {'+', '*'}
      How to perform the adjustment (and preprocessing)
    train : dict
      Dict of arguments to pass to the `.train` of the adjustment object.
    adapt_freq: dict, optional
      If given, a dictionary of args to pass to the frequency adaptation function.
    jitter_under: dict, optional
      If given, a dictionary of args to pass to `jitter_under_thresh`.
    jitter_over: dict, optional
      If given, a dictionary of args to pass to `jitter_over_thresh`.

    Returns
    -------
    xr.Dataset
      Trained algorithm's data.
    """
    if isinstance(group, dict):
        # So we can specifiy window and add_dims in yaml.
        group = sdba.Grouper.from_kwargs(**group)['group']
    elif isinstance(group, str):
        group = sdba.Grouper(group)

    if jitter_over is not None:
        ref = sdba.processing.jitter_over_thresh(ref, **jitter_over)
        hist = sdba.processing.jitter_over_thresh(hist, **jitter_over)

    if jitter_under is not None:
        ref = sdba.processing.jitter_under_thresh(ref, **jitter_under)
        hist = sdba.processing.jitter_under_thresh(hist, **jitter_under)

    if adapt_freq is not None:
        adapt_freq.setdefault('group', group)
        hist, pth, dP0 = sdba.processing.adapt_freq(ref, hist, **adapt_freq)

    ADJ = getattr(sdba.adjustment, method).train(ref, hist, kind=kind, group=group, **train)

    if adapt_freq is not None:
        ds = ADJ.ds.assign(pth=pth, dP0=dP0)
    else:
        ds = ADJ.ds
    return ds


@parse_config
def adjust_qm(
    adj_ds: xr.Dataset,
    sim : xr.DataArray,
    **adj_args
):
    """
    Adjust a simulation.

    Parameters
    ----------
    adj_ds : xr.Dataset
      A trained algorithm's dataset, as returned by `train_qm`.
    sim : xr.DataArray
      Simulated timeseries, projected period.
    adj_args: dict
      Other arguments to pass to the adjust method.

    Returns
    -------
    xr.DataArray
      scen, the bias-adjusted timeseries.
    """
    ADJ = sdba.adjustment.TrainAdjust.from_dataset(adj_ds)

    if 'detrend' in adj_args and isinstance(adj_args['detrend'], dict):
        name, kwargs = list(adj_args['detrend'].items())[0]
        kwargs = kwargs or {}
        kwargs.setdefault('group', ADJ.group)
        kwargs.setdefault('kind', ADJ.kind)
        adj_args['detrend'] = getattr(sdba.detrending, name)(**kwargs)

    with xc.set_options(sdba_encode_cf=True, sdba_extra_output=False):
        out = ADJ.adjust(sim, **adj_args)
    return out.rename(sim.name)


def invert_unphysical_temperatures(tasmin: xr.DataArray, tasmax: xr.Dataset):
    """
    Invert tasmin and tasmax points where tasmax <  tasmin.

    Returns
    -------
    tasmin : xr.DataArray
      New tasmin.
    tasmax : xr.DataArray
      New tasmax
    switched : xr.DataArray
      A scalar DataArray with the number of switched data points for each spatial point.
    """
    switch = (tasmax < tasmin) & tasmax.notnull() & tasmin.notnull()
    tn = xr.where(switch, tasmax, tasmin)
    tx = xr.where(switch, tasmin, tasmax)

    switch = switch.sum('time').rename('inverted_temps')
    switch.attrs.update(long_name='Number of inverted temperatures')

    tn.attrs.update(tasmin.attrs)
    tx.attrs.update(tasmax.attrs)
    return tn, tx, switch


def add_preprocessing_attr(scen, train_kwargs):
    fake_ref = xr.DataArray(name='ref')
    fake_hist = xr.DataArray(name='hist')

    preproc = []
    if 'jitter_under' in train_kwargs:
        preproc.append(xc.core.formatting.gen_call_string("jitter_under_thresh", fake_ref, fake_hist, train_kwargs['jitter_under']))
    if 'jitter_over' in train_kwargs:
        preproc.append(xc.core.formatting.gen_call_string("jitter_over_thresh", fake_ref, fake_hist, train_kwargs['jitter_over']))
    if 'adapt_freq' in train_kwargs:
        preproc.append(xc.core.formatting.gen_call_string("adapt_freq", fake_ref, fake_hist, train_kwargs['adapt_freq']))

    if preproc:
        scen.attrs['bias_adjustment'] += ", ref and hist were prepared with " + " and ".join(preproc)


@xc.units.declare_units(dtr="[temperature]", tasmax="[temperature]")
def _tasmin_from_dtr(dtr: xr.DataArray, tasmax: xr.DataArray):
    """Tasmin computed from DTR and tasmax.

    Tasmin as dtr subtracted from tasmax.

    Parameters
    ----------
    dtr: xr.DataArray
      Daily temperature range
    tasmax: xr.DataArray
      Daily maximal temperature.

    Returns
    -------
    xr.DataArray, [same as tasmax]
         Daily minium temperature
    """
    dtr = xc.units.convert_units_to(dtr, tasmax)
    tasmin = tasmax - dtr
    tasmin.attrs['units'] = tasmax.units
    return tasmin


tasmin_from_dtr = xc.core.indicator.Indicator(
    src_freq="D",
    identifier="tn_from_dtr",
    compute=_tasmin_from_dtr,
    standard_name="air_temperature",
    description="Daily minimal temperature as computed from tasmax and dtr.",
    units='K',
    cell_methods="time: minimum within days",
    var_name='tasmin',
    module='workflow',
    realm='atmos'
)


@xc.units.declare_units(tasmin="[temperature]", tasmax="[temperature]")
def _dtr(tasmin: xr.DataArray, tasmax: xr.DataArray):
    """DTR computed from tasmin and tasmax.

    Dtr as tasmin subtracted from tasmax.

    Parameters
    ----------
    tasmin: xr.DataArray
      Daily minimal temperature.
    tasmax: xr.DataArray
      Daily maximal temperature.

    Returns
    -------
    xr.DataArray, K
         Daily temperature range
    """
    tasmin = xc.units.convert_units_to(tasmin, 'K')
    tasmax = xc.units.convert_units_to(tasmax, 'K')
    dtr = tasmax - tasmin
    dtr.attrs['units'] = "K"
    return dtr


dtr = xc.core.indicator.Indicator(
    src_freq="D",
    identifier="dtr",
    compute=_dtr,
    description="Daily temperature range.",
    units='K',
    cell_methods="time: range within days",
    module='workflow',
    realm='atmos'
)
