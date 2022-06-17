from __future__ import annotations
import xarray as xr
import xclim as xc


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
