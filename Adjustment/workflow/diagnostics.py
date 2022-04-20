"""
Diagnostics for bias-adjustment
-------------------------------

This module defines graphic diagnostics for bias-adjustment outputs. It works like so:

The `checkup_plots` function that will iterate over so-call `checkup_plot` functions for
each variable and each site.
Those `checkup_plot` are functions plotting one figure for one set of 1D timeseries.
Their signature must look like:

    ..code:

    @register_checkup_plot(period)
    def plotting_func(fig, obs, raw, sce, **kwargs):
        # plotting code
        return fig

Where `period` is one of "present", "future" or None. Inputs `obs`, `raw` and `sce` are 1D daily timeseries of the reference, un-adjusted and adjusted data. Their name is the variable name.

- If period is "present", all three inputs are defined on the reference period.
- If period is "future", `obs` is missing and `raw` and `sce` are on a future period.
- If period is ``None``, the inputs are not subsetted, so they might not be the same length (`obs` should be shorter).
"""
import itertools
from pathlib import Path
import warnings

from clisops.core import subset
from dask import compute
import numpy as np
import xarray as xr
import xclim as xc
from xclim.core.calendar import convert_calendar, get_calendar
from xclim.core.formatting import update_xclim_history
from xclim.core.units import convert_units_to, units2pint
from xclim.sdba import measures
from xclim.sdba.detrending import PolyDetrend

from cartopy import crs
import matplotlib as mpl
import matplotlib.pyplot as plt


from .config import parse_config
_checkup_plot_funcs = {}


def register_checkup_plot(period=None, variables=None, intype='timeseries', spatial='site'):
    assert period in [None, 'present', 'future']
    assert intype in ['timeseries', 'training']
    assert spatial in ['site', 'whole']
    assert isinstance(variables, (tuple, list)) or variables is None

    def _register_func(func):
        func.__dict__.update(
            period=period, variables=variables, intype=intype, spatial=spatial
        )
        _checkup_plot_funcs[func.__name__] = func
        return func
    return _register_func


def _get_coord_str(da):
    return ', '.join(f"{name}={crd.item():.2f}" for name, crd in da.coords.items() if crd.size == 1)


AGGREGATORS = {
    'mean': {
        'pr': xc.atmos.precip_accumulation,
        'tasmin': xc.atmos.tn_mean,
        'tasmax': xc.atmos.tx_mean
    },
    'max': {
        'pr': xc.atmos.max_1day_precipitation_amount,
        'tasmin': xc.atmos.tn_max,
        'tasmax': xc.atmos.tx_max
    },
    'min': {
        'pr': lambda pr, freq='YS': pr.resample(time=freq, keep_attrs=True).min(),
        'tasmin': xc.atmos.tn_min,
        'tasmax': xc.atmos.tx_min
    }
}


@register_checkup_plot('present')
@parse_config
def annual_cycle_monthly_present(
    ax, obs, raw, sce,
    *, labels=['obs', 'raw', 'scen'], colors='krb', alpha=0.1
):
    """Plot of the annual cycle (monthly agg), for the present period"""
    var = obs.name
    aggfunc = AGGREGATORS['mean'][var]
    obs = aggfunc(obs, freq='MS')
    raw = aggfunc(raw, freq='MS')
    sce = aggfunc(sce, freq='MS')

    for da, label, color in zip([obs, raw, sce], labels, colors):
        g = da.groupby('time.month')
        mn = g.min()
        mx = g.max()
        ax.fill_between(mn.month, mn, mx, color=color, alpha=alpha)
        g.mean().plot(ax=ax, color=color, linestyle='-', label=label)

    ax.set_title(f"Annual cycle (monthly), {var} at {_get_coord_str(obs)} - present")
    ax.legend()


@register_checkup_plot('present')
@parse_config
def annual_cycle_daily_present(
    ax, obs, raw, sce,
    *, labels=['obs', 'raw', 'scen'], colors='krb', alpha=0.1
):
    """Plot of the annual cycle (day-of-year agg), for the present period."""
    var = obs.name

    for da, label, color in zip([obs, raw, sce], labels, colors):
        g = da.groupby('time.dayofyear')
        mn = g.min()
        mx = g.max()
        ax.fill_between(mn.dayofyear, mn, mx, color=color, alpha=alpha)
        g.mean().plot(ax=ax, color=color, linestyle='-', label=label)

    ax.set_title(f"Annual cycle, {var} at {_get_coord_str(obs)} - present")
    ax.legend()


@register_checkup_plot('future')
@parse_config
def annual_cycle_monthly_future(
    ax, raw, sce,
    *, labels=['raw', 'scen'], colors='rb', alpha=0.1
):
    """Plot of the annual cycle (monthly agg), for the future period (no obs)"""
    var = raw.name
    aggfunc = AGGREGATORS['mean'][var]
    raw = aggfunc(raw, freq='MS')
    sce = aggfunc(sce, freq='MS')

    for da, label, color in zip([raw, sce], labels, colors):
        g = da.groupby('time.month')
        mn = g.min()
        mx = g.max()
        ax.fill_between(mn.month, mn, mx, color=color, alpha=alpha)
        g.mean().plot(ax=ax, color=color, linestyle='-', label=label)

    ax.set_title(f"Annual cycle (monthly), {var} at {_get_coord_str(raw)} - future")
    ax.legend()


@register_checkup_plot('future')
@parse_config
def annual_cycle_daily_future(
    ax, raw, sce,
    *, labels=['raw', 'scen'], colors='rb', alpha=0.1, quantiles=[0.1, 0.9], rollwin=15,
):
    """Plot of the annual cycle (day-of-year agg), for the future period (no obs)"""
    var = raw.name

    for da, label, color in zip([raw, sce], labels, colors):
        g = da.groupby('time.dayofyear')
        qs = g.quantile(quantiles).rolling(dayofyear=rollwin, center=True).mean()
        ax.fill_between(qs.dayofyear, qs.isel(quantile=0), qs.isel(quantile=1), color=color, alpha=alpha)
        g.mean().plot(ax=ax, color=color, linestyle='-', label=label)

    ax.set_title(f"Annual cycle for {var} at {_get_coord_str(raw)} - future")
    ax.legend()


@register_checkup_plot()
@parse_config
def annual_mean_timeseries(
    ax, obs, raw, sce,
    *, labels=['obs', 'raw', 'scen'], colors='krb', degree=None,
):
    """Plot the timeseries of the annual means."""
    var = obs.name
    aggfunc = AGGREGATORS['mean'][var]
    obs = aggfunc(obs, freq='YS')
    raw = aggfunc(raw, freq='YS')
    sce = aggfunc(sce, freq='YS')

    degree = degree or (4 if var.startswith('tas') else 1)

    for da, label, color in zip([obs, raw, sce], labels, colors):
        da.plot(ax=ax, color=color, linestyle='-', label=label)
        if da is not obs:
            PolyDetrend(degree=degree).fit(da).ds.trend.plot(ax=ax, color=color)

    ax.set_title(f"Annual mean timeseries. {var} at {_get_coord_str(obs)}")
    ax.legend()


@register_checkup_plot()
@parse_config
def annual_maximum_timeseries(
    ax, obs, raw, sce,
    *, labels=['obs', 'raw', 'scen'], colors='krb', degree=None,
):
    """Plot the timeseries of the annual maximums."""
    var = obs.name
    aggfunc = AGGREGATORS['max'][var]
    obs = aggfunc(obs, freq='YS')
    raw = aggfunc(raw, freq='YS')
    sce = aggfunc(sce, freq='YS')

    degree = degree or (4 if var.startswith('tas') else 1)

    for da, label, color in zip([obs, raw, sce], labels, colors):
        da.plot(ax=ax, color=color, linestyle='-', label=label)
        if da is not obs:
            PolyDetrend(degree=degree).fit(da).ds.trend.plot(ax=ax, color=color)

    ax.set_title(f"Annual maximum timeseries. {var} at {_get_coord_str(obs)}")
    ax.legend()


@register_checkup_plot()
@parse_config
def annual_minimum_timeseries(
    ax, obs, raw, sce,
    *, labels=['obs', 'raw', 'scen'], colors='krb', degree=None,
):
    """Plot the timeseries of the annual minimums."""
    var = obs.name
    aggfunc = AGGREGATORS['min'][var]
    obs = aggfunc(obs, freq='YS')
    raw = aggfunc(raw, freq='YS')
    sce = aggfunc(sce, freq='YS')

    degree = degree or (4 if var.startswith('tas') else 1)

    for da, label, color in zip([obs, raw, sce], labels, colors):
        da.plot(ax=ax, color=color, linestyle='-', label=label)
        PolyDetrend(degree=degree).fit(da).ds.trend.plot(ax=ax, label=label, color=color)

    ax.set_title(f"Annual minimum timeseries. {var} at {_get_coord_str(obs)}")
    ax.legend()


@register_checkup_plot(period='present', variables=['pr'])
@parse_config
def dry_day_spells_distribution_present(
    ax, obs, raw, sce,
    * , labels=['obs', 'raw', 'scen'], colors='krb', thresh='1 mm/d'
):
    """Plot the dry-day spell lengths distribution for the present period."""
    thresh = convert_units_to(thresh, raw)

    runs = {(label, color): xc.indices.run_length.rle(da < thresh).pipe(lambda r: r.where(r > 0, 0))
            for da, label, color in zip([obs, raw, sce], labels, colors)}
    vmax = max(int(ddsl.max()) for ddsl in runs.values())

    for (label, color), ddsl in runs.items():
        count, edges = np.histogram(ddsl, range=(1, vmax), bins=vmax)
        ax.plot((edges[1:] + edges[:-1]) / 2, count, label=label, color=color)

    ax.set_title(f"Dry day spell lengths at {_get_coord_str(obs)} - Present")
    ax.set_ylabel('Frequency')
    ax.set_xlabel('Dry day spell length [days]')
    ax.legend()


@register_checkup_plot(period='future', variables=['pr'])
@parse_config
def dry_day_spells_distribution_future(
    ax, raw, sce,
    * , labels=['raw', 'scen'], colors='rb', thresh='1 mm/d'
):
    """Plot the dry-day spell lengths distribution for the present period."""
    thresh = convert_units_to(thresh, raw)

    runs = {(label, color): xc.indices.run_length.rle(da < thresh).pipe(lambda r: r.where(r > 0, 0))
            for da, label, color in zip([raw, sce], labels, colors)}
    vmax = max(int(ddsl.max()) for ddsl in runs.values())

    for (label, color), ddsl in runs.items():
        count, edges = np.histogram(ddsl, range=(1, vmax), bins=vmax)
        ax.plot((edges[1:] + edges[:-1]) / 2, count, label=label, color=color)

    ax.set_title(f"Dry day spell lengths at {_get_coord_str(raw)} - Future")
    ax.set_ylabel('Frequency')
    ax.set_xlabel('Dry day spell length [days]')
    ax.legend()


@register_checkup_plot(period='present')
@parse_config
def quantile_quantile(
    ax, obs, raw, sce,
    *, labels=['obs', 'raw', 'scen'], colors='krb', nquantiles=50
):
    """Quantile-quantile plot."""
    qs = np.linspace(0, 1, num=nquantiles)
    qobs = obs.quantile(qs)
    qraw = raw.quantile(qs)
    qsce = sce.quantile(qs)

    for qda, label, color in zip([qraw, qsce], labels[1:], colors[1:]):
        ax.plot(qobs, qda, label=label, marker='o', color=color)

    ax.set_xlabel('obs quantiles')
    ax.set_ylabel('sim quantiles')
    ax.plot(qobs, qobs, color=color[0], linestyle=':')
    ax.legend()

    if raw.name in ['pr']:
        ax.set_xscale('log')
        ax.set_yscale('log')
    ax.set_title(f"Quantile-quantile for {raw.name} at {_get_coord_str(raw)}")


@register_checkup_plot(period='present')
@parse_config
def empirical_pdf_present(
    ax, obs, raw, sce,
    *, labels=['obs', 'raw', 'scen'], colors='krb', bins='doane'
):
    """Plot the distributions over the reference period."""
    vmin = min(obs.min(), raw.min(), sce.min()).item()
    vmax = min(obs.max(), raw.max(), sce.max()).item()

    for da, label, color in zip([obs, raw, sce], labels, colors):
        count, edges = np.histogram(da, range=(vmin, vmax), bins=bins, density=True)
        ax.plot((edges[1:] + edges[:-1]) / 2, count, label=label, color=color)

    if raw.name in ['pr']:
        ax.set_yscale('log')
    ax.legend()
    ax.set_title(f"Empirical PDF for {raw.name} at {_get_coord_str(raw)} - Present")


@register_checkup_plot(period='future')
@parse_config
def empirical_pdf_future(
    ax, raw, sce,
    *, labels=['raw', 'scen'], colors='rb', bins='doane'
):
    """Plot the distributions over the reference period."""
    vmin = min(raw.min(), sce.min()).item()
    vmax = min(raw.max(), sce.max()).item()

    for da, label, color in zip([raw, sce], labels, colors):
        count, edges = np.histogram(da, range=(vmin, vmax), bins=bins, density=True)
        ax.plot((edges[1:] + edges[:-1]) / 2, count, label=label, color=color)

    if raw.name in ['pr']:
        ax.set_yscale('log')
    ax.legend()
    ax.set_title(f"Empirical PDF for {raw.name} at {_get_coord_str(raw)} - Future")


@register_checkup_plot(intype='training')
@parse_config
def adjustment_factors(
    ax, trds,
    *, var, cmap='viridis'
):
    norm = mpl.colors.LogNorm() if var in ['pr', 'dtr'] else None
    trds.af.plot(robust=True, ax=ax, norm=norm, cmap=cmap)
    ax.set_title(f'Adjustment factors of {var} at {_get_coord_str(trds)}.')


@register_checkup_plot(intype='training', spatial='whole', variables=['pr'])
@parse_config
def added_wet_days(
    ax, trds,
    *, var, cmap='viridis'
):
    cmap = mpl.cm.get_cmap(cmap).copy()
    cmap.set_under('lightgray')
    group = set(trds.dP0.dims) - {'lon', 'lat'}
    trds.dP0.mean(group).plot(ax=ax, vmin=0, cmap=cmap, robust=True)
    ax.set_title(f'Proportion of dry days corrected (mean over grouping)')


@register_checkup_plot(intype='training', spatial='whole', variables=['tasmin'])
@parse_config
def inverted_temperatures(
    ax, trds,
    *, var, cmap='viridis'
):
    trds.inverted_temps.plot(vmin=0, robust=True, cmap=cmap, ax=ax)
    ax.set_title(f'Number of inverted temperatures.')


@register_checkup_plot(spatial='whole')
@parse_config
def total_difference(
    ax, obs, raw, sce,
    *, cmap='RdBu_r'
):
    (sce - raw).sum('time').plot(robust=True, cmap=cmap, ax=ax)
    ax.set_title(f'Total difference (sum of scen minus raw), for {obs.name}.')


@parse_config
def checkup_plots(
    obs, raw, sce,
    training=None,
    points=None,
    *,
    checkups='all',
    num_points={'lat': 3, 'lon': 2},
    present=('1981', '2010'),
    future=('2071', '2100'),
    labels=['obs', 'raw', 'sce'],
    colors='krb',
    figsize=(6, 4),
    output_format='png',
    output_dir='.',
    output_points=None,
    verbose=False
):
    """Call checkup plotting functions iteratively.

    Parameters
    ----------
    obs: xr.Dataset
      The reference dataset for the bias-adjustment. Same grid as `raw` and `sce`.
    raw : xr.Dataset
      The simulated, non-adjusted data. Same grid as `obs` and `sce`.
    sce : xr.Dataset
      The simulated and adjusted data. Same grid as `obs` and `raw`.
    training : dict, optional
      Mapping from variable name to trained data of the adjustment.
    points : xr.Dataset, optional
      Sites to select for `timeseries` if the latter is not given. should have one variable per spatial dimension, all along dimension 'site'.
      If not given, random points are selected using :py:func:`random_site_selection` and ``num_points``.
    checkups : sequence of strings or "all"
      Which plotting function to call. If "all", all the registered functions are called.
    variables: sequence of strings
      Which variables to plot.
    num_points : int or sequence of ints or dict
      How many points to choose, see arg `num` of ``random_site_selection``.
    present : 2-tuple
      The start and end of the reference period.
    future : 2-tuple
      The start and end of the future period.
    labels : 3-tuple of strings
      The name to give to each dataset in the plots.
    colors : 3-tuple of colors
      The colors to associate with each dataset in the plots. Any matplotlib-understandable object is fine.
    figsize : 2-tuple of numbers
      The size of the figure. See arg `figsize` of `matplotlib.pyplot.figure`.
    output_format : string
      The file extension for output figures.
    output_dir : Pathlike
      A path to the folder where to save all figures.
    output_points : string, optional
      A filename where to save the selected points are saved to a netCDF in the output_dir.
      If None (default), they are not saved.
    verbose : bool
      If True, something is printed for each step of the work.
    """
    output_dir = Path(output_dir)

    scen_vars = list(obs.data_vars.keys())
    train_vars = list(training.keys())
    if points is None:
        if verbose:
            print('Creating mask and selecting points')

        # Select middle point to avoid NaN slices on the sides.
        mask = obs[scen_vars[0]].isel(time=int(obs.time.size / 2), drop=True).notnull().load()
        points = random_site_selection(mask, num_points)

    if output_points is not None:
        points.to_netcdf(output_dir / output_points)

    # The complex part : store and select the correct data to send...
    # Put everything in a dict for convenient access
    if verbose:
        print('Organizing data.')
    data = {}
    # To harmonize calendars
    ref_cal = get_calendar(sce.time)
    for var in scen_vars:
        # To harmonize units
        ref_units = units2pint(sce[var])
        data[('timeseries', var, None, 'whole')] = [
            convert_calendar(convert_units_to(ds[var], ref_units), ref_cal)
            for ds in [obs, raw, sce]
        ]

        data[('timeseries', var, 'present', 'whole')] = [
            ds.sel(time=slice(*present))
            for ds in data[('timeseries', var, None, 'whole')]
        ]

        data[('timeseries', var, 'future', 'whole')] = [
            ds.sel(time=slice(*future))
            for ds in data[('timeseries', var, None, 'whole')][1:]
        ]

    for var, ds in training.items():
        data[('training', var, None, 'whole')] = (ds,)

    # And the "site" subsets, these are loaded.
    if verbose:
        print('Loading site data.')
    subsetted = {k: [subset.subset_gridpoint(ds, lon=points.lon, lat=points.lat) for ds in data[k]] for k in data.keys()}

    subsetted = compute(subsetted)[0]

    for k in list(data.keys()):
        data[(k[0], k[1], k[2], 'site')] = subsetted[k]

    # All spatial='site'+intype='data' checkup plot function MUST accept these.
    kwargs = dict(labels=labels, colors=colors)
    fut_kwargs = dict(labels=labels[1:], colors=colors[1:])

    if checkups == 'all':
        checkups = _checkup_plot_funcs.keys()

    plotting_calls = []
    for var in (scen_vars + train_vars):
        for checkup, func in zip(checkups, map(_checkup_plot_funcs.get, checkups)):
            # Check if variable is compatible with this plot
            if var not in (func.variables or [var]):
                continue

            # Get the correct datasets
            args = data.get((func.intype, var, func.period, func.spatial))
            if args is None:
                continue
            if func.spatial == 'site' and func.intype == 'timeseries':
                kws = fut_kwargs if func.period == 'future' else kwargs
            elif func.intype == 'training':
                kws = {'var': var}
            else:
                kws = {}

            name = f"{var}_{checkup}"
            if func.spatial == 'site':
                for i in range(points.site.size):
                    plotting_calls.append(
                        (name + f"_site{i}", func, [arg.isel(site=i) for arg in args], kws)
                    )
            else:
                plotting_calls.append((name, func, args, kws))

    for name, func, args, kwargs in plotting_calls:
        fig, ax = plt.subplots(figsize=figsize)

        if verbose:
            print(f'Plotting {name} ...', end=' ')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            func(ax, *args, **kwargs)

        fig.tight_layout()
        fig.savefig(output_dir / f"{name}.{output_format}")
        plt.close(fig)
        if verbose:
            print('done.')


def random_site_selection(mask, num=4):
    """Select points (semi-)randomly from a grid.

    Parameters
    ----------
    mask : xr.DataArray
      Array with coords, from which points are taken.
      Only points where mask is True are considered.
    num : int or sequence of integers or dict
      The number of points to consider. If a sequence or a dict, then the grid is divided in
      as many zones and one point is selected in each. When a dict is given, it is a mapping from
      dimension name to number of subzones along that dimension.

    Return
    ------
    xarray.Dataset
      One variable per coord of mask, along dimension "site".
    """

    if isinstance(num, (int, float)):
        indexes = np.where(mask.values)

        if len(indexes[0]) == 0:
            raise ValueError('There are no True values in the mask.')

        randindexes = np.random.default_rng().integers(
            0, high=len(indexes[0]), size=(num,))

        return xr.Dataset(
            data_vars={
                name: mask[name].isel(
                    {name: xr.DataArray(index[randindexes], dims=('site',), name='site')})
                for name, index in zip(mask.dims, indexes)
            }
        )
    # else : divide in zones

    if isinstance(num, dict):
        num = [num[dim] for dim in mask.dims]

    slices = [
        [slice(i * (L // N), (i + 1) * (L // N)) for i in range(N)]
        for L, N in zip(mask.shape, num)
    ]

    points = []
    for zone_slices in itertools.product(*slices):
        zone = mask.isel({dim: slic for dim, slic in zip(mask.dims, zone_slices)})
        points.append(random_site_selection(zone, 1))

    return xr.concat(points, 'site')


def fig_compare_and_diff(sim, scen, op='difference', title=""):
    cmbias = mpl.cm.BrBG.copy()
    cmbias.set_bad('gray')
    cmimpr = mpl.cm.RdBu.copy()
    cmimpr.set_bad('gray')

    fig = plt.figure(figsize=(15, 5))
    gs = mpl.gridspec.GridSpec(6, 2, hspace=2)
    axsm = plt.subplot(gs[3:, 0], projection=crs.PlateCarree())
    axsc = plt.subplot(gs[3:, 1], projection=crs.PlateCarree())
    axim = plt.subplot(gs[:3, 1], projection=crs.PlateCarree())

    vmin = min(
        min(sim.quantile(0.05), scen.quantile(0.05)),
        -max(sim.quantile(0.95), scen.quantile(0.95))
    )

    ps = sim.plot(
        ax=axsm, vmin=vmin, vmax=-vmin,
        cmap=cmbias, add_colorbar=False, transform=crs.PlateCarree()
    )
    scen.plot(
        ax=axsc, vmin=vmin, vmax=-vmin,
        cmap=cmbias, add_colorbar=False, transform=crs.PlateCarree()
    )
    if op == 'distance':
        diff = abs(sim - scen)
    elif op == 'improvement':
        diff = abs(sim) - abs(scen)
    else:  # op == 'diff':
        diff = sim - scen
    pc = diff.plot(
        ax=axim, robust=True, cmap=cmimpr, center=0,
        add_colorbar=False, transform=crs.PlateCarree()
    )

    fig.suptitle(title, fontsize='x-large')
    axsm.set_title(sim.name.replace('_', ' ').capitalize())
    axsc.set_title(scen.name.replace('_', ' ').capitalize())
    axim.set_title(op.capitalize())
    fig.tight_layout()
    fig.colorbar(ps, cax=plt.subplot(gs[1, 0]), shrink=0.5, label=sim.attrs.get('long_name', sim.name), orientation='horizontal')
    fig.colorbar(pc, cax=plt.subplot(gs[0, 0]), shrink=0.5, label=op.capitalize(), orientation='horizontal')
    return fig


def fig_bias_compare_and_diff(ref, sim, scen, measure='bias', **kwargs):
    bsim = getattr(measures, measure)(sim, ref).rename(f'sim_{measure}')
    bscen = getattr(measures, measure)(scen, ref).rename(f'scen_{measure}')
    kwargs.setdefault('op', 'improvement')
    kwargs.setdefault('title', f'Comparing {measure} of {ref.name}, sim vs scen')
    return fig_compare_and_diff(bsim, bscen, **kwargs)


@measures.check_same_units_and_convert
@update_xclim_history
def rmse(sim: xr.DataArray, ref: xr.DataArray) -> xr.DataArray:
    """Root mean square error.

    The root mean square error on the time dimension between the simulation and the reference.

    Parameters
    ----------
    sim : xr.DataArray
      Data from the simulation (a time-series for each grid-point)
    ref : xr.DataArray
      Data from the reference (observations) (a time-series for each grid-point)

    Returns
    -------
    xr.DataArray,
      Root mean square error between the simulation and the reference
    """
    def _rmse(sim, ref):
        return np.mean(np.sqrt((sim - ref)**2), axis=-1)

    out = xr.apply_ufunc(
        _rmse,
        sim,
        ref,
        input_core_dims=[['time'], ['time']],
        dask='parallelized',
    )
    out.attrs.update(sim.attrs)
    out.attrs["long_name"] = "Root mean square"
    return out
