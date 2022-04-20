# IO utilities for the workflow
from clisops.core import subset
import dask
import h5py
import json
import logging
import numpy as np
from pathlib import Path
import shutil as sh
import xarray as xr
from xclim.core import calendar as xcal
import zarr

from .config import parse_config
logger = logging.getLogger(__name__)


def get_engine(
        file: str
) -> str:
    """
    Uses a h5py functionality to determine if a NetCDF file is compatible with h5netcdf

    Parameters
    ----------
    file: str
      Path to the file.

    Returns
    -------
    str
      Engine to use with xarray
    """
    # find the ideal engine for xr.open_mfdataset
    if Path(file).suffix == '.zarr':
        engine = 'zarr'
    elif h5py.is_hdf5(file):
        engine = "h5netcdf"
    else:
        engine = "netcdf4"

    return engine


def save_mfdataset_lazy(datasets, paths, encodings):
    writers, stores = zip(
        *[
            xr.backends.api.to_netcdf(
                ds, path,
                encoding=encoding,
                compute=False, multifile=True
            )
            for ds, path, encoding in zip(datasets, paths, encodings)
        ]
    )

    try:
        writes = [w.sync(compute=False) for w in writers]
    finally:
        pass

    return dask.delayed(
        [dask.delayed(xr.backends.api._finalize_store)(w, s) for w, s in zip(writes, stores)]
    )


def to_zarr_skip_existing(ds, path, mode='f', encoding=None, itervar=False, **kwargs):
    """According to mode, removes variables that we dont want to re-compute in ds.

    Parameters
    ----------
    ds: xr.Dataset
      The dataset we want to write.
    path: pathlike
      The zarr store path.
    mode: {'f', 'o', 'a'}
      If 'f', fails if any variable already exists.
      if 'o', removes the existing variables.
      if 'a', skip existing variables, writes the others.
    encodings : dict, optional
      If given, skipped variables0 are popped in place.
    itervar : bool
      If True, (data) variables are written one at a time, appending to the zarr.
      If True, this function computes, no matter what was passed to kwargs.
    **kwargs
      Other arguments passed to `ds.to_zarr`.
    """
    path = Path(path)
    if path.is_dir():
        tgtds = zarr.open(str(path), mode='r')
    else:
        tgtds = {}

    if encoding:
        encoding = encoding.copy()

    def _skip(var):
        exists = var in tgtds

        if mode == 'f' and exists:
            raise ValueError(f'Variable {var} exists in dataset {path}.')

        if mode == 'o':
            if exists:
                var_path = path / var
                print(f'Removing {var_path} to overwrite.')
                sh.rmtree(var_path)
            return False

        if mode == 'a':
            return exists

    for var in ds.data_vars.keys():
        if _skip(var):
            print('Skipping', var, 'in', path)
            ds = ds.drop_vars(var)
            if encoding:
                encoding.pop(var)

    if len(ds.data_vars) == 0:
        return None

    # Ensure no funky objects in attrs:
    def coerce_attrs(attrs):
        for k in attrs.keys():
            if not (
                isinstance(attrs[k], (str, float, int, np.ndarray))
                or isinstance(attrs[k], (tuple, list)) and isinstance(attrs[k][0], (str, float, int))
            ):
                attrs[k] = str(attrs[k])

    coerce_attrs(ds.attrs)
    for var in ds.variables.values():
        coerce_attrs(var.attrs)

    if itervar:
        kwargs['compute'] = True
        allvars = set(ds.data_vars.keys())
        if mode != 'a':
            dsbase = ds.drop_vars(allvars)
            dsbase.to_zarr(path, **kwargs)
        for name, var in ds.data_vars.items():
            dsvar = ds.drop_vars(allvars - {name})
            logger.info(f'Writing variable {name} to file {path}.')
            dsvar.to_zarr(
                path,
                mode='a',
                encoding={k: v for k, v in (encoding or {}).items() if k in dsvar},
                **kwargs
            )
    else:
        logger.info(f'Writing variables {list(ds.data_vars.keys())} to file {path}.')
        ds.to_zarr(path, mode='a', encoding=encoding, **kwargs)
    return path


def clean_attrs_cordexaws(ds):
    """Some datasets in the CORDEX-NA-AWS folder have broken attributes.

    This extracts the correct part of the attribute and replaces it IN PLACE.
    """
    name = ds.member_id.item()
    for attrname, attrval in ds.attrs.items():
        if isinstance(attrval, str):
            try:
                val = json.loads(attrval)
            except json.JSONDecodeError:
                pass
            else:
                if isinstance(val, dict) and name in val:
                    ds.attrs[attrname] = val[name]


@parse_config
def clean_up_ds(ds, *, bbox=None, variables=None, exp_freq='D'):

    if 'member_id' in ds.coords:
        clean_attrs_cordexaws(ds)

    if variables is not None:
        ds = ds.drop_vars(
            set(ds.data_vars.keys()).union(name for name, crd in ds.coords.items() if name not in crd.dims) - set(variables) - {'lon', 'lat'}
        )

    if bbox is not None:
        ds = subset.subset_bbox(ds, **bbox)

    # Check times
    if exp_freq is not None:
        freq = xr.infer_freq(ds.time)
        if freq != exp_freq:
            raise ValueError(f'Got freq {freq} instead of {exp_freq}.')

        if exp_freq == 'D':
            ds['time'] = ds.time.dt.floor(exp_freq)

    return fix_cordex_na(ds)


def fix_cordex_na(ds):
    """AWS-stored CORDEX datasets are all on the same standard calendar, this converts
    the data back to the original calendar, removing added NaNs.
    """
    orig_calendar = ds.attrs.get('original_calendar', 'standard')

    if orig_calendar in ['365_day', '360_day']:
        logger.info(f'Converting calendar to {orig_calendar}')
        ds = xcal.convert_calendar(ds, 'noleap')  # drops Feb 29th
        if orig_calendar == '360_day':
            time = xcal.date_range_like(ds.time, calendar='360_day')
            ds = ds.where(~ds.time.dt.dayofyear.isin([32, 91, 152, 244, 305]), drop=True)
            if ds.time.size != time.size:
                raise ValueError('I thought I had it woups. Conversion to 360_day failed.')
            ds['time'] = time
    return ds
