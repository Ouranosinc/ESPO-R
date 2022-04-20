"""
Regridding
"""
from typing import Optional

import os
from pathlib import Path
import xesmf as xe
import xarray as xr

from .config import parse_config


@parse_config
def regrid(
    ds_in: xr.Dataset,
    ds_out: xr.Dataset,
    *,
    method: str = "bilinear",
    weights: Optional[os.PathLike] = None,
    weights_dir: Optional[os.PathLike] = None,
    **kwargs
) -> xr.Dataset:
    """
    Regrid a dataset.

    Parameters
    ----------
    ds_in : xr.Dataset
      Input dataset defining the input grid and containing the variables to regrid.
    ds_out : xr.Dataset
      Dataset defining the output grid.
    method : str
      Regrid method.
    weights : os.PathLike
      A path pointing to a weights file, absolute or relative to weights_dir.
    weights_dir : os.PathLike
      A path pointing to a directory when weights file are saved.
      Defaults to the current working directory.

    ** kwargs:
      Other arguments to pass to the instantiation of the Regridder object.
      They could be unused if weights points to a file or to a directory where the correct file exists.

    Returns
    -------
    xr.Dataset
      Regridded dataset. All variables of `ds_in` with spatial dimensions are regridded, others are left out.
    """
    if weights is not None:
        weights = Path(weights_dir or '.') / weights
        if not weights.is_file():
            # No weights file exist, create the weights.
            reg = xe.Regridder(ds_in, ds_out, method, filename=None, **kwargs)
            reg.to_netcdf(weights)
        else:
            # Re-use existing file.
            reg = xe.Regridder(ds_in, ds_out, method, weights=weights, filename=None, **kwargs)
    else:
        # weights is None : create the weights and don't write them to file.
        reg = xe.Regridder(ds_in, ds_out, method, filename=None, **kwargs)

    return reg(ds_in, keep_attrs=True)
