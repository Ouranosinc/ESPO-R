#!/usr/bin/env python
# coding: utf-8

from clisops.core.subset import create_weight_masks, subset_shape
import dask
from dask.diagnostics import ProgressBar
import geopandas as gpd
import xclim as xc
import xesmf as xe
import xscen as xs
import xarray as xr
dask.config.set(num_workers=8)


if __name__ == '__main__':
    df = xs.catalog.parse_directory(
        directories=['/exec/pbourg/ESPO-G/NAM_SPLIT/iles/'],
        globpattern='*.zarr', 
        patterns=[
            '{frequency}_{mip_era}_{activity}_{institution}_{source}_{experiment}_{member}_{?dom}_{date_start}-{date_end}.zarr'
        ],
        read_from_file=['variable'],
        homogenous_info={
            'bias_adjust_project': 'ESPO-G6',
            'version': 'v1.0.1',
            'domain': 'iles',
            'processing_level': 'biasadjusted'
        }
    )

    cat = xs.DataCatalog({'df': df, 'esmcat': xs.catalog.esm_col_data})

    dref = xr.open_dataset('/exec/pbourg/lakeFrac_fx_ecmwf_era5-land_NAM.nc')
    polys = gpd.read_file('/exec/pbourg/PC/20230116/regions_simplified_admin.geojson')
    idm = polys[polys.name.str.startswith('Îles')]
    mask = (create_weight_masks(subset_shape(dref, idm, buffer=0.25), idm) >  0)
    dsgrid = mask.rename('mask').squeeze('geom', drop=True).to_dataset()

    reg = None
    outpath = '/exec/pbourg/ESPO-G/indicators/raw/ESPO-G6/{frequency}_{mip_era}_{activity}_{institution}_{source}_{experiment}_{member}_IDM_{date_start}-{date_end}.zarr'
    for dsid, dsraw in cat.to_dataset_dict().items():
        print(f'Computing for {dsid}')
        # Regrid
        if reg is None:
            reg = xe.Regridder(dsraw, dsgrid, method='conservative')
        dsreg = reg(dsraw, keep_attrs=True)
        dsreg['cat:domain'] = 'IDM'

        dsreg = dsreg.assign(tas=xc.atmos.tg(ds=dsreg))
        inds = xs.compute_indicators(
            dsreg,
            indicators='/home/pbourg/Projets/ESPO-G/configuration/portraits.yml',
        )
        for freq, outds in inds.items():
            with ProgressBar():
                xs.save_to_zarr(
                    outds,
                    outpath.format(**xs.utils.get_cat_attrs(outds)),
                    mode='o',
                )




