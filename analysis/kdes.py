import dask
from dask.diagnostics import ProgressBar
import numpy as np
from pathlib import Path
from scipy.stats import gaussian_kde
import xarray as xr
import xscen as xs
dask.config.set(num_workers=20)
ProgressBar().register()


def dropna(arr):
    return arr[~np.isnan(arr)]


def compute(ds, path):
    dd = {'tasmin': [], 'tasmax': []}
    for period, lbl in [(slice('1991', '2020'), 'present'), (slice('2071', '2100'), 'future')]:
        print('   ', lbl)
        print()
        dss = ds.sel(time=period).load()
        for var in ['tasmin', 'tasmax']:
            o = xr.DataArray(gaussian_kde(dropna(dss[var].values.flatten()))(X), dims=('T',), coords={'T': X}, name=var)
            dd[var].append(o)
    out = xr.merge([xr.concat(dd[var], xr.DataArray(['present', 'future'], dims=('period',), name='period')) for var in ['tasmin', 'tasmax']])
    out.to_zarr(path)


if __name__ == '__main__':
    root = Path('/exec/pbourg/ESPO-G/indsanal/')
    cat = xs.DataCatalog('/exec/pbourg/ESPO-G/indsanal/catalog.json')
    cat.esmcat._df['id'] = xs.catalog.generate_id(cat.df)
    probreg = dict(
        name="prob-reg",
        method="bbox",
        bbox=dict(
            lat_bnds=[45, 47],
            lon_bnds=[-75, -70]
        ),
    )
    X = np.arange(230, 320, 0.5)

    # The extracted ones:
    scat = cat.search(xrfreq='D', domain='prob-reg', experiment=['rcp45', 'ssp245'], variable=['tasmin', 'tasmax'], bias_adjust_project=['ESPO-TN', 'pcic', 'raw'])
    extracted = scat.to_dataset_dict(xarray_open_kwargs={'engine': 'zarr'})
    print()
    for dsid, ds in extracted.items():
        print(dsid)
        print()
        compute(ds, root / 'kde_{bias_adjust_project}_{mip_era}_{source}_{experiment}_prob-reg.zarr'.format(**xs.utils.get_cat_attrs(ds)))

    # The non-extracted
    ESPOG6R2 = (
        xs.DataCatalog('/jarre/scenario/jlavoie/ESPO-G6/cat_ESPO-G6_RDRS.json')
        .search(domain='QC-rdrs', experiment='ssp245', variable=['tasmin', 'tasmax'], xrfreq='D', processing_level='final')
        .to_dataset_dict(
            xarray_open_kwargs={'decode_timedelta': False},
        )
    )
    for dsid, ds in ESPOG6R2.items():
        ds = xs.extract.clisops_subset(ds, region=probreg)
        compute(ds, root / 'kde_{bias_adjust_project}_{mip_era}_{source}_{experiment}_prob-reg.zarr'.format(**xs.utils.get_cat_attrs(ds)))

    SCENGEN = (
        xs.DataCatalog('/tank/scenario/catalogues/ESPO-extra.json')
        .search(experiment='rcp45', variable=['tasmin', 'tasmax'], xrfreq='D', bias_adjust_project='ScenGen')
        .to_dataset_dict(
            xarray_open_kwargs={'decode_timedelta': False}
        )
    )
    for dsid, ds in SCENGEN.items():
        ds = xs.extract.clisops_subset(ds, region=probreg)
        compute(ds, root / 'kde_{bias_adjust_project}_{mip_era}_{source}_{experiment}_prob-reg.zarr'.format(**xs.utils.get_cat_attrs(ds)))
