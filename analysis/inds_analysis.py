from dask.distributed import Client
import dask
from dask.diagnostics import ProgressBar
from clisops.core.subset import create_weight_masks
import geopandas as gpd
import h5py
from pathlib import Path
import xarray as xr
import xclim as xc
import xscen as xs
import logging
logger = logging.getLogger('workflow')

if __name__ == '__main__':
    hausfat = [
        "BCC-CSM2-MR", "FGOALS-g3", "CMCC-ESM2", "CNRM-ESM2-1", "ACCESS-CM2", "ACCESS-ESM1-5",
        "MPI-ESM1-2-HR", "INM-CM5-0", "MIROC6", "MPI-ESM1-2-LR", "MRI-ESM2-0", "NorESM2-LM", "KACE-1-0-G", "GFDL-ESM4"
    ]
    sgmembs = [
        'CanESM2', 'CMCC-CMS', 'ACCESS1-3', 'BNU-ESM', 'INM-CM4',
        'IPSL-CM5A-LR', 'IPSL-CM5B-LR', 'HadGEM2-CC', 'MPI-ESM-LR', 'NorESM1-M', 'GFDL-ESM2M'
    ]

    cmip6 = (
        # '/jarre/scenario/jlavoie/ESPO-G6/cat_ESPO-G6_RDRS.json',
        '/tank/scenario/catalogues/simulation.json',
        dict(mip_era='CMIP6', source=hausfat, experiment=['ssp245'], processing_level='raw')
    )

    probreg = dict(
        name="ok-reg",
        method="bbox",
        bbox=dict(
            lat_bnds=[48, 50],
            lon_bnds=[-75, -70]
        ),
        buffer=1.5
    )

    regions = gpd.read_file('/exec/pbourg/PC/20230226/regions_simplified_admin.geojson')
    estrie = regions.iloc[[29]]

    for cat_file, crits in [cmip6]:
        with Client(
            n_workers=1,
            threads_per_worker=3,
            memory_limit='30GB',
            local_directory='/exec/pbourg/temp',
            dashboard_address=8785,
            # silence_logs=50
        ):
            scats = xs.search_data_catalogs(
                cat_file,
                variables_and_freqs={'tasmin': 'D', 'tasmax': 'D', 'dtr': 'D'},
                other_search_criteria=crits,
                match_hist_and_fut=True,
                allow_resampling=True,
                allow_conversion=True,
                restrict_members={'ordered': 1}
            )

            for dsid, scat in scats.items():
                if 'ESPO-G6' in scat.unique('bias_adjust_project'):
                    dsid = 'ESPO-G6_' + dsid
                print('Processing', dsid)
                outfile = Path(f'/exec/pbourg/ESPO-G/indsanal/day_{dsid}_ok-reg.zarr')
                if outfile.is_dir():
                    print('done')
                    continue
                if 'MRI-ESM2-0' in dsid:
                    kws = dict(chunks={'time': 1461, 'lat': 160, 'lon': 320, })
                elif 'CanDCS-U6' in dsid or 'CMIP6_ScenarioMIP' in dsid:
                    kws = dict(chunks={'time': 1461, 'lat': 160, 'lon': 320})
                else:
                    kws = {}
                if scat.df.format[0] == 'zarr':
                    engine = 'zarr'
                elif not h5py.is_hdf5(scat.df.path[0]):
                    engine = 'netcdf4'
                else:
                    engine = 'h5netcdf'
                ds = xs.extract_dataset(
                    scat,
                    region=probreg,
                    xr_open_kwargs={'engine': engine, 'use_cftime': True},
                    xr_combine_kwargs={'coords': 'minimal', 'data_vars': 'minimal', 'compat': 'override'}
                )['D']
                with ProgressBar():
                    ds.cf.chunk(
                        **xs.utils.translate_time_chunk(
                            dict(X=-1, Y=-1, time='4year'), ds.time.dt.calendar, ds.time.size
                        )
                    ).to_zarr(outfile)

                outfile2 = outfile.parent / outfile.name.replace('day_', 'yr_')
                if outfile2.is_dir():
                    print('Done')
                    continue
                ds = xr.open_zarr(outfile)
                if 'rotated_pole' in ds:
                    ds = ds.update(xs.regrid.create_bounds_rotated_pole(ds))
                out = xr.merge(
                    [
                        xc.atmos.daily_freezethaw_cycles(ds=ds, freq='MS'),
                        xc.atmos.daily_temperature_range(ds=ds, freq='MS'),
                        create_weight_masks(ds, estrie).squeeze('geom', drop=True).rename('mask')
                    ]
                ).chunk({'time': -1})
                with ProgressBar():
                    out.to_zarr(outfile2)
