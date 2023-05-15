# Comparison with ScenGen - Compute
# This script runs with xscen 0.5.0 (unless the `unstack_dates` call is modified)
import dask
from dask.distributed import Client
from pathlib import Path
import logging
import xarray as xr
from xclim.ensembles import ensemble_percentiles
import xscen as xs
from xscen.config import CONFIG
logger = logging.getLogger('workflow')


def get_indicator_list():
    pcind = xs.indicators.load_xclim_module('../configuration/portraits.yml')
    inds = {}
    for iden, ind in pcind.iter_indicators():
        var_name = ind.cf_attrs[0]['var_name']
        freq = ind._all_parameters['freq'].value.replace('YS', 'AS-JAN')
        inds.setdefault(freq, []).append(var_name)
    return inds


def split_dim(ds, dim):
    if dim not in ds.dims:
        return ds
    das = {}
    for da in ds.data_vars.values():
        if dim not in da.dims:
            continue
        for crd in da[dim].values:
            das[f"{da.name}_{crd}"] = da.sel({dim: crd}, drop=True)
    return xr.Dataset(data_vars=das, attrs=ds.attrs)


def crunch(ds, dataset):
    ds = xs.extract.clisops_subset(ds, CONFIG['regions']['QC'])

    out = {}

    dsm = xs.spatial_mean(
        ds.chunk({'lat': -1, 'lon': -1} if 'RDRS' not in dataset else {'rlat': -1, 'rlon': -1, 'bounds': -1}),
        'xesmf',
        region=CONFIG['regions']['QC'],
        call_clisops=False,
        kwargs={'skipna': True}
    ).rename(name='region').drop_vars(['id'])
    dsu = xs.utils.unstack_dates(ds, winter_starts_year=True)
    dsmu = xs.utils.unstack_dates(dsm, winter_starts_year=True)

    if 'tg_mean' in ds.data_vars:
        # Climatological means
        dsm_hist = dsmu.sel(time=slice('1981', '2010'))
        dsclim_reg = xr.Dataset(data_vars={
            'tg_climato': dsm_hist['tg_mean'].mean('time', keep_attrs=True),
            'tx_climato': dsm_hist['tx_mean'].mean('time', keep_attrs=True),
            'tn_climato': dsm_hist['tn_mean'].mean('time', keep_attrs=True),
            'pr_climato': dsm_hist['prcptot'].mean('time', keep_attrs=True)
        })
        ds_hist = dsu.sel(time=slice('1981', '2010'))
        dsclim = xr.Dataset(data_vars={
            'tg_climato': ds_hist['tg_mean'].mean('time', keep_attrs=True),
            'tx_climato': ds_hist['tx_mean'].mean('time', keep_attrs=True),
            'tn_climato': ds_hist['tn_mean'].mean('time', keep_attrs=True),
            'pr_climato': ds_hist['prcptot'].mean('time', keep_attrs=True)
        })

    if dataset in CONFIG['datasets']['refs']:
        out['timeseries'] = dsmu
        if 'tg_mean' in ds.data_vars:
            out['climato-reg'] = dsclim_reg
            out['climato'] = dsclim
    else:
        out['timeseries'] = ensemble_percentiles(dsmu, values=[10, 50, 90], keep_chunk_size=False, split=False)
        if 'tg_mean' in ds.data_vars:
            out['climato'] = ensemble_percentiles(dsclim, values=[10, 50, 90], keep_chunk_size=False, split=False)
            out['climato-reg'] = ensemble_percentiles(dsclim_reg, values=[10, 50, 90], keep_chunk_size=False, split=False)

        ds_fut = dsu.sel(time=slice('2071', '2100')).mean('time')
        ds_hist = dsu.sel(time=slice('1981', '2010')).mean('time')
        ds_d = ds_fut - ds_hist
        out['delta'] = ensemble_percentiles(ds_d, values=[10, 50, 90], keep_chunk_size=False, split=False)

        dsmud = dsmu - dsmu.sel(time=slice('1981', '2010')).mean('time')
        dsmud = ensemble_percentiles(dsmud, values=[10, 50, 90], keep_chunk_size=False, split=False)
        out['timeseries-delta'] = dsmud
    return out


if __name__ == '__main__':
    xs.load_config('analysis.yml', 'analysis-path.yml')
    dask.config.set(**{k: v for k, v in CONFIG['dask'].items() if k not in ['client-base']})
    Client(n_workers=2, threads_per_worker=10, memory_limit='14GB', **CONFIG['dask']['client-base'])

    catalog = xs.DataCatalog(CONFIG['extract']['catalog'])
    pcat = xs.ProjectCatalog(CONFIG['main']['catalog'])
    inds = get_indicator_list()

    for dataset in (CONFIG['datasets']['sims'] + CONFIG['datasets']['refs']):
        for freq, indlist in inds.items():
            outpath = Path(CONFIG['main']['save_directory']) / 'indicators'
            # if len(list(outpath.glob(f'{dataset}_*_{freq}.zarr')):
            #     logger.info('Some files already exist.')
            #     continue

            try:
                scat = catalog.search(xrfreq=freq, variable=indlist, **CONFIG['extract'][dataset])
                if not scat:
                    print(f'No {indlist}/{freq} for {dataset}.')
                    continue
                enscols = ['driving_institution', 'driving_model', 'institution', 'source', 'member']
                ds = scat.to_dataset(
                    concat_on='experiment' if dataset in CONFIG['datasets']['sims'] else None,
                    create_ensemble_on=enscols if dataset in CONFIG['datasets']['sims'] else None,
                    xarray_open_kwargs={
                        'decode_timedelta': False,
                        'chunks': CONFIG['extract']['best_chunks'].get(dataset, {})
                    }
                )
                if ds.lon.ndim == 2:
                    ds = ds.update(
                        catalog
                        .search(xrfreq='fx', variable=['lon_bounds', 'lat_bounds'], **CONFIG['extract'][dataset])
                        .to_dataset()
                    )
                outs = crunch(ds, dataset)
                tasks = []
                logger.info(f'Computing for {dataset}/{freq} ({len(outs)} versions)')
                for name, ds in outs.items():
                    if not pcat.exists_in_cat(source=dataset, xrfreq=freq, processing_level=f'crunched-{name}'):
                        tasks.append(
                            ds.chunk(-1).to_zarr(outpath / f'{dataset}_{name}_{freq}.zarr', compute=False)
                        )
                dask.compute(tasks)
                for name, ds in outs.items():
                    if not pcat.exists_in_cat(source=dataset, xrfreq=freq, processing_level=f'crunched-{name}'):
                        pcat.update_from_ds(
                            ds,
                            path=outpath / f'{dataset}_{name}_{freq}.zarr',
                            source=dataset,
                            xrfreq=freq,
                            processing_level=f'crunched-{name}'
                        )
            except ValueError as e:
                logger.exception(f'Crunching of {dataset}/{freq} failed with {e}')
                continue
