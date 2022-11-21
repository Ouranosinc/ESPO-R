"""Climate indices calculator."""
import atexit
from pathlib import Path
import shutil
import subprocess as sp

import dask
from dask.distributed import Client
import logging
import numpy as np
import xarray as xr
import xclim as xc
from xscen.catalog import ProjectCatalog
from xscen.config import load_config, CONFIG
from xscen.indicators import compute_indicators, load_xclim_module
from xscen.io import save_to_zarr
from xscen.scripting import send_mail_on_exit, timeout, TimeoutException
from xscen.utils import get_cat_attrs
import zarr
logger = logging.getLogger('workflow')


def get_missing_variables(scat, portraits):
    missing = []
    for indname, ind in portraits.iter_indicators():
        freq = ind.injected_parameters['freq'].replace('YS', 'AS-JAN')
        var_name = ind.cf_attrs[0]['var_name']
        if not scat.search(xrfreq=freq, variable=var_name):
            missing.append((indname, ind))
    return missing


if __name__ == '__main__':
    load_config('../my_config.yml', '../paths.yml', verbose=True)
    dask.config.set({k: v for k, v in CONFIG['dask'].items() if k not in ['local', 'client']})
    # dask.config.set(**CONFIG['dask']['local'])
    client = Client(**CONFIG['dask']['client'])

    atexit.register(send_mail_on_exit)

    logger.info('Reading catalog and indicators.')
    cat = ProjectCatalog('../project.json')
    mod = load_xclim_module('portraits')

    for (dsid,), scat in cat.search(processing_level=['biasadjusted', 'derived', 'prop-ref']).iter_unique('id'):
        missing = get_missing_variables(scat, mod)
        if len(missing) == 0:
            logger.info(f'Everything already done for {dsid}.')
            continue
        logger.info(f'{len(missing)} variables missing for {dsid}, computing.')

        if dsid == 'ERA5-land_AMNO':
            ds = xr.open_zarr(CONFIG['paths']['reference'])
            ds.attrs.update({'cat:source': 'ERA5-land', 'cat:experiment': 'obs', 'cat:id': dsid,
                             'cat:domain': 'AMNO', 'cat:xrfreq': 'D'})
            template = CONFIG['indicators']['ref_template']
        else:
            daily = scat.search(frequency='day')
            _, ds = daily.to_dataset_dict().popitem()
            print()
            if CONFIG['indicators']['work_on_local_copy']:
                extpath = Path(daily.df.iloc[0].path)
                inpath = Path(CONFIG['indicators']['tempdir']) / extpath.name
                if not inpath.is_dir():
                    logger.info('Copying dataset to local dir with rsync.')
                    res = sp.run(['rsync', '-a', '--info=progress2', str(extpath), str(inpath.parent)])
                ds = xr.open_zarr(inpath).assign_attrs(**ds.attrs)
            ds.attrs.update({k.replace('/', ':'): v for k, v in ds.attrs.items() if 'cat/' in k})
            template = CONFIG['indicators']['filename_template']

        ds = ds.assign(tas=xc.atmos.tg(ds=ds))

        for indname, ind in missing:
            logger.info(f'Computing {indname}.')
            freq = ind.injected_parameters['freq'].replace('YS', 'AS-JAN')
            var_name = ind.cf_attrs[0]['var_name']
            try:
                with timeout(7200, indname):
                    if freq == '2QS-OCT':
                        iAPR = np.where(ds.time.dt.month == 4)[0][0]
                        dsi = ds.isel(time=slice(iAPR, None))
                    else:
                        dsi = ds
                    if 'rolling' in ind.keywords:
                        temppath = Path(CONFIG['indicators']['outdir']) / f"temp_{indname}.zarr"
                        mult, *parts = xc.core.calendar.parse_offset(freq)
                        steps = xc.core.calendar.construct_offset(mult * 8, *parts)
                        for i, slc in enumerate(dsi.resample(time=steps).groups.values()):
                            dsc = dsi.isel(time=slc)
                            logger.info(f"Computing on slice {dsc.indexes['time'][0]}-{dsc.indexes['time'][-1]}.")
                            _, out = compute_indicators(dsc, indicators=[ind], to_level='derived').popitem()
                            outpath = Path(CONFIG['indicators']['outdir']) / template.format(**get_cat_attrs(out))
                            kwargs = {} if i == 0 else {'append_dim': 'time'}
                            save_to_zarr(out, temppath, rechunk={'time': -1}, mode='a', zarr_kwargs=kwargs)
                        logger.info(f'Moving from temp dir to final dir, removing temp dir.')
                        if outpath.is_dir():
                            if (outpath / var_name).is_dir():
                                shutil.rmtree(outpath / var_name)
                            shutil.move(temppath / var_name, outpath / var_name)
                            shutil.rmtree(temppath)
                        else:
                            shutil.move(temppath, outpath)
                        zmeta = (outpath / '.zmetadata')
                        if zmeta.is_file():
                            zmeta.unlink()  # Remove zmetadata as we added a var without updating it.
                        out = xr.open_zarr(outpath)[[var_name]]
                    else:
                        _, out = compute_indicators(dsi, indicators=[ind], to_level='derived').popitem()
                        outpath = Path(CONFIG['indicators']['outdir']) / template.format(**get_cat_attrs(out))
                        save_to_zarr(out, outpath, rechunk={'time': -1}, mode='a')
            except TimeoutException:
                logger.error(f'Timeout for task {indname}.')
                if 'rolling' in ind.keywords:
                    logger.warn(f'Removing folder {temppath}.')
                    shutil.rmtree(temppath)
                else:
                    for var in out.data_vars.keys():
                        logger.warn(f"Removing folder {outpath / var}.")
                        shutil.rmtree(outpath / var)
                continue

            cat.refresh()
            cat.update_from_ds(out, path=outpath)

        if CONFIG['indicators']['work_on_local_copy']:
            logger.info('Removing local copy of dataset.')
            shutil.rmtree(inpath)

        cat.refresh()
        # Iterate over datasets in catalog, excluding frequencies starting with d
        for path in cat.search(id=dsid, frequency=["^[^d].*"]).df.path:
            # Consolidate metadata merges all attributes in one file.
            zarr.consolidate_metadata(str(path))
