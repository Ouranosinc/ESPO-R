"""Climate indices calculator."""
import atexit
from pathlib import Path
import shutil
import subprocess as sp

import dask
from dask.distributed import Client
import logging
import xarray as xr
import xclim as xc
from xscen.catalog import ProjectCatalog
from xscen.config import load_config, CONFIG
from xscen.indicators import compute_indicators, load_xclim_module
from xscen.io import save_to_zarr
from xscen.scr_utils import send_mail_on_exit, timeout
import zarr
logger = logging.getLogger('workflow')


if __name__ == '__main__':
    load_config('../my_config.yml', '../paths.yml', verbose=True)
    dask.config.set({k: v for k, v in CONFIG['dask'].items() if k not in ['local', 'client']})
    # dask.config.set(**CONFIG['dask']['local'])
    client = Client(**CONFIG['dask']['client'])

    atexit.register(send_mail_on_exit)

    logger.info('Reading catalog and indicators.')
    cat = ProjectCatalog('../project.json')
    mod = load_xclim_module('portraits')

    for (dsid,), scat in cat.search(processing_level='biasadjusted').iter_unique('id'):
        if scat.df.variable.apply(len).sum() == 33:
            logger.info(f'33 variables exist for {dsid}, assuming everything is done.')
            continue

        logger.info(f'Opening {dsid}')
        daily = scat.search(frequency='day').df.iloc[0]
        _, ds = scat.to_dataset_dict().popitem()
        print()
        if CONFIG['indicators']['work_on_local_copy']:
            extpath = Path(daily.path)
            inpath = Path(CONFIG['indicators']['tempdir']) / extpath.name
            if not inpath.is_dir():
                logger.info('Copying dataset to local dir with rsync.')
                res = sp.run(['rsync', '-a', '--info=progress2', str(extpath), str(inpath.parent)])
            ds = xr.open_zarr(inpath).assign_attrs(**ds.attrs)

        ds = ds.assign(tas=xc.atmos.tg(ds=ds))

        for indname, ind in mod.iter_indicators():
            freq = ind.injected_parameters['freq'].replace('YS', 'AS-JAN')
            var_name = ind.cf_attrs[0]['var_name']
            if scat.search(xrfreq=freq, variable=var_name):
                logger.info(f'{indname} already computed.')
                continue

            logger.info(f'Computing {indname}.')
            with timeout(7200, indname):
                if 'rolling' in ind.keywords:
                    temppath = Path(CONFIG['indicators']['outdir']) / f"temp_{indname}.zarr"
                    mult, *parts = xc.core.calendar.parse_offset(freq)
                    steps = xc.core.calendar.construct_offset(mult * 8, *parts)
                    for i, slc in enumerate(ds.resample(time=steps).groups.values()):
                        dsc = ds.isel(time=slc)
                        logger.info(f"Computing on slice {dsc.indexes['time'][0]}-{dsc.indexes['time'][-1]}.")
                        _, out = compute_indicators(dsc, indicators=[ind]).popitem()
                        outpath = Path(CONFIG['indicators']['outdir']) / CONFIG['indicators']['filename_template'].format(**out.attrs)
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
                    _, out = compute_indicators(ds, indicators=[ind]).popitem()
                    outpath = Path(CONFIG['indicators']['outdir']) / CONFIG['indicators']['filename_template'].format(**out.attrs)
                    save_to_zarr(out, outpath, rechunk={'time': -1}, mode='a')

            cat.update_from_ds(out, path=outpath)

        if CONFIG['indicators']['work_on_local_copy']:
            logger.info('Removing local copy of dataset.')
            shutil.rmtree(inpath)

        cat.refresh()
        # Iterate over datasets in catalog, excluding frequencies starting with d
        for path in cat.search(id=dsid, frequency=["^[^d].*"]).df.path:
            # Consolidate metadata merges all attributes in one file.
            zarr.consolidate_metadata(str(path))
