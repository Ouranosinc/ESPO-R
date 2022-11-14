# Script
import argparse
import atexit
from pathlib import Path
# from dask.diagnostics import ProgressBar
from dask.distributed import Client
from dask import compute, config as dskconf
import logging
import pandas as pd
from xclim.core.calendar import convert_calendar, get_calendar

from xscen.biasadjust import train, adjust
from xscen.catalog import ProjectCatalog
from xscen.common import translate_time_chunk, stack_drop_nans, unstack_fill_nan
from xscen.config import CONFIG, load_config
from xscen.dianogstics import properties_and_measures
from xscen.extraction import search_data_catalogs, extract_dataset
from xscen.io import rechunk, save_to_zarr
from xscen.regrid import regrid_dataset
from xscen.scripting import measure_time, send_mail_on_exit, timeout
from xscen.utils import get_cat_attrs
from variables import dtr, tasmin_from_dtr


# Load configuration, verbose only for the master process.
load_config('../paths.yml', '../my_config.yml', verbose=(__name__ == '__main__'))
logger = logging.getLogger('workflow')


def search_raw_sims(dsid):
    crit = dict(zip(['driving_model', 'institution', 'source', 'experiment'], dsid.split('_')))
    if crit['institution'] == 'OURANOS':
        drivers = {
            "MPI-M-MPI-ESM-LR": ("MPI-M", "MPI-ESM-LR"),
            "CCCma-CanESM2": ("CCCma", "CanESM2"),
            "NOAA-GFDL-GFDL-ESM2M": ("NOAA-GFDL", "GFDL-ESM2M"),
            "CNRM-CERFACS-CNRM-CM5": ("CNRM-CERFACS", "CNRM-CM5")
        }
        crit['driving_institution'], crit['driving_model'] = drivers[crit['driving_model']]
        scat = search_data_catalogs(
            other_search_criteria=crit,
            **CONFIG['extract']['sim-mrcc5']
        )
        sim_id, scat = scat.popitem()
        scat.df['driving_model'] = dsid.split('_')[0]
        scat.df['driving_institution'] = pd.NA
    else:
        scat = search_data_catalogs(
            other_search_criteria=crit,
            **CONFIG['extract']['sim-cordex']
        )
        sim_id, scat = scat.popitem()
    return scat


def get_task_checker(tasks, start=0, stop=-1, exclude=None):
    exclude = exclude or []
    if isinstance(start, int):
        start = tasks[start]
    if isinstance(stop, int):
        stop = tasks[stop]

    # Transform into a dictionary where keys are task names and values their rank.
    tasks = dict(map(reversed, enumerate(tasks)))

    def _task_checker(task):
        return tasks[start] <= tasks[task] <= tasks[stop] and task not in exclude

    return _task_checker


if __name__ == '__main__':
    daskkws = CONFIG['dask'].get('client-base', {})
    dskconf.set(**{k: v for k, v in CONFIG['dask'].items() if k != 'client'})

    # ProgressBar().register()
    parser = argparse.ArgumentParser(
        description="Compute the whole regrid-bias-adjust workflow for a given dataset.")
    parser.add_argument('--mode', '-m', type=str, nargs=1, default='f',
                        help='Writing mode : f to fail on existing stores, o to overwrite everything, a to append to zarr stores.')
    parser.add_argument("simulation", type=str, nargs=1,
                        help=f"The simulation name to process")
    parser.add_argument('--conf', '-c', action='append', nargs=2, default=[],
                        help=f"Extra config options to set (dotted name, value).")

    args = parser.parse_args()
    dsid = args.simulation[0]
    mode = args.mode[0]
    CONFIG.update_from_list(args.conf)

    wdir = Path(CONFIG['paths']['workdir'])
    variables = CONFIG['main']['variables']
    ref_period = slice(*map(str, CONFIG['main']['reference_period']))
    tgt_period = slice(*map(str, CONFIG['main']['target_period']))

    region = CONFIG['main']['region']
    region_data = CONFIG['regions'][region]

    atexit.register(send_mail_on_exit, subject=f"Simulation {dsid}/{region}")

    pcat = ProjectCatalog('../project.json')

    logger.info(f"Starting or pursuing computation for simulation: {dsid}/{region}")

    ##############################
    # Section 0 - Get references #
    ##############################

    # Check if they exist in the catalog.
    if len(pcat.search(id=['ref-noleap', 'ref-standard', 'ref-360'], domain=region, processing_level='prepared').df) < 3:
        with (
            Client(n_workers=3, threads_per_worker=5, memory_limit="15GB", **daskkws),
            measure_time(name='makeref', logger=logger)
        ):
            logger.info('Extracting ref, gridref and regridding the former to the latter')
            ref_cat = search_data_catalogs(
                **CONFIG['extract']['ref']
            ).popitem()[1]
            dref = extract_dataset(
                ref_cat,
                region={'buffer': 1, **region_data},
            )['D']

            gridref_cat = search_data_catalogs(
                **CONFIG['extract']['gridref']
            ).popitem()[1]
            dgridref = extract_dataset(
                gridref_cat,
                region=region_data,
            )['D']
            pvar = list(dgridref.data_vars.keys())[0]
            dgridref['mask'] = dgridref[pvar].isel(time=0).notnull()

            # Regrid ref to gridref
            dref = regrid_dataset(
                dref, dgridref, regridder_kwargs=dict(method='conservative'), to_level='prepared'
            )

            logger.info('Computing ref properties')
            dref_props, _ = properties_and_measures(dref.chunk({'time': -1}), '../Analysis/properties', to_level_prop='prop-ref')
            props_path = Path(CONFIG['paths']['properties']) / f"ref_{region}_prop-ref.zarr"

            if CONFIG['main']['stack_drop_nans']:
                logger.info('Stacking and dropping nans')
                dref = stack_drop_nans(
                    dref,
                    dref[variables[0]].isel(time=130, drop=True).notnull(),
                    to_file=wdir / f'coords_{region}.nc'
                )

            logger.info('Converting calendars')
            dref = dref.chunk({d: CONFIG['main']['chunks'][d] for d in dref.dims})
            drefnl = convert_calendar(dref, "noleap")
            dref360 = convert_calendar(dref, "360_day", align_on="year")

            encoding = {v: {'dtype': 'float32'} for v in variables}

            tasks = [
                save_to_zarr(dref, wdir / f"ref_{region}_standard.zarr", compute=False, encoding=encoding),
                save_to_zarr(drefnl, wdir / f"ref_{region}_noleap.zarr", compute=False, encoding=encoding),
                save_to_zarr(dref360, wdir / f"ref_{region}_360_day.zarr", compute=False, encoding=encoding),
                save_to_zarr(dref_props, props_path, rechunk={'lon': -1, 'lat': -1, 'distance': -1})
            ]
            logger.info('Working')
            compute(tasks)
            pcat.update_from_ds(dref_props, path=props_path)
            pcat.update_from_ds(dref, path=wdir / f"ref_{region}_standard.zarr", processing_level='prepared', id='ref-standard')
            pcat.update_from_ds(drefnl, path=wdir / f"ref_{region}_noleap.zarr", processing_level='prepared', id='ref-noleap')
            pcat.update_from_ds(dref360, path=wdir / f"ref_{region}_360_day.zarr", processing_level='prepared', id='ref-360_day')

    ##################################
    # Section 1 - Extract and regrid #
    ##################################
    if not pcat.search(id=dsid, processing_level='regridded', domain='region'):
        with (
            Client(n_workers=8, threads_per_worker=3, memory_limit="7GB", **daskkws),
            measure_time(name='regrid', logger=logger)
        ):
            scat = search_raw_sims(dsid)
            ds_in = extract_dataset(
                scat,
                xr_combine_kwargs={'data_vars': 'minimal'}
            )['D']

            dref = pcat.search(id='ref-default', processing_level='prepared', domain=region).to_dask()
            encoding = {k: {'dtype': 'float32'} for k in variables}

            out = regrid_dataset(ds_in, dref)
            out = out.chunk(translate_time_chunk({'time': '4year'}, get_calendar(out), out.time.size))
            save_to_zarr(out, wdir / f"{dsid}_{region}_regridded.zarr", mode=mode, encoding=encoding)
            pcat.update_from_ds(
                out,
                id=dsid,
                domain=region,
                path=wdir / f"{dsid}_{region}_regridded.zarr",
                processing_level="regridded"
            )

    if not pcat.search(id=dsid, processing_level='rechunked', domain=region):
        with (
            Client(n_workers=2, threads_per_worker=5, memory_limit="18GB", **daskkws),
            measure_time(name=f'rechunk', logger=logger)
        ):
            dsentry = pcat.search(id=dsid, processing_level='regridded', domain=region)
            ds = dsentry.to_dask(xarray_open_kwargs={'use_cftime': True})
            cal = get_calendar(ds)
            Nt = ds.time.size

            chunks = {v: {d: CONFIG['custom']['chunks'][d] for d in ds[v].dims} for v in variables}
            chunks.update(time=None, lat=None, lon=None)
            chunks = translate_time_chunk(chunks, cal, Nt)

            rechunk(
                dsentry.path.iloc[0],
                str(wdir / f"{dsid}_{region}_rechunked.zarr"),
                chunks_over_var=chunks,
                worker_mem="2GB"
            )
            pcat.update_from_ds(
                ds,
                processing_level='rechunked',
                path=wdir / f"{dsid}_{region}_rechunked.zarr"
            )

    if not pcat.search(id=dsid, processing_level='prop-sim', domain=region):
        with (
            Client(n_workers=9, threads_per_worker=3, memory_limit="7GB", **daskkws),
            measure_time(name=f'simproperties', logger=logger),
            timeout(3600, task='simproperties')
        ):
            dsim = pcat.search(id=dsid, processing_level='regridded', domain=region).to_dask(xarray_open_kwargs={'use_cftime': True})
            simcal = get_calendar(dsim)

            dref = pcat.search(id=f'ref-{simcal}', domain=region, processing_level='prepared').to_dask(xarray_open_kwargs={'use_cftime': True})
            dref_props = pcat.search(id=f'ref-{simcal}', domain=region, processing_level='prop-ref').to_dask(xarray_open_kwargs={'use_cftime': True})

            props, meas = properties_and_measures(
                dsim.chunk({'time': -1}),
                '../Analysis/properties',
                dref_for_measure=dref_props,
                to_level_prop='prop-sim',
                to_level_meas='meas-sim'
            )

            out_dir = Path(CONFIG['paths']['properties'])
            out_dir.mkdir(exist_ok=True, parents=True)
            compute(
                save_to_zarr(props, out_dir / f"{dsid}_{region}_prop-sim.zarr", mode=mode, compute=False),
                save_to_zarr(meas, out_dir / f"{dsid}_{region}_meas-sim.zarr", mode=mode, compute=False)
            )

            pcat.update_from_ds(props, path=out_dir / f"{dsid}_{region}_prop-sim.zarr")
            pcat.update_from_ds(meas, path=out_dir / f"{dsid}_{region}_meas-sim.zarr")

    if len(pcat.search(id=dsid, processing_level='train-data', domain=region, variable=variables)) != len(variables):
        with Client(n_workers=9, threads_per_worker=3, memory_limit="7GB", **daskkws) as c:
            dhist = pcat.search(
                id=dsid, processing_level='rechunked', domain=region, variable=variables
            ).to_dask(xarray_open_kwargs={'use_cftime': True}).sel(time=slice(*tgt_period))
            dhist = dhist.assign(dtr=dtr(tasmin=dhist.tasmin, tasmax=dhist.tasmax))
            dref = pcat.search(id=f'ref-{simcal}', domain=region, processing_level='prepared').to_dask(xarray_open_kwargs={'use_cftime': True})

            for var, conf in CONFIG['biasadjust']['variables'].items():
                outfile = wdir / f"{dsid}_{region}_{var}_training.zarr"
                kwargs = conf.copy()
                kwargs.pop('adjust', None)

                with measure_time(name=f'train {var}', logger=logger):
                    trds = train(dref, dhist, var=[var], **kwargs)
                    save_to_zarr(trds, outfile)
                pcat.update_from_ds(
                    dhist, path=outfile, processing_level='training_data', xrfreq='fx', frequency='fx', date_start=None, date_end=None
                )

    if len(pcat.search(id=dsid, processing_level='adjusted', domain=region, variable=variables)) != len(variables):
        with Client(n_workers=6, threads_per_worker=3, memory_limit="10GB", **daskkws) as c:
            dsim = pcat.search(
                id=dsid, processing_level='rechunked', domain=region, variable=variables
            ).to_dask(xarray_open_kwargs={'use_cftime': True}).sel(time=slice(*ref_period))
            dsim = dsim.assign(dtr=dtr(tasmin=dsim.tasmin, tasmax=dsim.tasmax))

            for var, conf in CONFIG['biasadjust']['variables'].items():
                if var not in variables:
                    continue
                outfile = wdir / f"{dsid}_{region}_{var}_adjusted.zarr"
                adjkwargs = conf.get('adjust', {})
                trds = pcat.search(
                    id=dsid, domain=region, processing_level='training_data', variable=var
                ).to_dask(xarray_open_kwargs={'use_cftime': True})

                with measure_time(name=f'adjust {var}', logger=logger):
                    scen = adjust(
                        trds, dsim, [tgt_period.start, tgt_period.stop], adjkwargs
                    )
                    save_to_zarr(scen, outfile)
                pcat.update_from_ds(scen, path=outfile, processing_level='adjusted')

    if not pcat.search(id=dsid, processing_level='clean', domain=region):
        with (
            Client(n_workers=4, threads_per_worker=3, memory_limit="15GB", **daskkws),
            measure_time(name=f'cleanup', logger=logger)
        ):
            scen = pcat.search(id=dsid, processing_level='adjusted', variable=variables).to_dask(xarray_open_kwargs={'use_cftime': True})
            if 'dtr' in scen and 'tasmax' in scen:
                scen['tasmin'] = tasmin_from_dtr(dtr=scen.dtr, tasmax=scen.tasmax)
                scen = scen.drop_vars('dtr')

            if CONFIG['main']['stack_drop_nans']:
                scen = unstack_fill_nan(scen, coords=wdir / f'coords_{region}.nc')
                scen = scen.chunk({d: CONFIG['main']['chunks'][d] for d in ds.dims})

            fmtkws = get_cat_attrs(scen)

            for var, attrs in CONFIG['main']['attrs'].items():
                obj = scen if var == 'global' else scen[var]
                for attrname, attrtmpl in attrs.items():
                    obj.attrs[attrname] = attrtmpl.format(**fmtkws)

            for var in ds.data_vars.values():
                for attr in list(var.attrs.keys()):
                    if attr not in CONFIG['main']['final_attrs_names']:
                        del var.attrs[attr]

            save_to_zarr(scen, wdir / f"{dsid}_{region}_final.zarr", mode=mode)
            pcat.update_from_ds(scen, path=wdir / f"{dsid}_{region}_final.zarr", processing_level='clean')

    if not pcat.search(id=dsid, processing_level='prop-scen', domain=region):
        with (
            Client(n_workers=9, threads_per_worker=3, memory_limit="7GB", **daskkws),
            measure_time(name=f'scenproperties', logger=logger),
            timeout(3600, task='scenproperties')
        ):
            dscen = pcat.search(id=dsid, processing_level='clean', domain=region).to_dask(xarray_open_kwargs={'use_cftime': True})
            scencal = get_calendar(dscen)

            dref = pcat.search(id=f'ref-{scencal}', domain=region, processing_level='prepared').to_dask(xarray_open_kwargs={'use_cftime': True})
            dref_props = pcat.search(id=f'ref-{scencal}', domain=region, processing_level='prop-ref').to_dask(xarray_open_kwargs={'use_cftime': True})

            props, meas = properties_and_measures(
                dscen.chunk({'time': -1}),
                '../Analysis/properties',
                dref_for_measure=dref_props,
                to_level_prop='prop-scen',
                to_level_meas='meas-scen'
            )

            out_dir = Path(CONFIG['paths']['properties'])
            compute(
                save_to_zarr(props, out_dir / f"{dsid}_{region}_prop-scen.zarr", mode=mode, compute=False),
                save_to_zarr(meas, out_dir / f"{dsid}_{region}_meas-scen.zarr", mode=mode, compute=False)
            )

            pcat.update_from_ds(props, path=out_dir / f"{dsid}_{region}_prop-scen.zarr")
            pcat.update_from_ds(meas, path=out_dir / f"{dsid}_{region}_meas-scen.zarr")

    if not pcat.search(id=dsid, processing_level='final', domain=region):
        with (
            Client(n_workers=3, threads_per_worker=5, memory_limit="20GB", **daskkws),
            measure_time(name=f'final zarr rechunk', logger=logger)
        ):
            dsentry = pcat.search(id=dsid, processing_level='clean', domain=region)
            ds = dsentry.to_dask(xarray_open_kwargs={'use_cftime': True})
            cal = get_calendar(ds)
            Nt = ds.time.size

            chunks = {v: translate_time_chunk(CONFIG['main']['out_chunks'], cal, Nt) for v in variables}
            chunks.update(time=None, lat=None, lon=None)

            out = Path(CONFIG['paths']['output'])
            out.mkdir(exist_ok=True, parents=True)

            rechunk(
                dsentry.path.iloc[0],
                str(out / f"{dsid}_{region}.zarr"),
                chunks_over_var=chunks,
                worker_mem="2GB"
            )
            pcat.update_from_ds(
                ds,
                processing_level='final',
                path=str(out / f"{dsid}_{region}.zarr"),
            )
