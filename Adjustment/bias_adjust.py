# Script
import argparse
import atexit
from pathlib import Path
# from dask.diagnostics import ProgressBar
from dask.distributed import Client, performance_report
from dask import compute, config as dskconf
import logging
import matplotlib.pyplot as plt
import xarray as xr
from xclim import atmos
from xclim.core.calendar import convert_calendar, get_calendar
from xclim.sdba import properties

from workflow.biasadjust import train_qm, adjust_qm, dtr, tasmin_from_dtr, add_preprocessing_attr
from workflow.catalog import DataCatalog
from workflow.common import minimum_calendar, translate_time_chunk, stack_drop_nans, unstack_fill_nan
from workflow.config import CONFIG, load_config
from workflow.diagnostics import fig_compare_and_diff, fig_bias_compare_and_diff, rmse
from workflow.io import clean_up_ds, to_zarr_skip_existing
from workflow.regridding import regrid
from workflow.utils import get_task_checker, measure_time, rechunk, send_mail, send_mail_on_exit, timeout

# Load configuration, verbose only for the master process.
load_config('paths.yml', 'my_config.yml', verbose=(__name__ == '__main__'))
logger = logging.getLogger('workflow')


def compute_properties(sim, ref, ref_period, fut_period):
    # Je load deux des variables pour essayer d'éviter les KilledWorker et Timeout
    ds_hist = sim.sel(time=ref_period)
    pr_threshes = ref.pr.quantile([0.9, 0.99], dim='time', keep_attrs=True).load()

    out = xr.Dataset(data_vars={
        'pr_wet_freq_q99_hist': properties.relative_frequency(ds_hist.pr, op='>=', thresh=pr_threshes.sel(quantile=0.99, drop=True), group='time'),
        'tx_mean_rmse': rmse(atmos.tx_mean(ds_hist.tasmax, freq='MS').chunk({'time': -1}), atmos.tx_mean(ref.tasmax, freq='MS').chunk({'time': -1})),
        'tn_mean_rmse': rmse(atmos.tn_mean(ds_hist.tasmin, freq='MS').chunk({'time': -1}), atmos.tn_mean(ref.tasmin, freq='MS').chunk({'time': -1})),
        'prcptot_rmse': rmse(atmos.precip_accumulation(ds_hist.pr, freq='MS').chunk({'time': -1}), atmos.precip_accumulation(ref.pr, freq='MS').chunk({'time': -1})),
        'nan_count': sim.to_array().isnull().sum('time').mean('variable'),
    })
    out['pr_wet_freq_q99_hist'].attrs['long_name'] = 'Relative frequency of days with precip over the 99th percentile of the reference, in the present.'

    if fut_period is not None:
        out['pr_wet_freq_q99_fut'] = properties.relative_frequency(sim.pr.sel(time=fut_period).chunk({'time': -1}), op='>=', thresh=pr_threshes.sel(quantile=0.99, drop=True), group='time')
        out['pr_wet_freq_q99_fut'].attrs['long_name'] = 'Relative frequency of days with precip over the 99th percentile of the reference, in the future.'

    return out


def _maybe_unstack(ds, rechunk=None):
    if CONFIG['custom']['stack_drop_nans']:
        ds = unstack_fill_nan(ds, coords=rdir / f'coords_{region}.nc')
        if rechunk is not None:
            ds = ds.chunk(rechunk)
    return ds


if __name__ == '__main__':
    daskkws = CONFIG['dask'].get('client', {})
    dskconf.set(**{k: v for k, v in CONFIG['dask'].items() if k != 'client'})

    # ProgressBar().register()
    steps = [
        "makeref", "regrid", "rechunk", "simproperties",
        "train", "adjust", "cleanup", "finalzarr", "scenproperties", "checkup"
    ]
    parser = argparse.ArgumentParser(
        description="Compute the whole regrid-bias-adjust workflow for a given dataset.")
    parser.add_argument("--jumpto", '-j', type=str, nargs=1, default=['makeref'],
                        help=f'Jump to a section (one of {steps})')
    parser.add_argument("--stopat", '-s', type=str, nargs=1, default=['checkup'],
                        help=f'Stop after doing a section (one of {steps})')
    parser.add_argument("--exclude", '-x', type=str, nargs='*', default=[],
                        help=f"Don't perform certain sections (one or more of {steps})")
    parser.add_argument('--mode', '-m', type=str, nargs=1, default='f',
                        help='Writing mode : f to fail on existing stores, o to overwrite everything, a to append to zarr stores.')
    parser.add_argument("simulation", type=str, nargs=1,
                        help=f"The simulation name to process")
    parser.add_argument('--conf', '-c', action='append', nargs=2, default=[],
                        help=f"Extra config options to set (dotted name, value).")

    args = parser.parse_args()
    parts = args.simulation[0].split('_')
    scenario = parts[-1]
    simulation = '_'.join(parts[:-1])
    mode = args.mode[0]
    CONFIG.update_from_list(args.conf)

    we_should_do = get_task_checker(steps, start=args.jumpto[0], stop=args.stopat[0], exclude=args.exclude)

    wkdir = Path(CONFIG['paths']['workdir'])
    rdir = Path(CONFIG['paths']['refdir'])
    variables = CONFIG['custom']['variables']
    ref_period = slice(*map(str, CONFIG['custom']['reference_period']))
    tgt_period = slice(*map(str, CONFIG['custom']['target_period']))
    fut_period = slice(*map(str, CONFIG['custom']['future_period']))
    region = CONFIG['custom']['region']
    fmtkws = {'region': region, 'simulation': simulation, 'scenario': scenario}
    dask_perf_file = Path(CONFIG['paths']['reports'].format(**fmtkws)) / 'perf_report_template.html'
    dask_perf_file.parent.mkdir(exist_ok=True, parents=True)

    atexit.register(send_mail_on_exit, subject=f"Simulation {simulation}_{scenario}/{region}")

    logger.info(f"Starting or pursuing computation for simulation: {simulation}_{scenario}/{region}")

    if we_should_do('makeref'):
        with (
            Client(n_workers=3, threads_per_worker=5, memory_limit="15GB", **daskkws),
            performance_report(dask_perf_file.with_name('perf_report_makeref.html')),
            measure_time(name='makeref', logger=logger)
        ):
            # Open ref
            dref = xr.open_zarr(CONFIG['paths']['reference'])

            bbox = CONFIG['custom']['bbox'][region]
            dref = clean_up_ds(dref, variables=variables, bbox=bbox)

            dref_ref = dref.sel(time=ref_period).chunk({'time': -1})
            dref_props = compute_properties(dref_ref, dref_ref, ref_period, None).chunk({'lon': -1, 'lat': -1})

            if CONFIG['custom']['stack_drop_nans']:
                dref = stack_drop_nans(
                    dref,
                    dref[variables[0]].isel(time=130, drop=True).notnull(),
                    to_file=rdir / f'coords_{region}.nc'
                )

            dref = dref.chunk({d: CONFIG['custom']['chunks'][d] for d in dref.dims})
            drefnl = convert_calendar(dref, "noleap")
            dref360 = convert_calendar(dref, "360_day", align_on="year")

            encoding = {v: {'dtype': 'float32'} for v in variables}

            tasks = [
                dref.to_zarr(rdir / f"ref_{region}_default.zarr", compute=False, encoding=encoding),
                drefnl.to_zarr(rdir / f"ref_{region}_noleap.zarr", compute=False, encoding=encoding),
                dref360.to_zarr(rdir / f"ref_{region}_360_day.zarr", compute=False, encoding=encoding),
                dref_props.to_zarr(rdir / f"ref_{region}_properties.zarr", compute=False)
            ]
            compute(tasks)

            logger.info('Reference generated, painting nan count and sending plot.')
            dref_props = xr.open_zarr(rdir / f"ref_{region}_properties.zarr").load()

            fig, ax = plt.subplots(figsize=(10, 10))
            cmap = plt.cm.winter.copy()
            cmap.set_under('white')
            dref_props.nan_count.plot(ax=ax, vmin=1, vmax=1000, cmap=cmap)
            ax.set_title(f'Reference {region} - NaN count \nmax {dref_props.nan_count.max().item()} out of {dref_ref.time.size}')
            send_mail(
                subject=f'Reference for region {region} - Success',
                msg=f"Action 'makeref' succeeded for region {region}.",
                attachments=[fig]
            )
            plt.close('all')

    if we_should_do('regrid'):
        with (
            Client(n_workers=8, threads_per_worker=3, memory_limit="7GB", **daskkws),
            performance_report(dask_perf_file.with_name('perf_report_regrid.html')),
            measure_time(name='regrid', logger=logger)
        ):
            cat = DataCatalog.from_csv(['catalog.csv'])
            cat = cat.search(simulation_id=f"{simulation}_{scenario}")
            if len(cat.keys()) == 2:
                cat = cat.search(domain_id='NAM-22i')
            sim_id, ds_in = cat.to_dataset_dict(
                xarray_combine_by_coords_kwargs={'combine_attrs': 'override'}
            ).popitem()
            print()
            # For regridding we need chunks that along time only, spatial slices.
            # And to combine and write to zarr we need uniform chunks.
            # 40 evenly divided the number of elements in hist`, so that chunks are uniform (except the very last one)
            # But it's a bit small, so we will rechunk the combination to 160 which is a multiple of 40 and also a guess.
            # The thing is : in regridding the spatial size will blow up by a factor of 5
            # we need to choose chunks that will still be reasonable after regridding.
            ds_in = clean_up_ds(ds_in.sel(time=tgt_period), variables=variables)

            ds_out = xr.open_zarr(rdir / f'ref_{region}_noleap.zarr')
            encoding = {k: {'dtype': 'float32'} for k in variables}

            out = regrid(ds_in, ds_out, locstream_out=CONFIG['custom']['stack_drop_nans'])
            out = out.chunk(translate_time_chunk({'time': '4year'}, get_calendar(out), out.time.size))
            to_zarr_skip_existing(out, wkdir / f"ds_regridded.zarr", mode, encoding=encoding, compute=True)

    if we_should_do('rechunk'):
        with (
            Client(n_workers=2, threads_per_worker=5, memory_limit="18GB", **daskkws),
            performance_report(dask_perf_file.with_name(f'perf_report_rechunk.html')),
            measure_time(name=f'rechunk', logger=logger)
        ):
            ds = xr.open_zarr(wkdir / f"ds_regridded.zarr")
            cal = get_calendar(ds)
            Nt = ds.time.size

            chunks = {v: {d: CONFIG['custom']['chunks'][d] for d in ds[v].dims} for v in variables}
            chunks.update(time=None, lat=None, lon=None)
            chunks = translate_time_chunk(chunks, cal, Nt)

            rechunk(
                wkdir / f"ds_regridded.zarr",
                wkdir / f"ds_regchunked.zarr",
                chunks=chunks,
                worker_mem="2GB"
            )

    if we_should_do('simproperties'):
        with (
            Client(n_workers=9, threads_per_worker=3, memory_limit="7GB", **daskkws),
            performance_report(dask_perf_file.with_name(f'perf_report_simprops.html')),
            measure_time(name=f'simproperties', logger=logger),
            timeout(3600, task='simproperties')
        ):
            dsim = xr.open_zarr(wkdir / 'ds_regchunked.zarr')
            simcal = get_calendar(dsim)
            dref = xr.open_zarr(rdir / f"ref_{region}_{simcal}.zarr").sel(time=ref_period)

            out = compute_properties(dsim, dref, ref_period, fut_period)

            out_path = Path(CONFIG['paths']['checkups'].format(
                region=region, simulation=simulation, scenario=scenario, step='sim'
            ))
            out_path.parent.mkdir(exist_ok=True, parents=True)
            to_zarr_skip_existing(out, out_path, mode, itervar=True)

            logger.info('Sim properties computed, painting nan count and sending plot.')
            dsim_props = unstack_fill_nan(xr.open_zarr(out_path), coords=rdir / f'coords_{region}.nc')
            nan_count = dsim_props.nan_count.load()

            fig, ax = plt.subplots(figsize=(12, 8))
            cmap = plt.cm.winter.copy()
            cmap.set_under('white')
            nan_count.plot(ax=ax, vmin=1, vmax=1000, cmap=cmap)
            ax.set_title(f'Raw simulation {simulation}/{scenario} {region} - NaN count \nmax {nan_count.max().item()} out of {dsim.time.size}')
            send_mail(
                subject=f'Properties of {simulation}/{scenario} {region} - Success',
                msg=f"Action 'simproperties' succeeded.",
                attachments=[fig]
            )
            plt.close('all')

    if we_should_do('train'):
        with Client(n_workers=9, threads_per_worker=3, memory_limit="7GB", **daskkws) as c:
            dhist = xr.open_zarr(wkdir / 'ds_regchunked.zarr').sel(time=ref_period)

            simcal = get_calendar(dhist)
            refcal = minimum_calendar(simcal, CONFIG['custom']['maximal_calendar'])
            if simcal != refcal:
                dhist = convert_calendar(dhist, refcal)

            dref = xr.open_zarr(rdir / f"ref_{region}_{refcal}.zarr").sel(time=ref_period)

            for var, conf in CONFIG['biasadjust']['variables'].items():
                if var == 'dtr':
                    ref = dtr(tasmin=dref.tasmin, tasmax=dref.tasmax)
                    hist = dtr(tasmin=dhist.tasmin, tasmax=dhist.tasmax)
                else:
                    ref = dref[var]
                    hist = dhist[var]

                outfile = wkdir / f"ds_{var}_training.zarr"
                if outfile.is_dir() and mode == 'a':
                    logger.warning(f"{var} already trained.")
                    continue

                kwargs = conf.copy()
                kwargs.pop('adjust', None)

                with (
                    performance_report(dask_perf_file.with_name(f'perf_report_train_{var}.html')),
                    measure_time(name=f'train {var}', logger=logger)
                ):

                    trds = train_qm(ref, hist, **kwargs)
                    trds.to_zarr(outfile)

    if we_should_do('adjust'):

        with Client(n_workers=6, threads_per_worker=3, memory_limit="10GB", **daskkws) as c:
            dsim = xr.open_zarr(wkdir / f"ds_regchunked.zarr").sel(time=tgt_period)

            simcal = get_calendar(dsim)
            refcal = minimum_calendar(simcal, CONFIG['custom']['maximal_calendar'])
            if simcal != refcal:
                dsim = convert_calendar(dsim, refcal)

            for var, conf in CONFIG['biasadjust']['variables'].items():
                outfile = wkdir / f"ds_{var}_adjusted.zarr"
                if outfile.is_dir() and mode == 'a':
                    logger.warning(f"{var} already adjusted.")
                    continue

                if var == 'dtr':
                    sim = dtr(tasmin=dsim.tasmin, tasmax=dsim.tasmax)
                else:
                    sim = dsim[var]

                adjkwargs = conf.pop('adjust', {})
                trds = xr.open_zarr(wkdir / f"ds_{var}_training.zarr")

                with (
                    performance_report(dask_perf_file.with_name(f'perf_report_adjust_{var}.html')),
                    measure_time(name=f'adjust {var}', logger=logger)
                ):

                    scen = adjust_qm(trds, sim, **adjkwargs)
                    add_preprocessing_attr(scen, conf)
                    scen.to_dataset().to_zarr(outfile)

    if we_should_do('cleanup'):

        with (
            Client(n_workers=4, threads_per_worker=3, memory_limit="15GB", **daskkws),
            performance_report(dask_perf_file.with_name(f'perf_report_cleanup.html')),
            measure_time(name=f'cleanup', logger=logger)
        ):
            cat = DataCatalog.from_csv(['catalog.csv'])
            cat = cat.search(simulation_id=f"{simulation}_{scenario}")
            if len(cat.keys()) == 2:
                cat = cat.search(domain_id='NAM-22i')
            refattrs = xr.open_dataset(cat.df.iloc[0].path).attrs

            outs = {v: xr.open_zarr(wkdir / f"ds_{v}_adjusted.zarr")[v] for v in CONFIG['biasadjust']['variables'].keys()}

            if 'dtr' in outs and 'tasmax' in outs:
                outs['tasmin'] = tasmin_from_dtr(dtr=outs.pop('dtr'), tasmax=outs['tasmax'])

            ds = xr.Dataset(data_vars=outs)

            if CONFIG['custom']['stack_drop_nans']:
                ds = unstack_fill_nan(ds, coords=rdir / f'coords_{region}.nc')
                ds = ds.chunk({d: CONFIG['custom']['chunks'][d] for d in ds.dims})

            for var, attrs in CONFIG['custom']['attrs'].items():
                obj = ds if var == 'global' else ds[var]
                for attrname, attrtmpl in attrs.items():
                    obj.attrs[attrname] = attrtmpl.format(refattrs=refattrs, **fmtkws)
            for var in ds.data_vars.values():
                for attr in list(var.attrs.keys()):
                    if attr not in CONFIG['custom']['final_attrs_names']:
                        del var.attrs[attr]

            to_zarr_skip_existing(ds, wkdir / "ds_final.zarr", mode, compute=True)

    if we_should_do('finalzarr'):
        with (
            Client(n_workers=3, threads_per_worker=5, memory_limit="20GB", **daskkws),
            performance_report(dask_perf_file.with_name(f'perf_report_final_zarr.html')),
            measure_time(name=f'final zarr rechunk', logger=logger)
        ):
            with xr.open_zarr(wkdir / "ds_final.zarr") as ds:
                cal = get_calendar(ds)
                timesize = ds.time.size

            chunks = {v: translate_time_chunk(CONFIG['custom']['out_chunks'], cal, timesize) for v in variables}
            chunks.update(time=None, lat=None, lon=None)

            out = Path(CONFIG['paths']['output'].format(**fmtkws))
            out.parent.mkdir(exist_ok=True, parents=True)

            rechunk(
                wkdir / f"ds_final.zarr",
                out,
                chunks=chunks,
                worker_mem="2GB"
            )

    if we_should_do('scenproperties'):
        with (
            Client(n_workers=9, threads_per_worker=3, memory_limit="7GB", **daskkws),
            performance_report(dask_perf_file.with_name(f'perf_report_scenprops.html')),
            measure_time(name=f'scenprops', logger=logger),
            timeout(5400, task='scenproperties')
        ):
            dscen = xr.open_zarr(CONFIG['paths']['output'].format(**fmtkws))
            scen_cal = get_calendar(dscen)
            dref = _maybe_unstack(
                xr.open_zarr(rdir / f"ref_{region}_{scen_cal}.zarr").sel(time=ref_period),
                rechunk={d: CONFIG['custom']['out_chunks'][d] for d in ['lat', 'lon']}
            )

            out = compute_properties(dscen, dref, ref_period, fut_period)

            out_path = CONFIG['paths']['checkups'].format(
                region=region, simulation=simulation, scenario=scenario, step='scen'
            )
            to_zarr_skip_existing(out, out_path, mode, itervar=True)

    if we_should_do('checkup'):
        with (
            Client(n_workers=6, threads_per_worker=3, memory_limit="10GB", **daskkws),
            performance_report(dask_perf_file.with_name(f'perf_report_checkup.html')),
            measure_time(name=f'checkup', logger=logger)
        ):
            import matplotlib.pyplot as plt

            ref = xr.open_zarr(rdir / f"ref_{region}_properties.zarr").load()
            sim = _maybe_unstack(xr.open_zarr(Path(CONFIG['paths']['checkups'].format(step='sim', **fmtkws)))).load()
            scen = xr.open_zarr(CONFIG['paths']['checkups'].format(step='scen', **fmtkws)).load()

            fig_dir = Path(CONFIG['paths']['checkfigs'].format(**fmtkws))
            fig_dir.mkdir(exist_ok=True, parents=True)
            paths = []

            # NaN count
            fig_compare_and_diff(
                sim.nan_count.rename('sim'),
                scen.nan_count.rename('scen'),
                title='Comparing NaN counts.'
            ).savefig(fig_dir / 'Nan_count.png')
            paths.append(fig_dir / 'Nan_count.png')

            # Extremes - between fut and hist
            fig_compare_and_diff(
                scen.pr_wet_freq_q99_hist.rename('historical'),
                scen.pr_wet_freq_q99_fut.rename('future'),
                title='Comparing frequency of extremes future vs present.'
            ).savefig(fig_dir / 'Extremes_pr_scen_hist-fut.png')
            paths.append(fig_dir / 'Extremes_pr_scen_hist-fut.png')

            for var in ['pr_wet_freq_q99_hist', 'tx_mean_rmse', 'tn_mean_rmse', 'prcptot_rmse']:
                fig_bias_compare_and_diff(
                    ref[var], sim[var], scen[var],
                ).savefig(fig_dir / f'{var}_bias_compare.png')
                paths.append(fig_dir / f'{var}_bias_compare.png')

            send_mail(
                subject=f"{simulation}_{scenario}/{region} - Succès",
                msg=f"Toutes les étapes demandées pour la simulation {simulation}_{scenario}/{region} ont été accomplies.",
                attachments=paths
            )
            plt.close('all')
