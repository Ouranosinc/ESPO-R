"""
Diagnostic properties calculator.

This file requires xscen >= 0.3.4 and xclim >= 0.38.
"""
import atexit
import logging
import dask
from dask.distributed import Client
from pathlib import Path
from xscen.catalog import ProjectCatalog
from xscen.config import CONFIG, load_config
from xscen.diagnostics import properties_and_measures
from xscen.extract import search_data_catalogs, extract_dataset
from xscen.indicators import load_xclim_module
from xscen.io import save_to_zarr
from xscen.regrid import regrid_dataset
from xscen.scripting import send_mail, send_mail_on_exit, timeout, TimeoutException
logger = logging.getLogger('workflow')


def has_been_done(cat, kind, N, **crit):
    ans = False
    props = cat.search(processing_level=f"prop-{kind}", **crit)
    if len(props) > 1:
        raise ValueError(f'Woups, got more than one dataset for props ({kind}, {crit})!')
    elif len(props) == 1 and len(props.df.variable[0]) == N:
        ans = True

    if kind != 'ref':
        meas = cat.search(processing_level=f"meas-{kind}", **crit)
        if len(meas) > 1:
            raise ValueError(f'Woups, got more than one dataset for measures ({kind}, {crit})!')
        elif len(meas) == 0 or len(meas.df.variable[0]) != N:
            ans = False
    return ans


if __name__ == '__main__':
    load_config('prop_conf.yml')
    dask.config.set({k: v for k, v in CONFIG['dask'].items() if k not in ['local', 'client']})
    # dask.config.set(**CONFIG['dask']['local'])
    region = [reg for reg in CONFIG['regions'] if reg['name'] == CONFIG['properties']['region']][0]

    atexit.register(send_mail_on_exit)

    # Get all indicators, except the base ones.
    mod = [
        ind
        for iden, ind in load_xclim_module(CONFIG['properties']['module']).iter_indicators()
        if iden not in {'first_eof', 'correlogram'}
    ]
    N = len(mod)

    # List all members
    cat = ProjectCatalog('../project.json')
    members = cat.search(processing_level='biasadjusted').df.drop_duplicates('id')

    # Open the reference.
    if has_been_done(cat, 'ref', N, domain=region['name'], id='ERA5-land_AMNO'):
        logger.info(f'Already computed properties for ERA5-land_AMNO. Opening.')
        _, props_ref = cat.search(processing_level='prop-ref', domain=region['name'], id='ERA5-land_AMNO').to_dataset_dict().popitem()
    else:
        with Client(**CONFIG['dask']['client']) as client:
            with timeout(7200):
                path = Path(CONFIG['paths']['properties']) / CONFIG['properties']['ref_filename'].format(domain=region['name'])
                logger.info(f'Computing properties for {path.stem}')
                ref_ct = search_data_catalogs(**CONFIG['extract']['ref']).popitem()[1]
                dref = extract_dataset(ref_ct, region=region)['D']
                print()
                props_ref, _ = properties_and_measures(dref.chunk({'time': -1}), mod, to_level_prop='prop-ref')
                save_to_zarr(props_ref, path, rechunk={'lon': -1, 'lat': -1, 'distance': -1}, mode='a')
                cat.update_from_ds(props_ref, path=path)

    for i, member in members.iterrows():
        crit = member[['driving_institution', 'driving_model', 'institution', 'source', 'experiment']].to_dict()
        if crit['source'] == 'ERA5-land':
            continue

        path_sim = Path(CONFIG['paths']['properties']) / CONFIG['properties']['filename'].format(level='sim', domain=region['name'], kind='properties', **crit)
        did_scen = has_been_done(cat, 'scen', N, domain=region['name'], id=member['id'])
        did_sim = has_been_done(cat, 'sim', N, domain=region['name'], id=member['id'])

        if not did_scen or not did_sim:
            scen_ct = search_data_catalogs(
                other_search_criteria={'processing_level': 'biasadjusted', **crit}, **CONFIG['extract']['scen']
            ).popitem()[1]
            dscen = extract_dataset(scen_ct, region=region)['D']
            print()

        # Open scen
        if did_scen:
            logger.info(f"Already computed properties and measures for scen {member['id']}")
        else:
            logger.info(f"Computing properties and measures for scen {member['id']}")
            props, meas = properties_and_measures(
                dscen.chunk({'time': -1}),
                mod,
                dref_for_measure=props_ref,
                to_level_prop='prop-scen',
                to_level_meas='meas-scen'
            )
            path_props = Path(CONFIG['paths']['properties']) / CONFIG['properties']['filename'].format(level='scen', domain=region['name'], kind='properties', **crit)
            path_meas = Path(CONFIG['paths']['properties']) / CONFIG['properties']['filename'].format(level='scen', domain=region['name'], kind='measures', **crit)

            with Client(**CONFIG['dask']['client']) as client:
                try:
                    with timeout(3600, 'prop-meas-scen'):
                        pdel = save_to_zarr(props, path_props, compute=False, rechunk={'lon': -1, 'lat': -1, 'distance': -1}, mode='a'),
                        mdel = save_to_zarr(meas, path_meas, compute=False, rechunk={'lon': -1, 'lat': -1, 'distance': -1}, mode='a')
                        dask.compute(pdel, mdel)
                except TimeoutException as err:
                    send_mail(subject="Scen diags timed out", msg=f"Diagnostics of {member['id']} (scen) timed out with {err}")
                else:
                    cat.update_from_ds(props, path=path_props)
                    cat.update_from_ds(meas, path=path_meas)

        # Open sim
        if did_sim:
            logger.info(f"Already computed properties for sim {member['id']}")
        else:
            logger.info(f"Computing properties for sim {member['id']}")
            # We need to pad the different catalog scheme
            cordex_crit = crit.copy()
            inst = cordex_crit.pop('driving_institution')
            cordex_crit['driving_model'] = f"{inst}-{crit['driving_model']}"
            if 'GEMatm' in crit['driving_model']:
                cordex_crit['driving_model'] += '-ESMsea'
            cordex_crit['source'] = f"{crit['institution']}-{crit['source']}"
            try:
                sim_ct = search_data_catalogs(other_search_criteria=cordex_crit, **CONFIG['extract']['sim']).popitem()[1]
            except ValueError as err:
                logger.warning(f"Got {err}.")
                sim_ct = search_data_catalogs(other_search_criteria=crit, **CONFIG['extract']['sim-mrcc5']).popitem()[1]
            dsim_raw = extract_dataset(sim_ct, region={'buffer': 3, **region}, xr_open_kwargs={'drop_variables': ['time_bnds']})['D']
            print()
            dsim = regrid_dataset(dsim_raw, '/dev/shm', dscen)

            props, meas = properties_and_measures(
                dsim.chunk({'time': -1}),
                mod,
                dref_for_measure=props_ref,
                to_level_prop='prop-sim',
                to_level_meas='meas-sim'
            )
            path_props = Path(CONFIG['paths']['properties']) / CONFIG['properties']['filename'].format(level='sim', domain=region['name'], kind='properties', **crit)
            path_meas = Path(CONFIG['paths']['properties']) / CONFIG['properties']['filename'].format(level='sim', domain=region['name'], kind='measures', **crit)
            with Client(**CONFIG['dask']['client']) as client:
                try:
                    with timeout(5400, 'prop-meas-sim'):
                        dask.compute(
                            save_to_zarr(
                                props, path_props, compute=False, rechunk={'lon': -1, 'lat': -1, 'distance': -1}, mode='a'
                            ),
                            save_to_zarr(
                                meas, path_meas, compute=False, rechunk={'lon': -1, 'lat': -1, 'distance': -1}, mode='a'
                            )
                        )
                except TimeoutException as err:
                    send_mail(subject="Sim diags timed out", msg=f"Diagnostics of {member['id']} (sim) timed out with {err}")
                else:
                    cat.update_from_ds(props, path=path_props, info_dict={'id': member['id']})
                    cat.update_from_ds(meas, path=path_meas, info_dict={'id': member['id']})

    logger.info('All done.')
