import pandas as pd
from pathlib import Path
from dask.diagnostics import ProgressBar
from dask.distributed import Client
from dask import config as dskconf
import xarray as xr
from workflow.config import load_config, CONFIG
from workflow.utils import measure_time


# Load configuration, verbose only for the master process.
load_config('paths.yml', 'my_config.yml', verbose=(__name__ == '__main__'))
dskconf.set(num_workers=12)
ProgressBar().register()

if __name__ == '__main__':
    df = pd.read_csv(CONFIG['paths']['status_list'])
    df = df[~df.concatenated]
    for sim_id in df.simulation:
        with measure_time(f'Contenating {sim_id}.'):
            scenario = sim_id.split('_')[-1]
            simulation = '_'.join(sim_id.split('_')[:-1])

            patched = Path(CONFIG['paths']['output'].format(region='fixnm', simulation=simulation, scenario=scenario)).is_dir()
            if patched:
                # Create a client, because the patched version is too hard to handle for thread-base dask (needs too much memory)
                client = Client(
                    n_workers=1,
                    threads_per_worker=6,
                    memory_limit='11GB',
                    local_directory=Path(CONFIG['dask']['client']['local_directory']) / 'concat',
                    dashboard_address=8785
                )
                dsNM = xr.open_zarr(CONFIG['paths']['output'].format(region='fixnm', simulation=simulation, scenario=scenario))
                dsMS = xr.open_zarr(CONFIG['paths']['output'].format(region='fixms', simulation=simulation, scenario=scenario))
                dsSS = xr.open_zarr(CONFIG['paths']['output'].format(region='fixs', simulation=simulation, scenario=scenario))

            dsN = xr.open_zarr(CONFIG['paths']['output'].format(region='north', simulation=simulation, scenario=scenario))
            dsM = xr.open_zarr(CONFIG['paths']['output'].format(region='middle', simulation=simulation, scenario=scenario))
            dsS = xr.open_zarr(CONFIG['paths']['output'].format(region='south', simulation=simulation, scenario=scenario))

            if patched:
                dsC = xr.concat([dsN, dsNM, dsM, dsMS, dsS, dsSS.isel(lat=slice(None, -1))], 'lat').chunk({'lat': 50})
            else:
                dsC = xr.concat([dsN, dsM, dsS], 'lat')

            dsC.attrs['title'] = f"ESPO-R5 v1.0.0 - {simulation} {scenario}"
            dsC.to_zarr(CONFIG['paths']['concat_output'].format(simulation=simulation, scenario=scenario))

            df = pd.read_csv(CONFIG['paths']['status_list'], index_col='simulation')
            df.loc[sim_id, 'concatenated'] = True
            df.to_csv(CONFIG['paths']['status_list'])

            if patched:
                client.close()

    print('All concatenations done for today.')
