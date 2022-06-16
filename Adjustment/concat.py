import pandas as pd
from pathlib import Path
from dask.diagnostics import ProgressBar
from dask.distributed import Client
from dask import config as dskconf
import sys
import xarray as xr
from workflow.config import load_config, CONFIG
from workflow.utils import measure_time


# Load configuration, verbose only for the master process.
load_config('../paths.yml', '../my_config.yml', verbose=(__name__ == '__main__'))
dskconf.set(num_workers=12)
ProgressBar().register()

if __name__ == '__main__':
    sim_id = sys.argv[-1]
    with measure_time(f'Contenating {sim_id}.'):
        scenario = sim_id.split('_')[-1]
        simulation = '_'.join(sim_id.split('_')[:-1])

        dsN = xr.open_zarr(CONFIG['paths']['output'].format(region='north', simulation=simulation, scenario=scenario))
        dsM = xr.open_zarr(CONFIG['paths']['output'].format(region='middle', simulation=simulation, scenario=scenario))
        dsS = xr.open_zarr(CONFIG['paths']['output'].format(region='south', simulation=simulation, scenario=scenario))

        dsC = xr.concat([dsN, dsM, dsS], 'lat')
        dsC.attrs['title'] = f"ESPO-R5 v1.0.0 - {simulation} {scenario}"
        dsC.to_zarr(CONFIG['paths']['concat_output'].format(simulation=simulation, scenario=scenario))

    print('Done.')
