from pathlib import Path
import pandas as pd
from xscen.catalog import ProjectCatalog, parse_directory
from xscen.config import load_config


if __name__ == '__main__':
    load_config('../my_config.yml', '../paths.yml')
    if Path('../project.json').is_file():
        pcat = ProjectCatalog('../project.json')
    else:
        pcat = ProjectCatalog.create('../project.json')

    scendf = parse_directory(
        directories=['/tank/smith/espo-r5_temp/'],
        patterns=['{frequency}_{driving_institution}_{driving_model}_{institution}_{source}_{experiment}_{date_start}-{date_end}.zarr'],
        globpattern="*.zarr",
        read_from_file=['variable'],
        homogenous_info={
            'bias_adjust_institution': 'Ouranos',
            'bias_adjust_project': 'ESPO-R5',
            'activity': 'CORDEX',
            'mip_era': 'CMIP5',
            'type': 'simulation',
            'processing_level': 'biasadjusted',
            'domain': 'AMNO',
            'version': '1.0'
        }
    )

    propsdf = parse_directory(
        directories=['/tank/smith/espo-r5_temp/properties/'],
        patterns=[
            '{driving_institution}_{driving_model}_{institution}_{source}_{experiment}_{domain}_{?processing_level}_{?kind}.zarr',
            '{source}_{domain}_{?processing_level}_{?kind}.zarr',
        ],
        globpattern="*.zarr",
        read_from_file=['variable', 'processing_level'],
        homogenous_info={
            'bias_adjust_institution': 'Ouranos',
            'bias_adjust_project': 'ESPO-R5',
            'activity': 'CORDEX',
            'mip_era': 'CMIP5',
            'type': 'simulation',
            'frequency': 'fx',
            'version': '1.0',
        }
    )
    # propsdf.processing_level.replace({'sim': 'sim-props', 'scen': 'scen-prop', 'ref': 'ref-properties'})

    inddf = parse_directory(
        directories=['/tank/smith/espo-r5_temp/indices/', '/exec/pbourg/ESPO-R5/'],
        patterns=['{driving_institution}_{driving_model}_{institution}_{source}_{experiment}_{xrfreq}_{date_start}-{date_end}.zarr'],
        globpattern="*.zarr",
        read_from_file=['variable'],
        homogenous_info={
            'bias_adjust_institution': 'Ouranos',
            'bias_adjust_project': 'ESPO-R5',
            'activity': 'CORDEX',
            'mip_era': 'CMIP5',
            'type': 'simulation',
            'domain': 'AMNO',
            'processing_level': 'derived',
            'version': '1.0',
        }
    )

    df = pd.concat([propsdf, inddf])
    df = pd.concat([df, scendf[~scendf.path.isin(df.path)]])

    pcat.update(df)
