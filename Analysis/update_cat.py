from xscen.catalog import ProjectCatalog, parse_directory
from xscen.config import load_config, CONFIG


if __name__ == '__main__':
    load_config('../my_config.yml', '../paths.yml')
    pcat = ProjectCatalog('../project.json')

    df = parse_directory(
        directories=[CONFIG['indicators']['final_output'], CONFIG['indicators']['outdir']],
        extension="*.zarr",
        patterns=[
            "{driving_institution}_{driving_model}_{institution}_{source}_{experiment}_{xrfreq}_{date_start}-{date_end}.zarr"
        ],
        read_from_file=["variable"],
        homogenous_info={
            "processing_level": "biasadjusted",
            "type": "simulations",
            "bias_adjust_project": "ESPO-R5",
            "bias_adjust_institution": "Ouranos",
            "domain": "AMNO",
            "mip_era": "CMIP5",
            "activity": "CORDEX",
        }
    )
    pcat.update(df)
