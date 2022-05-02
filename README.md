# ESPO-R : Ensemble de Scénarios Polyvalents d'Ouranos - Régional

## Context
The need to adapt to climate change is present in a growing number of fields, 
leading to an increase in the demand for climate scenarios for often interrelated 
sectors of activity. In order to meet this growing demand and to ensure the 
availability of climate scenarios responding to numerous vulnerability, impact and 
adaptation (VIA) studies, Ouranos is working to create a set of operational 
multipurpose climate scenarios "Ensemble de Scénarios Polyvalents d'Ouranos” (ESPO)
covering the territory of North America at a resolution of 0.1 degree (~9km). This 
operational product actually consists of two related datasets 1) ESPO-R (described 
here) produced from a process of bias adjustment and statistical downscaling of 
regional climate simulations from the Canadian Regional Climate Model of Ouranos, as
well as those available via the CORDEX research program and 2) ESPO-G produced 
following to the same methodology, but from global simulations available via the 
CMIP program.


## Data processing tools
The production and regular update of ESPO-R/G operational datasets represents a challenge
in terms of computational resources. Ouranos has invested a great deal of effort in the
development of powerful tools for this type of data processing via its xclim software 
package https://xclim.readthedocs.io/en/stable/.  Built upon the packages xarray and 
dask, xclim benefits from simple to use parallelization and distributed computing tools 
and can be easily deployed on High Performance Computing (HPC) environments.


## ESPO-R v1.0

### Input data
#### Reference data
ESPO-R v1.0 uses the ERA5-Land reanalysis https://confluence.ecmwf.int/display/CKB/ERA5-Land 
(Muñoz Sabater, J., 2019 & 2021) as its reference (or target) dataset .  ERA5-Land is a re-run
of the land component of the ERA5 climate reanalysis, forced by meteorological fields from 
ERA5 and cover the period 1950 to the 2-3 months before the present. ERA5-Land benefits numerous 
improvements making it more accurate for all types of land applications. In particular, ERA5-Land 
runs at enhanced resolution (9 km vs 31 km in ERA5).

The selection of ERA5-land was carried out ....

| Dataset             | Start year | End year    | Spatial coverage        | Spatial resolution | Temporal resolution |
|---------------------|------------|-------------|-------------------------|--------------------|---------------------|
| ERA5                | 1979       | Present     | global                  | ~32 Km             | 1 h                 |
| **ERA5-Land**       | **1979**   | **Present** | ***global (land only)** | **~9 Km**          | **1 h**             |
| NCEP Reanalysis 2   | 1979       | Present     | global                  | 2.5 x 2.5 degrees  | 6 h                 |
| NCEP CFSR           | 1979       | 2009        | global                  | ~40 Km             | 1 h                 |
| MERRA2              | 1980       | Present     | global                  | ~50 Km             | 3 h                 |
| AgCFSR              | 1979       | 2010        | global (land only)      | ~30 Km             | 1 h                 |
| AgMERRA             | 1979       | 2010        | global (land only)      | ~30 Km             | 1 h                 |
| WFDEI-GEM-CaPa      | 1979       | 2016        | global (land only)      | ~10 Km             | 1 h                 |
| NRCAN Gridded v2017 | 1950       | 2017        | Canada (land only)      | ~10 Km             | 1 day               |

#### Regional climate simulations

#### Methodology


### References
Muñoz Sabater, J., (2019): ERA5-Land hourly data from 1981 to present. Copernicus Climate 
Change Service (C3S) Climate Data Store (CDS). (Accessed on 15-12-2021), 10.24381/cds.e2161bac

Muñoz Sabater, J., (2021): ERA5-Land hourly data from 1950 to 1980. Copernicus Climate 
Change Service (C3S) Climate Data Store (CDS). (Accessed on 15-12-2021), 10.24381/cds.e2161bac