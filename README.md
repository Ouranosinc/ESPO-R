# ESPO-R

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
The production and regular update of ESPO-R /G operational datasets represents a challenge
in terms of computational resources. Ouranos has invested a great deal of effort in the
development of powerful tools for this type of data processing via its xclim software 
package https://xclim.readthedocs.io/en/stable/.  Built upon the packages xarray and 
dask, xclim benefits from simple to use parallelization and distributed computing tools 
and can be easily deployed on High Performance Computing (HPC) environments.


## ESPO-R v1.0

