# ESPO-R : Ensemble de Scénarios Polyvalents d'Ouranos - modèles Régionaux du climat

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
development of powerful tools for this type of data processing via its [xclim software 
package](https://xclim.readthedocs.io/en/stable/).  Built upon the packages xarray and 
dask, xclim benefits from simple to use parallelization and distributed computing tools 
and can be easily deployed on High Performance Computing (HPC) environments.


## ESPO-R5 v1.0


### Reference data
ESPO-R5 v1.0 uses the [ERA5-Land reanalysis](https://confluence.ecmwf.int/display/CKB/ERA5-Land)
(Muñoz Sabater, J., 2019 & 2021) as its reference (or target) dataset .  ERA5-Land is a re-run
of the land component of the ERA5 climate reanalysis, forced by meteorological fields from 
ERA5 and cover the period 1950 to the 2-3 months before the present. ERA5-Land benefits numerous 
improvements making it more accurate for all types of land applications. In particular, ERA5-Land 
runs at enhanced resolution (9 km vs 31 km in ERA5).

ERA5-land was retained after an evaluation of multiple candidate datasets (table 1) against observed data for the 
variables of daily maximum and minimum temperatures, and daily total precipitation for the period 1981-2010.  
Observed data for the comparison consisted of Third Generation of Homogenized Daily Temperature for Canada (Vincent et al. 2020), 
as well as Second Generation of Daily Adjusted Precipitation for Canada (Mékis and Vincent. 2011) (AHCCD). To be included in the 
assessment, adjusted station data had to have 25 years of valid data for the period 1981-2010, a valid year requiring 
each month to have no more than 10% missing data.

The evaluation criteria included: 
1) a comparison of the mean annual cycle (figure 1), 
2) an evaluation of the inter-annual seasonal time series (figurea 2a-c), and 
3) a seasonal evaluation of the quantile bias (5, 25 , 50, 75, 95) of the daily 
values between station data and the various candidates (figures 3a-b). 

Summary results of quantitative comparisons (figures 1 to 3) indicate that there is no clear winner for the choice of 
reference dataset, with results varying by season or criteria. As such, ERA5-Land was chosen because it 
generally shows good results while presenting the advantages of an increased spatial and temporal resolution
as well as a temporal coverage up to the present (Table 1).

**Table 1. Summary of reference dataset candidates for ESPO-R v1.0.**

| Dataset             | Start year | End year    | Spatial coverage       | Spatial resolution | Temporal resolution | Reference                               |
|---------------------|------------|-------------|------------------------|--------------------|---------------------|-----------------------------------------| 
| ERA5                | 1979       | Present     | global                 | ~32 Km             | 1 h                 | Hersbach et al. 2018                    |
| **ERA5-Land**       | **1979**   | **Present** | **global (land only)** | **~9 Km**          | **1 h**             | **Muñoz-Sabater, J. et al. 2019, 2021** |
| NCEP Reanalysis 2   | 1979       | Present     | global                 | 2.5 x 2.5 degrees  | 6 h                 | Kanamitsu et al. 2002                   |
| NCEP CFSR           | 1979       | 2009        | global                 | ~40 Km             | 1 h                 | Saha et al. 2010                        |
| MERRA2              | 1980       | Present     | global                 | ~50 Km             | 3 h                 | Gelaro, et al. 2017                     |
| AgCFSR              | 1979       | 2010        | global (land only)     | ~30 Km             | 1 h                 | Ruane et al. 2015                       |
| AgMERRA             | 1979       | 2010        | global (land only)     | ~30 Km             | 1 h                 | Ruane et al. 2015                       |
| WFDEI-GEM-CaPa      | 1979       | 2016        | global (land only)     | ~10 Km             | 1 h                 | Asong et al. 2020                       |
| NRCAN Gridded v2017 | 1950       | 2017        | Canada (land only)     | ~10 Km             | 1 day               | McKenney et al. 2011                    |



![img.png](images/img.png)

**Figure 1.** Summary of assessment of mean annual cycle (1981-2010) between candidate datasets and adjusted station data for daily maximum temperature (left column), daily minimum temperature (middle column) and total precipitation (right column). The figures represent the distribution of mean square (top) and correlation (bottom) error values between stations and gridded data.

![img_1.png](images/img_1.png)
a)
![img_2.png](images/img_2.png)
b)
![img_3.png](images/img_3.png)
c)

**Figure 2.** Summary of evaluation of interannual seasonal time series (1981-2010) between candidate datasets and AHCCD stations for daily maximum temperature (a), daily minimum temperature (b) and daily total precipitation (c) variables ). The figures represent the distribution of mean square (top) and correlation (bottom) error values between stations and gridded data.

![img_4.png](images/img_4.png)
a)
![img.png](images/img_5.png)
b)
![img.png](images/img_6.png)
c)

**Figure 3.** Summary of bias by percentile (1981-2010) between candidate datasets for daily values of maximum temperatures (a), minimum temperatures (b) and total precipitation (c). The comparison was made for the seasons of winter (DJF: 1st column), spring (MAM: 2nd column), summer (JJA: 3rd column) and autumn (SON: 4th column). The results for the compared percentiles (5, 25, 50, 75, and 95) are organized by row in ascending order, starting from the top.


### Regional climate simulations

### Methodology


## References
Asong, Z. E., Elshamy, M. E., Princz, D., Wheater, H. S., Pomeroy, J. W., Pietroniro, A., and Cannon, A.: High-resolution meteorological forcing data for hydrological modelling and climate change impact analysis in the Mackenzie River Basin, Earth Syst. Sci. Data, 12, 629–645, https://doi.org/10.5194/essd-12-629-2020, 2020.

Gelaro R., et al., 2017. The Modern-Era Retrospective Analysis for Research and Applications, Version 2 (MERRA-2). J. Clim., doi: 10.1175/JCLI-D-16-0758.1

Hersbach H., Bell B., Berrisford P., Biavati G., Horányi A., Muñoz Sabater J., Nicolas J., Peubey C., Radu R., Rozum I., Schepers D., Simmons A., Soci C., Dee D., Thépaut J-N. (2018). ERA5 hourly data on single levels from 1979 to present. Copernicus Climate Change Service (C3S) Climate Data Store (CDS). (Accessed on 15-12-2021), 10.24381/cds.adbb2d47.

Kanamitsu, M., et al , 2002: NCEP-DOE AMIP-II Reanalysis (R-2), Bull. Amer. Meteor. Soc., 83, 1631-1643.

McKenney, D.W., M.F. Hutchinson, P. Papadol, K. Lawrence, J. Pedlar, K. Campbell, E. Milewska, R.F. Hopkinson, D. Price, and T. Owen, 2011. Customized Spatial Climate Models for North America. Bull. Amer. Meteor. Soc., 92, 1611-1622, https://doi.org/10.1175/2011BAMS3132.1

Mekis, É and L.A. Vincent, 2011: An overview of the second generation adjusted daily precipitation dataset for trend analysis in Canada. Atmosphere-Ocean 49(2), 163-177 doi:10.1080/07055900.2011.583910

Muñoz Sabater, J., (2019): ERA5-Land hourly data from 1981 to present. Copernicus Climate Change Service (C3S) Climate Data Store (CDS). (Accessed on 15-12-2021), 10.24381/cds.e2161bac

Muñoz Sabater, J., (2021): ERA5-Land hourly data from 1950 to 1980. Copernicus Climate Change Service (C3S) Climate Data Store (CDS). (Accessed on 15-12-2021), 10.24381/cds.e2161bac

Ruane, A.C., R. Goldberg, and J. Chryssanthacopoulos, 2015: AgMIP climate forcing datasets for agricultural modeling: Merged products for gap-filling and historical climate series estimation, Agr. Forest Meteorol., 200, 233-248, doi:10.1016/j.agrformet.2014.09.016

Saha, S., et al. 2010. NCEP Climate Forecast System Reanalysis (CFSR) Selected Hourly Time-Series Products, January 1979 to December 2010. Research Data Archive at the National Center for Atmospheric Research, Computational and Information Systems Laboratory. https://doi.org/10.5065/D6513W89

Vincent, L.A., M.M. Hartwell and X.L. Wang, 2020: A Third Generation of Homogenized Temperature for Trend Analysis and Monitoring Changes in Canada’s Climate. Atmosphere-Ocean. https://doi.org/10.1080/07055900.2020.1765728
