import ast
import logging
from typing import Any, List, Mapping, Optional, Sequence, Tuple, Union
from pathlib import Path, PosixPath
import itertools
from copy import deepcopy
import dask
from dask.diagnostics import ProgressBar
import cftime
import pandas as pd
import xarray as xr
import xclim as xc
import json
from intake.source.utils import reverse_format
import intake_esm
from intake_esm.cat import ESMCatalogModel
import subprocess

import os

from .config import parse_config, recursive_update, CONFIG
from .io import get_engine
logger = logging.getLogger('workflow')

COLUMNS = [ # enlever les _id
    "simulation_id",
    "project_id", # ajout
    "ensemble_id",
    "model_institution_id",
    "model_id",
    "driver_institution_id",
    "driver_id",
    "experiment_id",
    "timestep_id", # frequency?
    "domain_id",  # Résolution? 
    # "bbox", #?
    "member_id",
    "variable_id",
    "orig_variable", # remove
    "timedelta",
    "date_start",
    "date_end",
    "processing_level",
    "format",
    "path",
]
"""Official column names."""


_esm_col_data = {
    "esmcat_version": "0.1.0",
    "assets": {
        "column_name": "path",
        "format_column_name": "format"
    },
    "aggregation_control": {
        "variable_column_name": "orig_variable",
        "groupby_attrs": ["simulation_id", "domain_id", "processing_level", "timestep_id"],
        "aggregations": [
            {"type": "join_existing", "attribute_name": "date_start", "options": {"dim": "time"}},
            {"type": "union", "attribute_name": "orig_variable"}
        ],
    },
    "attributes": []
}
"""Official ESM column data for the catalogs."""


def parse_list_of_strings(elem):
    """Parse a element of a csv in case it is a list of strings."""
    if elem.startswith('(') or elem.startswith('['):
        out = ast.literal_eval(elem)
        return out
    return elem,


csv_kwargs = {
    "dtype": {
        'ensemble_id': 'category',
        'simulation_id': 'category',
        'driver_institution_id': 'category',
        'driver_id': 'category',
        'member_id': 'category',
        'experiment_id': 'category',
        'timestep_id': 'category',
        'model_institution_id': 'category',
        'model_id': 'category',
        'domain_id': 'category',
        # 'variable_id': 'category',
        # 'orig_variable': 'category',
        'processing_level': 'category',
        'format': 'category',
        'path': 'string[pyarrow]'
    },
    "parse_dates": ['date_start', 'date_end'],
    "converters": {
        'timedelta': pd.to_timedelta,
        'variable_id': parse_list_of_strings,
        'orig_variable': parse_list_of_strings,
    }
}
"""Offical kwargs to pass to `pd.read_csv` when opening an Ouranos catalog."""


def get_cv(key, folder=None):
    if folder is None:
        folder = Path(__file__).parent / "CVs"
    with (Path(folder) / f"{key}.json").open() as f:
        cv = json.load(f)
    return cv


inferfreq_to_timestep = {
    "1H": "1h",
    "3H": "3h",
    "6H": "6h",
    "1D": "day",
    "1W": "sem",
    "1M": "mon",
    "1A": "year",
}


class DataCatalog(intake_esm.esm_datastore):
    """A intake_esm catalog adapted to the workflow's syntax."""

    def __init__(self, *args, **kwargs):
        kwargs['read_csv_kwargs'] = recursive_update(csv_kwargs.copy(), kwargs.get('read_csv_kwargs', {}))
        super().__init__(*args, **kwargs)

    @classmethod
    def from_csv(
        cls,
        paths: Union[os.PathLike, Sequence[os.PathLike]],
        esmdata: Optional[Union[os.PathLike, dict]] = None,
        *,
        read_csv_kwargs: Mapping[str, Any] = None,
        name: str = "virtual",
        **intake_kwargs
    ):
        """Create a DataCatalog from one or more csv files.

        Parameters
        ----------
        paths: paths or sequence of paths
          One or more paths to csv files.
        esmdata: path or dict, optional
          The "ESM collection data" as a path to a json file or a dict.
          If None (default), `catalog._esm_col_data` is used.
        read_csv_kwargs : dict, optional
          Extra kwargs to pass to `pd.read_csv`, in addition to the ones in `catalog.csv_kwargs`.
        name: str, optional
          If `metadata` doesn't contain it, a name to give to the catalog.
        """
        if isinstance(paths, os.PathLike):
            paths = [paths]

        if isinstance(esmdata, os.PathLike):
            with open(esmdata) as f:
                esmdata = json.load(f)
        elif esmdata is None:
            esmdata = deepcopy(_esm_col_data)
        if 'id' not in esmdata:
            esmdata['id'] = name

        read_csv_kwargs = recursive_update(csv_kwargs.copy(), read_csv_kwargs or {})

        df = pd.concat(
            [pd.read_csv(p, **read_csv_kwargs) for p in paths]
        ).reset_index(drop=True)

        # Create the intake catalog
        return cls({'esmcat': esmdata, 'df': df}, **intake_kwargs)

    def __dir__(self) -> List[str]:
        rv = [
            'iter_unique',
            'drop_duplicates',
            'check_valid'
        ]
        return super().__dir__() + rv

    def unique(self, columns: Union[str, list] = None):
        """
        Simpler way to get unique elements from a column in the catalog.
        """
        if self.df.size == 0:
            raise ValueError('Catalog is empty.')
        out = super().unique()
        if columns is not None:
            out = out[columns]
        return out

    def iter_unique(self, *columns):
        """Iterate over sub-catalogs for each group of unique values for all specified columns.

        This is a generator that yields a tuple of the unique values of the current
        group, in the same order as the arguments, and the sub-catalog.
        """
        for values in itertools.product(*map(self.unique, columns)):
            sim = self.search(**dict(zip(columns, values)))
            if sim:  # So we never yield empty catalogs
                yield values, sim

    def drop_duplicates(self, columns=['path']):
        # Drop duplicates
        self.esmcat.df.drop_duplicates(
            subset=columns,
            keep="last",
            ignore_index=True,
            inplace=True
        )

    def check_valid(self):
        # In case files were deleted manually, double-check that files do exist
        def check_existing(row):
            path = Path(row.path)
            exists = (
                (path.is_dir() and path.suffix == '.zarr')
                or (path.is_file() and path.suffix == '.nc')
            )
            if not exists:
                logger.info(f'File {path} was not found on disk, removing from catalog.')
            return exists

        self.esmcat._df = self.df[self.df.apply(check_existing, axis=1)].reset_index(drop=True)


class ProjectCatalog(DataCatalog):
    """A DataCatalog that can upload itself even from subcatalogs."""

    @classmethod
    def create(cls, filename: os.PathLike, *, project: Optional[dict] = None):
        """Create a new project catalog from some project metadata.

        Creates the json from default `_esmcol_data`and an empty csv file.

        Parameters
        ----------
        filename : PathLike
          A path to the json file (with or without suffix).
        project : dict-like
          Metadata to create the catalog. If None, `CONFIG['project']` will be used.
          Valid fields are:

          - name : Name of the project, given as the catalog's "title".
          - id : slug-like version of the name, given as the catalog's id (should be url-proof)
                 Defaults to a modified name.
          - version : Version of the project (and thus the catalog), string like "x.y.z".
          - description : Detailed description of the project, given to the catalog's "description".

          At least one of `id` and `name` must be given, the rest is optional.

        Returns
        -------
        ProjectCatalog
          An empty intake_esm catalog.
        """
        path = Path(filename)
        meta_path = path.with_suffix('.json')
        data_path = path.with_suffix('.csv')

        if meta_path.is_file() or data_path.is_file():
            raise os.FileExistsError(
                'Catalog file already exist (at least one of {meta_path} or {data_path}).'
            )

        meta_path.parent.mkdir(parents=True, exist_ok=True)

        project = project or CONFIG.get('project') or {}

        if 'id' not in project and 'name' not in project:
            raise ValueError('At least one of "id" or "name" must be given in the metadata.')

        esmdata = {
            "catalog_file": str(data_path),
            "id": project.get('id', project.get('name', '').replace(' ', '')),
        }
        if 'name' in project:
            esmdata['title'] = project['name']

        esmdata = recursive_update(_esm_col_data.copy(), esmdata)

        df = pd.DataFrame(columns=COLUMNS)

        cat = cls({'esmcat': esmdata, 'df': df})
        cat.serialize(
            path.stem,
            directory=path.parent,
            catalog_type='file',
            to_csv_kwargs={'compression': None}
        )
        return cls(str(meta_path))

    def __init__(self, df, *args, **kwargs):
        super().__init__(df, *args, **kwargs)
        self.meta_file = df if not isinstance(df, dict) else None

    # TODO: Implement a way to easily destroy part of the catalog to "reset" some steps
    def update(
        self,
        df: Optional[Union["DataCatalog", intake_esm.esm_datastore, pd.DataFrame, pd.Series, Sequence[pd.Series]]] = None
    ):
        """Updates the catalog with new data and writes the new data to the csv file.

        Once the internal dataframe is updated with `df`, the csv on disk is parsed,
        updated with the internal dataframe, duplicates are dropped and everything is
        written back to the csv. This means that nothing is _removed_* from the csv when
        calling this method, and it is safe to use even with a subset of the catalog.

        * If a file was deleted between the parsing of the catalog and this call,
        it will be removed from the csv when `check_valid` is called.

        Parameters
        ----------
        df : Union[pd.DataFrame, pd.Series, DataCatalog]
          Data to be added to the catalog.
        """

        # Append the new DataFrame or Series
        if isinstance(df, DataCatalog) or isinstance(df, intake_esm.esm_datastore):
            self.esmcat._df = self.df.append(df.df)
        elif df is not None:
            self.esmcat._df = self.df.append(df)

        self.check_valid()
        self.drop_duplicates()

        if self.meta_file is not None:
            self.df.to_csv(self.esmcat.catalog_file, index=False, compression=None)
        else:
            # Update the catalog file saved on disk
            disk_cat = DataCatalog({'esmcat': self.esmcat.dict(), 'df': pd.read_csv(self.esmcat.catalog_file, **self.read_csv_kwargs)})
            disk_cat.esmcat._df = disk_cat.df.append(self.df)
            disk_cat.check_valid()
            disk_cat.drop_duplicates()
            disk_cat.df.to_csv(disk_cat.esmcat.catalog_file, index=False, compression=None)

    def refresh(self):
        """
        Re-reads the catalog csv saved on disk.
        """
        if self.meta_file is None:
            raise ValueError('Only full catalogs can be refresed, but this instance is only a subset.')
        self.esmcat = ESMCatalogModel.load(
            self.meta_file,
            read_csv_kwargs=self.read_csv_kwargs
        )
        initlen = len(self.esmcat.df)
        self.check_valid()
        self.drop_duplicates()
        if len(self.df) != initlen:
            self.update()

    def __repr__(self) -> str:
        return (
            f'<{self.esmcat.id or ""} project catalog with {len(self)} dataset(s) from '
            f'{len(self.df)} asset(s) ({"subset" if self.meta_file is None else "full"})>'
        )


def concat_data_catalogs(*dcs):
    """Concatenate a multiple DataCatalogs.

    Output catalog is the union of all rows and all derived variables, with the the "esmcat"
    of the first DataCatalog. Duplicate rows are dropped and the index is reset.
    """
    registry = {}
    catalogs = []
    for dc in dcs:
        registry.update(dc.derivedcat._registry)
        catalogs.append(dc.df)
    df = pd.concat(catalogs, axis=0).drop_duplicates(ignore_index=True)
    dvr = intake_esm.DerivedVariableRegistry()
    dvr._registry.update(registry)

    return DataCatalog({'esmcat': dcs[0].esmcat, 'df': df}, registry=dvr)


@parse_config
def get_asset_list(root_paths, extension="*.nc"):
    """List files with a given extension from a list of paths.

    Search is done with GNU's `find` and parallized through `dask`.
    """

    @dask.delayed
    def _file_dir_files(directory, extension):
        try:
            cmd = ["find", "-L", directory.as_posix(), "-name", extension]
            proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            output = proc.stdout.read().decode("utf-8").split()
        except Exception:
            output = []
        return output

    filelist = list()
    for r in root_paths:
        root = Path(r)
        pattern = "*/"

        dirs = [x for x in root.glob(pattern) if x.is_dir() and x.suffix != '.zarr']

        filelistroot = [_file_dir_files(directory, extension) for directory in dirs]
        # watch progress
        with ProgressBar():
            filelistroot = dask.compute(*filelistroot)
        filelist.extend(list(itertools.chain(*filelistroot)))

        # add files in the first directory
        filelist.extend([str(x) for x in root.glob(f"**/{extension}")])

    return sorted(filelist)


def name_parser(path, patterns, read_from_file=None):
    """Extract metadata information from the file path.

    Parameters
    ----------
    path : str
      Full file path.
    patterns : list or str
      List of patterns to try in `reverse_format`
    read_from_file : list of string, optional
      A list of columns to parse from the file's metadata itself.
    """
    path = Path(path)

    d = {}
    for pattern in patterns:
        folder_depth = len(Path(pattern).parts) - 1
        # stem is a path with the same number of parts as the pattern
        stem = str(Path(*path.parts[-1 - folder_depth:]))
        try:
            d = reverse_format(pattern, stem)
            if d:
                break
        except ValueError:
            continue
    if not d:
        logger.warn(f"No pattern matched with path {path}..")
    else:
        logger.debug(f"Parsed file path {path} and got {len(d)} fields.")

    # files with a single year/month
    if ("date_end" not in d.keys()) and ("date_start" in d.keys()):
        d["date_end"] = d["date_start"]

    d["path"] = path
    d["format"] = path.suffix[1:]

    if read_from_file:
        missing = set(read_from_file) - d.keys()
        if missing:
            try:
                fromfile = parse_from_ds(path, names=missing)
            except Exception as err:
                logger.error(f'Unable to parse file {path}, got : {err}')
            finally:
                d.update(fromfile)
                logger.debug(f"Parsed file data and got {len(missing.intersection(d.keys()))} more fields.")
    # strip to clean off lost spaces and line jumps
    return {k: v.strip() if isinstance(v, str) else v for k, v in d.items()}


@parse_config
def parse_directory(
    directories: list,
    extension: str,
    patterns: list,
    *,
    simulation_id_columns: list = ["ensemble_id", "driver_institution_id", "driver_id",
                                   "model_institution_id", "model_id",
                                   "domain_id", "member_id", "experiment_id"],
    read_from_file: Union[bool, Sequence[str], Tuple[Sequence[str], Sequence[str]]] = False,
    homogenous_info: dict = None,
    cvs_dir: Union[str, PosixPath] = None,
    xr_open_kwargs: Mapping[str, Any] = None
) -> pd.DataFrame:
    """
    Parse files in a directory and return them as a pd.DataFrame.

    Parameters
    ----------
    directories : list
        List of directories to parse. The parse is recursive.
    extension: str
        A glob pattern for file name matching, usually only a suffix like "*.nc".
    patterns : list
        List of possible patterns to be used by intake.source.utils.reverse_filename() to decode the file names. See Notes below.
    simulation_id_columns : list
        List of column names on which to base simulation_id.
    read_from_file : boolean or set of strings or tuple of 2 sets of strings.
        If True, if some fields were not parsed from their path, files are opened and
        missing fields are parsed from their metadata, if found.
        If a set of column names, only those fields are parsed from the file, if missing.
        If False (default), files are never opened.
        If a tuple of 2 lists of strings, only the first file of groups defined by the
        first list of columns is read and the second list of columns is parsed from the
        file and applied to the whole group.
    homogenous_info : dict, optional
        Using the {column_name: description} format, information to apply to all files.
    cvs_dir: Union[str, PosixPath], optional
        Directory where JSON controlled vocabulary files are located. See Notes below.

    Notes
    -----
    - Columns names are: ["ensemble_id", "driver_institution_id", "driver_id", "member_id", "experiment_id", "timestep_id", "model_institution_id",
                          "model_id", "domain_id", "variable_id", "date_start", "date_end", "processing_level"]
    - Not all column names have to be present, but "timestep_id", "variable_id", "date_start", "date_end" & "processing_level" are necessary.
    - 'patterns' should highlight the columns with braces.
        - A wildcard can be used for irrelevant parts of a filename.
        - Example: "{*}_{*}_{domain_id}_{*}_{variable_id}_{date_start}_{ensemble_id}_{experiment_id}_{processing_level}_{*}.nc"
    - JSON files for the controlled vocabulary must have the same name as the column. One file per column.

    Returns
    -------
    pd.DataFrame
      Parsed directory files

    """
    homogenous_info = homogenous_info or {}
    columns = set(COLUMNS) - homogenous_info.keys()
    first_file_only = None  # The set of columns defining groups for which read the first file.
    if not isinstance(read_from_file, bool) and not isinstance(read_from_file[0], str):
        # A tuple of 2 lists
        first_file_only, read_from_file = read_from_file
    if read_from_file is True:
        # True but not a list of strings
        read_from_file = columns
    elif read_from_file is False:
        read_from_file = set()

    filelist = get_asset_list(directories, extension=extension)
    logger.info(f"Found {len(filelist)} files to parse.")

    def _update_dict(entry):
        z = {k: entry.get(k) for k in columns}
        return z

    @dask.delayed
    def delayed_parser(*args, **kwargs):
        return _update_dict(name_parser(*args, **kwargs))

    parsed = [delayed_parser(x, patterns, read_from_file=read_from_file if first_file_only is None else []) for x in filelist]
    with ProgressBar():
        parsed = dask.compute(*parsed)
    df = pd.DataFrame(parsed)

    def read_first_file(grp, cols, xrkwargs):
        fromfile = parse_from_ds(grp.path.iloc[0], cols, **xrkwargs)
        logger.info(f"Got {len(fromfile)} fields, applying to {len(grp)} entries.")
        out = grp.copy()
        for col, val in fromfile.items():
            for i in grp.index:  # If val is an iterable we can't use loc.
                out.at[i, col] = val
        return out

    if first_file_only is not None:
        df = df.groupby(first_file_only).apply(read_first_file, cols=read_from_file, xrkwargs=(xr_open_kwargs or {})).reset_index(drop=True)

    if df.shape[0] == 0:
        raise FileNotFoundError("No files found while parsing.")

    # add homogeous info
    for key, val in homogenous_info.items():
        df[key] = val

    # Fill missing orig_variable with variable_id.
    df["orig_variable"].fillna(df["variable_id"], inplace=True)

    # Replace DataFrame entries by definitions found in CV
    if cvs_dir is not None:
        # Read all CVs and replace values in catalog accordingly
        # TODO: In theory, CVs could also be contained within a single file
        # TODO: Je suggère 1 yaml par ensemble.
        cvs = {
            key: get_cv(key, cvs_dir)
            for key in COLUMNS
            if (Path(cvs_dir) / f"{key}.json").is_file()
        }
        df = df.replace(cvs)

    # Parse dates
    df['date_start'] = df['date_start'].apply(date_parser)
    df['date_end'] = df['date_end'].apply(date_parser, end_of_period=True)

    # translate timestep_id into timedeltas
    df['timedelta'].fillna(
        df["timestep_id"].replace(get_cv("timestep_to_timedelta")),
        inplace=True
    )
    df["timedelta"] = pd.to_timedelta(df["timedelta"])

    # Create simulation_id from user specifications
    df["simulation_id"] = df[simulation_id_columns].apply(
        lambda row: '_'.join(map(str, filter(pd.notna, row.values))),
        axis=1
    )
    # Sort columns and return
    return df.loc[:, COLUMNS]


def parse_from_ds(obj: Union[os.PathLike, xr.Dataset], names: Sequence[str], **xrkwargs):
    """Parse a list of catalog fields from the file/dataset itself.

    If passed a path, this opens the file.

    Infers the variable_id from the variables.
    Infers timestep_id, timedelta, date_start and date_end from the time coordinate if present.
    Infers other attributes from the coordinates or the global attributes.
    """
    attrs = {}
    if not isinstance(obj, xr.Dataset):
        ds = xr.open_dataset(obj, engine=get_engine(obj), **xrkwargs)
        logger.info(f'Parsing attributes from file {obj}.')
    else:
        ds = obj
        logger.info(f'Parsing attributes from dataset.')

    for name in names:
        if name == "variable_id":
            attrs["variable_id"] = tuple(ds.data_vars.keys())
        elif name == "orig_variable":
            attrs["orig_variable"] = tuple(ds.data_vars.keys())
        elif (
            name in ("timedelta", "timestep_id")
            and name not in attrs
            and 'time' in ds.coords
            and ds.time.size > 3
        ):
            # round to the minute to catch floating point imprecision
            freq = xr.infer_freq(ds.time.dt.round('T'))
            if freq:
                if "timedelta" in names:
                    attrs["timedelta"] = ds.time[1].item() - ds.time[0].item()
                if "timestep_id" in names:
                    m, b, _, _ = xc.core.calendar.parse_offset(freq)
                    offset_str = f"{m:d}{b}"
                    if offset_str in inferfreq_to_timestep:
                        attrs["timestep_id"] = inferfreq_to_timestep[offset_str]
        elif name == "date_start" and "time" in ds.coords:
            attrs["date_start"] = ds.indexes['time'][0]
        elif name == "date_end" and "time" in ds.coords:
            attrs["date_end"] = ds.indexes['time'][-1]
        elif name in ds.coords:
            attrs[name] = tuple(ds.coords[name].values)
        elif name in ds.attrs:
            attrs[name] = ds.attrs[name].strip()

    logger.debug(f'Got fields {attrs.keys()} from file.')
    return attrs


def date_parser(
    date,
    *,
    end_of_period: Optional[bool] = False,
    out_dtype: Optional[str] = "datetime",
    strtime_format: Optional[str] = "%Y-%m-%d"
) -> Union[str, pd.Timestamp]:
    """
    Returns a datetime from a string

    Parameters
    ----------
    date : str
      Date to be converted
    end_of_period : bool, optional
      If True, the date will be the end of month or year depending on what's most appropriate
    out_dtype: str, optional
      Choices are 'datetime' or 'str'
    strtime_format: str, optional
      If out_dtype=='str', this sets the strftime format

    Returns
    -------
    pd.Timestamp, str
      Parsed date

    """

    # Formats, ordered depending on string length
    fmts = {
        4: ["%Y"], 6: ["%Y%m"], 7: ["%Y-%m"], 8: ["%Y%m%d"],
        10: ["%Y%m%d%H", "%Y-%m-%d"], 12: ["%Y%m%d%H%M"], 19: ["%Y-%m-%dT%H:%M:%S"]
    }

    def _parse_date(date, fmts):
        for fmt in fmts:
            try:
                s = pd.to_datetime(date, format=fmt)
                match = fmt
                break
            except Exception:
                pass
        else:
            raise ValueError(f"Can't parse date {date} with formats {fmts}.")
        return s, match

    fmt = None
    if isinstance(date, str):
        date, fmt = _parse_date(date, fmts[len(date)])
    elif isinstance(date, cftime.datetime):
        for n in range(3):
            try:
                date = pd.Timestamp.fromisoformat((date - pd.Timedelta(n)).isoformat())
            except ValueError:  # We are NOT catching OutOfBoundsDatetime.
                pass
            else:
                break
        else:
            raise ValueError("Unable to parse cftime date {date}, even when moving back 2 days.")
    elif not isinstance(date, pd.Timestamp):
        date = pd.Timestamp(date)

    if end_of_period and fmt:
        if "m" not in fmt:
            date = date + pd.tseries.offsets.YearEnd(1)
        elif "d" not in fmt:
            date = date + pd.tseries.offsets.MonthEnd(1)
        # TODO: Implement subdaily ?

    if out_dtype == "str":
        return date.strftime(strtime_format)

    return date
