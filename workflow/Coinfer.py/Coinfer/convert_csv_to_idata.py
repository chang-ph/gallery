import logging
from pathlib import Path
from typing import Any, cast

import arviz as az
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _guess_type(value: str) -> type:
    if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
        return int
    lower_val = value.lower()
    if lower_val in ["true", "false"]:
        return bool
    try:
        float(value)
        return float
    except ValueError:
        pass
    return str


def convert_csv_to_idata(mcmcdata_dir: Path) -> dict[str, az.InferenceData]:
    dataframes: list[pd.DataFrame] = []
    for item in mcmcdata_dir.iterdir():
        if item.suffix != ".csv":
            continue
        logger.debug("%s", item)
        df = pd.read_csv(item, names=['chain_name', 'var_name', 'draw', 'var_value'])  # type: ignore
        dataframes.append(df)
    df = pd.concat(dataframes)
    df['chain_name'] = df['chain_name'].astype(str)  # avoid error when chain name is pure digit
    var_names = df['var_name'].unique()
    logger.debug("%s", var_names)
    chain_groups = df.groupby('chain_name')  # type: ignore
    idatas: dict[str, az.InferenceData] = {}
    for chain_name, chain_df in chain_groups:
        data_dict: dict[str, np.ndarray] = {}
        total_iteration = 0
        for var_name in var_names:
            var_data: pd.DataFrame = cast(pd.DataFrame, chain_df[chain_df['var_name'] == var_name])
            first_value = var_data['var_value'].iloc[0]
            dtype = _guess_type(first_value)

            if dtype is bool:
                values = var_data['var_value'].map({'true': True, 'false': False, 'True': True, 'False': False}).values
            else:
                values = var_data['var_value'].astype(dtype).values
            data_dict[cast(str, var_name)] = cast(np.ndarray, values)
        lenset = set(len(v) for v in data_dict.values())
        if len(lenset) != 1:
            raise ValueError(
                f"values should have the same length: { {name: len(val) for name, val in data_dict.items()} }"
            )
        total_iteration = lenset.pop()

        coords: dict[str, Any] = {'chain': [chain_name], 'draw': np.arange(total_iteration)}
        dims = {var_name: ['chain', 'draw'] for var_name in var_names}
        idata = az.from_dict(  # type: ignore
            posterior=data_dict, coords=coords, dims=dims
        )
        idatas[cast(str, chain_name)] = idata
    return idatas
