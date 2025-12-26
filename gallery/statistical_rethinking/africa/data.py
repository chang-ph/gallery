# /// script
# dependencies = [
#   "pandas",
#   "numpy",
#   "bokeh",
#   "requests",
#   "ruamel-yaml",
# ]
# ///

import pandas as pd
import numpy as np
from io import StringIO
from Coinfer import current_workflow

def interpret_data(data: bytes):
    df = pd.read_csv(StringIO(data.decode('utf-8')), sep=';')
    df['log_gdp'] = np.log(df['rgdppc_2000'])
    df = df.dropna()
    return [df['log_gdp'].to_list(), df['rugged'].to_list(), df['cont_africa'].to_list()]

flow = current_workflow()
flow.parse_data(interpret_data)
