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

def interpret_data(data):
    df = pd.read_csv(StringIO(data.decode("utf-8")), delimiter=';')
    assert df.shape == (48, 5)  # hide
    df['tank_index'] = range(1, len(df) + 1)
    return (df['density'].to_list(), df['tank_index'].to_list(), df['surv'].to_list())

flow = current_workflow()
flow.parse_data(interpret_data)
