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
    df['log_pop'] = np.log(df['population'])
    df['society'] = range(1, len(df) + 1)
    return (df['total_tools'].to_list(), df['log_pop'].to_list(), df['society'].to_list())

flow = current_workflow()
flow.parse_data(interpret_data)
