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
    df['contact_high'] = np.where(df['contact'] == "high", 1, 0)

    # New col where we center(!) the log_pop values
    mean_log_pop = df['log_pop'].mean()
    df['log_pop_c'] = df['log_pop'] - mean_log_pop
    return [df['total_tools'].to_list(), df['log_pop_c'].to_list(), df['contact_high'].to_list()]

flow = current_workflow()
flow.parse_data(interpret_data)
