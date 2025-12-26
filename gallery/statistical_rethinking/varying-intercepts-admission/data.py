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

    unique_depts = df['dept'].unique()
    dept_map = {key: idx for idx, key in enumerate(unique_depts, 1)}

    df['male'] = (df['gender'] == 'male').astype(int)

    df['dept_id'] = df['dept'].map(dept_map)

    return (df['applications'].to_list(), df['dept_id'].to_list(), df['male'].to_list(), df['admit'].to_list())

flow = current_workflow()
flow.parse_data(interpret_data)
