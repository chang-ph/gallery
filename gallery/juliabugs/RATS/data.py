
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
    df = pd.read_csv(StringIO(data.decode("utf-8")))
    
    # Convert to matrix format (N rats x T time points)
    Y = df.values
    
    # Time points: 8, 15, 22, 29, 36 days
    x = [8.0, 15.0, 22.0, 29.0, 36.0]
    xbar = 22.0
    
    N = Y.shape[0]  # Number of rats (30)
    T = Y.shape[1]  # Number of time points (5)
    
    return [
        x,
        xbar,
        N,
        T,
        Y.tolist(),
    ]
    # return {
    #     'x': x,
    #     'xbar': xbar,
    #     'N': N,
    #     'T': T,
    #     'Y': Y.tolist(),
    # }

flow = current_workflow()
flow.parse_data(interpret_data)
