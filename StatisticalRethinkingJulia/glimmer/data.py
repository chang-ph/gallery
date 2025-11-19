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
    x = np.repeat(-1, 9).tolist()
    x.extend(np.repeat(1, 11).tolist())
    y = np.repeat(0, 10).tolist()
    y.extend(np.repeat(1, 10).tolist())
    return (x, y)

flow = current_workflow()
flow.parse_data(interpret_data)
