
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
    return [9, 6]

flow = current_workflow()
flow.parse_data(interpret_data)
