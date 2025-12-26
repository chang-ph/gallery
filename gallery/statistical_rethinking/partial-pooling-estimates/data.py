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
    μ = 1.4
    σ = 1.5
    nponds = 60
    ni = np.repeat([5, 10, 25, 35], 15)

    a_pond = np.random.normal(μ, σ, nponds)

    dsim = pd.DataFrame({
        'pond': range(1, nponds + 1),
        'ni': ni,
        'true_a': a_pond
    })

    prob = 1 / (1 + np.exp(-dsim['true_a']))

    dsim['s'] = [np.random.binomial(int(ni[i]), prob[i]) for i in range(nponds)]

    dsim['p_nopool'] = dsim['s'] / dsim['ni']

    return (dsim['pond'].to_list(), dsim['s'].to_list(), dsim['ni'].to_list())

flow = current_workflow()
flow.parse_data(interpret_data)
