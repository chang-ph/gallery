"""
This model is from https://github.com/vectorly-ai/Gallery/tree/main/StatisticalRethinkingJulia/centered-oceanic-tool-complexity.
The data it needs is also in that directory (https://github.com/vectorly-ai/Gallery/blob/main/StatisticalRethinkingJulia/centered-oceanic-tool-complexity/data.csv).
If you don't provide any data in the workflow, that data will be used by default.
If you want to provide data, it should be provided in the same format as the default data and attached to the workflow.
"""

# ## Data

using Pkg
Pkg.develop(; path=ARGS[1])  # load Coinfer.jl

using Turing
using Coinfer
using Statistics: mean

flow = Coinfer.current_workflow()

@model function m10_10stan_c(total_tools, log_pop_c, contact_high)
    α ~ Normal(0, 100)
    βp ~ Normal(0, 1)
    βc ~ Normal(0, 1)
    βpc ~ Normal(0, 1)

    for i in 1:length(total_tools)
        λ = exp(α + βp*log_pop_c[i] + βc*contact_high[i] + βpc*contact_high[i]*log_pop_c[i])
        total_tools[i] ~ Poisson(λ)
    end
end;

flow.model = m10_10stan_c
