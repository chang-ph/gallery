"""
This model is from https://github.com/vectorly-ai/Gallery/tree/main/StatisticalRethinkingJulia/multilevel-reedfrogs.
The data it needs is also in that directory (https://github.com/vectorly-ai/Gallery/blob/main/StatisticalRethinkingJulia/multilevel-reedfrogs/data.csv).
If you don't provide any data in the workflow, that data will be used by default.
If you want to provide data, it should be provided in the same format as the default data and attached to the workflow.
"""

using Pkg
Pkg.develop(; path=ARGS[1])  # load Coinfer.jl

using Turing
using Coinfer

flow = Coinfer.current_workflow()

@model function m12_2(density, tank, surv)
    σ ~ truncated(Cauchy(0, 1), 0, Inf)
    α ~ Normal(0, 1)

    N_tank = length(tank)
    α_tank ~ filldist(Normal(α, σ), N_tank)

    logitp = α_tank[tank]
    surv ~ product_distribution(BinomialLogit.(density, logitp))
end;

flow.model = m12_2
