"""
This model is from https://github.com/vectorly-ai/Gallery/tree/main/StatisticalRethinkingJulia/varying-intercepts-reedfrogs.
The data it needs is also in that directory (https://github.com/vectorly-ai/Gallery/blob/main/StatisticalRethinkingJulia/varying-intercepts-reedfrogs/data.csv).
If you don't provide any data in the workflow, that data will be used by default.
If you want to provide data, it should be provided in the same format as the default data and attached to the workflow.
"""

using Pkg
Pkg.develop(; path=ARGS[1])  # load Coinfer.jl

using Coinfer
using Random
using Turing
using StatsFuns: logistic

flow = Coinfer.current_workflow()

@model function reedfrogs(Nᵢ, i, Sᵢ)
    αₜₐₙₖ ~ filldist(Normal(0, 1.5), length(i))
    pᵢ = logistic.(αₜₐₙₖ[i])
    Sᵢ ~ product_distribution(Binomial.(Nᵢ, pᵢ))
end;

flow.model = reedfrogs
