"""
This model is from https://github.com/vectorly-ai/Gallery/tree/main/StatisticalRethinkingJulia/admit-reject.
The data it needs is also in that directory (https://github.com/vectorly-ai/Gallery/blob/main/StatisticalRethinkingJulia/admit-reject/data.csv).
If you don't provide any data in the workflow, that data will be used by default.
If you want to provide data, it should be provided in the same format as the default data and attached to the workflow.
"""

using Pkg
Pkg.develop(; path=ARGS[1])  # load Coinfer.jl

using Turing
using Coinfer

flow = Coinfer.current_workflow()

@model function m_pois(admit, reject)
    α₁ ~ Normal(0, 100)
    α₂ ~ Normal(0, 100)

    for i in 1:length(admit)
        λₐ = exp(α₁)
        λᵣ = exp(α₂)
        admit[i] ~ Poisson(λₐ)
        reject[i] ~ Poisson(λᵣ)
    end
end;

flow.model = m_pois
