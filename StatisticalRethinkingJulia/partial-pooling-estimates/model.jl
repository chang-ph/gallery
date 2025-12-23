"""
This model is from https://github.com/vectorly-ai/Gallery/tree/main/StatisticalRethinkingJulia/partial-pooling-estimates
"""

using Pkg
Pkg.develop(; path=ARGS[1])  # load Coinfer.jl

using Turing
using Coinfer
using StatsFuns: logistic

flow = Coinfer.current_workflow()

@model function m12_3(pond, s, ni)
    σ ~ truncated(Cauchy(0, 1), 0, Inf)
    α ~ Normal(0, 1)

    N_ponds = length(pond)

    α_pond ~ filldist(Normal(α, σ), N_ponds)

    logitp = α_pond[pond]
    s ~ product_distribution(BinomialLogit.(ni, logitp))
end;

flow.model = m12_3
