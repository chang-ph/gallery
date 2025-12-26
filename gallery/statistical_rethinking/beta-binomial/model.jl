"""
This model is from https://github.com/vectorly-ai/Gallery/tree/main/StatisticalRethinkingJulia/beta-binomial.
The data it needs is also in that directory (https://github.com/vectorly-ai/Gallery/blob/main/StatisticalRethinkingJulia/beta-binomial/data.csv).
If you don't provide any data in the workflow, that data will be used by default.
If you want to provide data, it should be provided in the same format as the default data and attached to the workflow.
"""

using Pkg
Pkg.develop(; path=ARGS[1])  # load Coinfer.jl

using Turing
using Coinfer
using StatsFuns: logistic

flow = Coinfer.current_workflow()

@model function m11_5(admit, applications)
    θ ~ truncated(Exponential(1), 0, Inf)
    α ~ Normal(0, 2)

    ## alpha and beta for the BetaBinomial must be provided.
    ## The two parameterizations are related by
    ## alpha = prob * theta, and beta = (1-prob) * theta.
    ## See https://github.com/rmcelreath/rethinking/blob/master/man/dbetabinom.Rd

    prob = logistic(α)
    alpha = prob * θ
    beta = (1 - prob) * θ
    admit ~ product_distribution(BetaBinomial.(applications, alpha, beta))
end

flow.model = m11_5
