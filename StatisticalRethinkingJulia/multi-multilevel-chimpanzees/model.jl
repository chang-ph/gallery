"""
This model is from https://github.com/vectorly-ai/Gallery/tree/main/StatisticalRethinkingJulia/multi-multilevel-chimpanzees.
The data it needs is also in that directory (https://github.com/vectorly-ai/Gallery/blob/main/StatisticalRethinkingJulia/multi-multilevel-chimpanzees/data.csv).
If you don't provide any data in the workflow, that data will be used by default.
If you want to provide data, it should be provided in the same format as the default data and attached to the workflow.
"""

using Pkg
Pkg.develop(; path=ARGS[1])  # load Coinfer.jl

using Turing
using Coinfer

flow = Coinfer.current_workflow()

@model function m12_5(pulled_left, actor, block, condition, prosoc_left)
    ## Total num of y
    N = length(pulled_left)

    ## Separate σ priors for each actor and block
    σ_actor ~ truncated(Cauchy(0, 1), 0, Inf)
    σ_block ~ truncated(Cauchy(0, 1), 0, Inf)

    ## Number of unique actors in the data set
    N_actor = length(unique(actor)) ## 7
    N_block = length(unique(block))

    ## Vector of actors (1,..,7) which we'll set priors on
    α_actor ~ filldist(Normal(0, σ_actor), N_actor)
    α_block ~ filldist(Normal(0, σ_block), N_block)

    ## Prior for intercept, prosoc_left, and the interaction
    α ~ Normal(0, 10)
    βp ~ Normal(0, 10)
    βpC ~ Normal(0, 10)

    logitp = α .+ α_actor[actor] + α_block[block] .+ (βp .+ βpC * condition) .* prosoc_left

    pulled_left ~ product_distribution(BinomialLogit.(1, logitp))
end;

flow.model = m12_5
