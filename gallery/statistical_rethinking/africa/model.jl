"""
This model is from https://github.com/vectorly-ai/Gallery/tree/main/StatisticalRethinkingJulia/africa.
The data it needs is also in that directory (https://github.com/vectorly-ai/Gallery/blob/main/StatisticalRethinkingJulia/africa/data.csv).
If you don't provide any data in the workflow, that data will be used by default.
If you want to provide data, it should be provided in the same format as the default data and attached to the workflow.
"""

using Pkg
Pkg.develop(; path=ARGS[1])  # load Coinfer.jl

using Turing
using Coinfer

flow = Coinfer.current_workflow()

@model function model_fn(y, x₁, x₂)
    σ ~ truncated(Cauchy(0, 2), 0, Inf)
    βAR ~ Normal(0, 10)
    βR ~ Normal(0, 10)
    βA ~ Normal(0, 10)
    α ~ Normal(0, 100)

    μ = α .+ βR * x₁ .+ βA * x₂ .+ βAR * x₁ .* x₂
    y ~ MvNormal(μ, σ)
end

flow.model = model_fn
