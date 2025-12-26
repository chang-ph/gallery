"""
This model is from https://github.com/vectorly-ai/Gallery/tree/main/StatisticalRethinkingJulia/non-identifiable
"""

using Pkg
Pkg.develop(; path=ARGS[1])  # load Coinfer.jl

using Turing
using Coinfer

flow = Coinfer.current_workflow()

@model function m8_4(y)
    ## Can't really set a Uniform[-Inf,Inf] on σ
    α₁ ~ Uniform(-3000, 1000)
    α₂ ~ Uniform(-1000, 3000)
    σ ~ truncated(Cauchy(0, 1), 0, Inf)

    y ~ Normal(α₁ + α₂, σ)
end

flow.model = m8_4
