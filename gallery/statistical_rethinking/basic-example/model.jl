"""
This model is from https://github.com/vectorly-ai/Gallery/tree/main/StatisticalRethinkingJulia/basic-example
"""

using Pkg
Pkg.develop(; path=ARGS[1])  # load Coinfer.jl

Pkg.add("Turing")

using Turing
using Coinfer

flow = Coinfer.current_workflow()

@model function gdemo(x, y)
    s ~ InverseGamma(2, 3)
    m ~ Normal(0, sqrt(s))
    x ~ Normal(m, sqrt(s))
    y ~ Normal(m, sqrt(s))
end

flow.model = gdemo
