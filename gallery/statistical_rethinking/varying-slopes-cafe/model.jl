"""
This model is from https://github.com/vectorly-ai/Gallery/tree/main/StatisticalRethinkingJulia/varying-slopes-cafe.
The data it needs is also in that directory (https://github.com/vectorly-ai/Gallery/blob/main/StatisticalRethinkingJulia/varying-slopes-cafe/data.csv).
If you don't provide any data in the workflow, that data will be used by default.
If you want to provide data, it should be provided in the same format as the default data and attached to the workflow.
"""

using Pkg
Pkg.develop(; path=ARGS[1])  # load Coinfer.jl

using Turing
using Coinfer

flow = Coinfer.current_workflow()

@model function m13_1(cafe, afternoon, wait)
    Rho ~ LKJ(2, 1.0)
    sigma ~ truncated(Cauchy(0, 2), 0, Inf)
    sigma_cafe ~ filldist(truncated(Cauchy(0, 2), 0, Inf), 2)
    a ~ Normal(0, 10)
    b ~ Normal(0, 10)

    dist_mu = [a, b]
    dist_Sigma = sigma_cafe .* Rho .* sigma_cafe'
    dist_Sigma = (dist_Sigma' + dist_Sigma) / 2
    a_b_cafe ~ filldist(MvNormal(dist_mu, dist_Sigma), 20)

    a_cafe = a_b_cafe[1, :]
    b_cafe = a_b_cafe[2, :]

    μ = a_cafe[cafe] + b_cafe[cafe] .* afternoon
    wait ~ MvNormal(μ, sigma)
end;

flow.model = m13_1
