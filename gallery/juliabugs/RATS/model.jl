"""
RATS hierarchical model using JuliaBUGS with legacy BUGS syntax.

This is a normal hierarchical model from the BUGS examples.
The model describes the growth of 30 rats measured at 5 time points.

Reference: BUGS Examples Volume I - Rats
"""

using Pkg
Pkg.develop(; path=ARGS[1])

using JuliaBUGS
using Random
using AbstractMCMC
using Coinfer

flow = Coinfer.current_workflow()

model_def = @bugs("""
model{
    for(i in 1:N) {
        for(j in 1:T) {
            Y[i, j] ~ dnorm(mu[i, j], tau.c)
            mu[i, j] <- alpha[i] + beta[i] * (x[j] - xbar)
        }
        alpha[i] ~ dnorm(alpha.c, alpha.tau)
        beta[i] ~ dnorm(beta.c, beta.tau)
    }
    tau.c ~ dgamma(0.001, 0.001)
    sigma <- 1 / sqrt(tau.c)
    alpha.c ~ dnorm(0.0, 1.0E-6)
    alpha.tau ~ dgamma(0.001, 0.001)
    beta.c ~ dnorm(0.0, 1.0E-6)
    beta.tau ~ dgamma(0.001, 0.001)
    alpha0 <- alpha.c - xbar * beta.c
}
""", true, false)

inits = (
    alpha = fill(250.0, 30),
    beta = fill(6.0, 30),
    var"alpha.c" = 150.0,
    var"beta.c" = 10.0,
    var"tau.c" = 1.0,
    var"alpha.tau" = 1.0,
    var"beta.tau" = 1.0
)

function model(x, xbar, N, T, Y)
    _Y=stack(Y; dims=1)
    data = (x=x, xbar=xbar, N=N, T=T, Y=convert(Array{Int}, _Y))
    compile(model_def, data, inits)
end

flow.model = model
