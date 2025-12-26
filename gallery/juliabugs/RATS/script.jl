using StableRNGs
using AbstractMCMC
using AdvancedHMC

flow = Coinfer.current_workflow()
m = flow.model(flow.parsed_data...)

parallel_algorithm = Meta.parse(flow.settings["sampling"]["parallel_algorithm"]) |> eval
iteration_count = flow.settings["sampling"]["iteration_count"]
num_chains = flow.settings["sampling"]["num_chains"]

Coinfer.sample(
    StableRNG(Int(floor(time()))),
    m,
    NUTS(0.8),
    parallel_algorithm,
    iteration_count,
    num_chains;
)
