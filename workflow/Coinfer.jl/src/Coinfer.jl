module Coinfer

using JSON
using HTTP
using Turing
using Logging
using Base.CoreLogging
using TensorBoardLogger
using Dates
using DynamicPPL
using UUIDs
using YAML
using Base.Filesystem
using DataFrames
using CSV
using OnlineStats
using OnlineStatsBase
using AbstractMCMC

include("tensorboard.jl")

function is_sync()
    ENV["COINFER_SYNC"] != "FALSE"
end

function default_endpoints()
    return get(ENV, "COINFER_SERVER_ENDPOINT", "https://api.coinfer.ai")
end

mutable struct Workflow
    model
    parsed_data
    analyzer
    settings
end

function get_object(objid::String)
    url = endpoint("api", "/object/$objid")
    headers = headers_with_token("Content-Type" => "application/json")
    resp = HTTP.get(url, headers)
    json = JSON.parse(String(resp.body))
    if json["status"] != "ok"
        throw(ErrorException("Failed to get object $objid: $(json["message"])"))
    end
    return json["data"]
end

wf = Ref{Workflow}()

function current_workflow()
    if isassigned(wf)
        return wf[]
    end
    settings = YAML.load(read("../workflow.yaml", String))  # cwd=model root directory
    parsed_data = []
    if Filesystem.isfile("../tmp/parsed-data")
        parsed_data = [_convert_type(x) for x in JSON.parsefile("../tmp/parsed-data")]
    end

    wf[] = Workflow(nothing, parsed_data, nothing, settings)
    return wf[]
end

function _convert_type(v::Union{Any,Array{Any}})
    if isa(v, Vector)
        isempty(v) && return v
        return convert.(typeof(v[1]), v)
    end
    return v
end

function endpoint(name, path; endpoints=default_endpoints())
    return rstrip(endpoints, '/') * "/" * name * "/" * lstrip(path, '/')
end

const _TOKEN = Ref{String}()
set_token(t) = _TOKEN[] = t

function get_token()
    token = get(ENV, "COINFER_AUTH_TOKEN", "")
    isempty(token) && (token = _TOKEN[])
    return token
end

function headers_with_token(args...)
    token = get_token()
    headers = ["Authorization" => "bearer $(token)"]
    for p in args
        push!(headers, p)
    end
    return headers
end

function response_data(resp; debug=false)
    if resp.status != 200
        @error "HTTP Error" url = resp.request.url code = resp.status
    end
    body = String(resp.body)
    if debug
        println(">>>", body)
    end
    data = JSON.parse(body)
    if !("status" in keys(data)) && ("body" in keys(data))
        data = data["body"]
    end
    if data["status"] != "ok"
        @error "HTTP Error" url = resp.request.url return_data = data
        error(data["message"])
    end
    return data["data"]
end

function with_data_logger(experiment_id::String, url::String, data_serializer::Function, chain_name::String)
    logger = CoinferLogger(experiment_id, data_serializer; endpoint=url, chain_name=chain_name)
    return logger
end

function create_experiment()
    url = endpoint("api", "/object")
    data = Dict{String,Any}(
        "payload" => Dict{String,Any}(
            "object_type" => "experiment",
            "model_id" => get(ENV, "MODEL_ID", ""),
            "input_id" => get(ENV, "EXPERIMENT_INPUT", ""),
        ),
    )
    xp_meta = Dict()
    if !isempty(xp_meta)
        data["xp_meta"] = xp_meta
    end
    headers = headers_with_token("Content-Type" => "application/json")
    res = HTTP.post(url, headers, JSON.json(data))
    return response_data(res)
end

function update_experiment_runinfo(exp_id, batch_id, run_id, status)
    if !is_sync()
        return nothing
    end
    data = Dict{String,Any}(
        "payload" => Dict{String,Any}(
            "object_type" => "experiment",
            "meta" => Dict(
                "run_info" => Dict(
                    "experiment_id" => exp_id,
                    "batch_id" => batch_id,
                    "run_id" => run_id,
                    "status" => status,
                ),
            ),
        ),
    )
    url = endpoint("api", "/object/$exp_id")
    headers = headers_with_token("Content-Type" => "application/json")
    res = HTTP.post(url, headers, JSON.json(data))
    response_data(res)
end

function get_experiment_id()
    experiment_id = get(ENV, "EXPERIMENT_ID", "")
    if isempty(experiment_id)
        experiment = create_experiment()
        experiment_id = experiment["short_id"]
        ENV["EXPERIMENT_ID"] = experiment_id
    end
    return experiment_id
end

function initialize_batch_id()
    if !haskey(ENV, "BATCH_ID")
        ENV["BATCH_ID"] = ENV["RUN_ID"] = string(UUIDs.uuid1().value; base=62)
    end
end

function write_data_csv(chain_name, iteration, data::Vector{Pair{String,Any}})
    mcmc_data_path = get(ENV, "COINFER_MCMC_DATA_PATH", "mcmcdata")
    df = DataFrame(;
        chain_name=String[], var_name=String[], iteration=Int[], var_value=Union{Float64,Int,Bool}[]
    )
    for (var_name, var_data) in data
        push!(df, (chain_name, var_name, iteration, var_data))
    end
    if size(df)[1] > 0
        csv_path = joinpath(
            mcmc_data_path, "$(HTTP.escapeuri(chain_name)).csv"
        )
        CSV.write(csv_path, df; append=true)
    end
end

function sample(args...; kwargs...)
    initialize_batch_id()
    # exp_id = get_experiment_id()
    exp_id = ENV["EXPERIMENT_ID"]
    mcmc_data_path = get(ENV, "COINFER_MCMC_DATA_PATH", "mcmcdata")
    mkpath(mcmc_data_path)
    url = endpoint("api", "/object/" * exp_id)

    try
        chain_name = get(kwargs, :chain_name, "chain#")
        logger = with_data_logger(exp_id, url, write_data_csv, chain_name)
        tb_callback = TensorBoardCallbackExt(logger)
        cb = tb_callback
        AbstractMCMC.sample(args...; callback=cb, kwargs...)
        update_experiment_runinfo(exp_id, ENV["BATCH_ID"], ENV["RUN_ID"], "SAMPLE_FIN")
    catch exp
        @error "ERROR" exception=(exp, catch_backtrace())
        update_experiment_runinfo(exp_id, ENV["BATCH_ID"], ENV["RUN_ID"], "ERR")
        exit(-1)
    end
end

### Logger
mutable struct CoinferLogger <: TensorBoardLogger.AbstractLogger
    experiment_id::String
    endpoint::String
    auth_token::String
    global_step::Int
    step_increment::Int
    min_level::LogLevel
    data_serializer::Function
    iteration::Int
    chain_name::String
end

function CoinferLogger(
    experiment_id,
    data_serializer::Function;
    endpoint="",
    auth_token="",
    purge_step::Union{Int,Nothing}=nothing,
    step_increment=1,
    min_level::LogLevel=Logging.Info,
    chain_name::String="chain#",
)
    start_step = something(purge_step, 0)
    return CoinferLogger(
        experiment_id,
        endpoint,
        auth_token,
        start_step,
        step_increment,
        min_level,
        data_serializer,
        0,
        chain_name,
    )
end

# Implement the AbstractLogger Interface
TensorBoardLogger.set_step!(lg::CoinferLogger, step) = lg.global_step = step
TensorBoardLogger.set_step_increment!(lg::CoinferLogger, Δstep) = lg.step_increment = Δstep
TensorBoardLogger.increment_step!(lg::CoinferLogger, Δ_Step) = lg.global_step += Δ_Step
function increment_step!(lg::CoinferLogger, Δ_Step)
    return TensorBoardLogger.increment_step!(lg, Δ_Step)
end

TensorBoardLogger.step(lg::CoinferLogger) = lg.global_step
TensorBoardLogger.reset!(lg::CoinferLogger) = TensorBoardLogger.set_step!(lg, 0)
CoreLogging.catch_exceptions(lg::CoinferLogger) = false
CoreLogging.min_enabled_level(lg::CoinferLogger) = lg.min_level
CoreLogging.shouldlog(lg::CoinferLogger, level, _module, group, id) = true

function _preprocess(message, key, val::OnlineStatsBase.Series, data)
    new_series = Series([x for x in val.stats if !isa(x, OnlineStats.KHist)]...)
    TensorBoardLogger.preprocess(message * "/$key", new_series, data)
end

function _preprocess(message, key, val::T, data) where {T}
    TensorBoardLogger.preprocess(message * "/$key", val, data)
end

function CoreLogging.handle_message(
    lg::CoinferLogger, level, message, _module, group, id, file, line; kwargs...
)
    if isempty(lg.endpoint)
        # calling @warn/info in handle_message causes recursive-calls
        # thus stackoverflow.
        #@warn "No endpoint provided, log ignored."
        println("#Warning# No endpoint provided in CoinferLogger, log ignored.")
        return nothing
    end

    log = Dict{Symbol,Any}(:data => nothing)
    i_step = lg.step_increment # :log_step_increment default value
    if !isempty(kwargs)
        data = Vector{Pair{String,Any}}()
        for (key, val) in pairs(kwargs)
            if key == :iteration
                log[:iteration] = val
                continue
            end
            # special key describing step increment
            if key == :log_step_increment
                i_step = val
                continue
            end
            _preprocess(message, key, val, data)
        end
        log[:data] = data
    end
    iter = increment_step!(lg, i_step)
    log[:step] = iter
    try
        lg.data_serializer(lg.chain_name * string(group), log[:iteration], log[:data])
    catch exp
        println("ERROR exception=", (exp, catch_backtrace()))
        println("kwargs=", kwargs)
        rethrow
    end
end

end
