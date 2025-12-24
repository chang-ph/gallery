using Dates
using TuringCallbacks

struct TensorBoardCallbackExt
    cb::TuringCallbacks.TensorBoardCallback
    lock::Base.AbstractLock
end

function TensorBoardCallbackExt(lg::AbstractLogger)
    cb = TuringCallbacks.TensorBoardCallback(lg)
    return TensorBoardCallbackExt(
        cb,
        ReentrantLock(),
    )
end

function (cbex::TensorBoardCallbackExt)(rng, model, sampler, transition, state, iteration; kwargs...)
    lock(cbex.lock) do
        stats = cbex.cb.stats
        lg = cbex.cb.logger
        variable_filter = Base.Fix1(TuringCallbacks.filter_param_and_value, cbex.cb)
        extras_filter = Base.Fix1(TuringCallbacks.filter_extras_and_value, cbex.cb)
        hyperparams_filter = Base.Fix1(TuringCallbacks.filter_hyperparams_and_value, cbex.cb)

        if iteration == 1 && cbex.cb.include_hyperparams
            # If it's the first iteration, we write the hyperparameters.
            hparams = Dict(Iterators.filter(
                hyperparams_filter,
                hyperparams(model, sampler, transition, state; kwargs...)
            ))
            if !isempty(hparams)
                TuringCallbacks.TensorBoardLogger.write_hparams!(
                    lg,
                    hparams,
                    hyperparam_metrics(model, sampler)
                )
            end
        end


        # TODO: Should we use the explicit interface for TensorBoardLogger?
        with_logger(lg) do
            for (k, val) in Iterators.filter(
                variable_filter,
                TuringCallbacks.params_and_values(model, sampler, transition, state; kwargs...)
            )
                stat = stats[k]

                # Log the raw value
                @info "$(cbex.cb.param_prefix)$k" val _group = kwargs[:chain_number] iteration

                # Update statistic estimators
                OnlineStats.fit!(stat, val)

                # Need some iterations before we start showing the stats
                @info "$(cbex.cb.param_prefix)$k" stat _group = kwargs[:chain_number] iteration
            end

            # Transition statistics
            if cbex.cb.include_extras
                for (name, val) in Iterators.filter(
                    extras_filter,
                    TuringCallbacks.extras(model, sampler, transition, state; kwargs...)
                )
                    @info "$(cbex.cb.extras_prefix)$(name)" val _group = kwargs[:chain_number] iteration

                    # TODO: Make this customizable.
                    if val isa Real
                        stat = stats["$(cbex.cb.extras_prefix)$(name)"]
                        fit!(stat, float(val))
                        @info ("$(cbex.cb.extras_prefix)$(name)") stat _group = kwargs[:chain_number] iteration
                    end
                end
            end
            # Increment the step for the logger.
            increment_step!(lg, 1)
        end
    end
end
