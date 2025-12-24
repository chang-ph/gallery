## Documentation

- [Coinfer.jl](https://coinfer.ai/server/dev/)

## NOTE

`Coinfer.jl` can be [run locally or remotely](https://coinfer.ai/server/dev/examples/run_model.jl).

Ideally, we want remote runs to behave exactly like local runs without requiring additional interaction with the server. However, there are notable differences between the two scenarios, leading to three specific types of server interactions in the serverless worker:

- **Creating a new experiment**
- **Reporting experiment status changes**
- **Uploading worker logs**

These three interactions are implemented in Python code rather than in `Coinfer.jl` due to the following reasons:

- **Experiment Creation**: Experiments should be created as early as possible to provide immediate responses to users. The response should include the experiment ID, allowing users to query the experiment status. This cannot be done in `Coinfer.jl` because Julia instantiation takes a long time.
    - [query_experiment](https://github.com/vectorly-ai/server/blob/8fd5d54c4a878bd2718a1bcb4b22de5662be090b/serverless/common/command/pass_model_param.py#L149)
    - [create_experiment](https://github.com/vectorly-ai/server/blob/8fd5d54c4a878bd2718a1bcb4b22de5662be090b/serverless/common/command/pass_model_param.py#L152)
- **Experiment Status Reporting**: The same reasoning applies to reporting experiment status changes.
    - [update_experiment_status](https://github.com/vectorly-ai/server/blob/8fd5d54c4a878bd2718a1bcb4b22de5662be090b/serverless/common/command/pass_model_param.py#L158)
    - [update_experiment_status](https://github.com/vectorly-ai/server/blob/8fd5d54c4a878bd2718a1bcb4b22de5662be090b/serverless/common/command/run_model.py#L85)
    - [update_experiment_status](https://github.com/vectorly-ai/server/blob/8fd5d54c4a878bd2718a1bcb4b22de5662be090b/serverless/common/command/run_model.py#L160)
    - [update_experiment_status](https://github.com/vectorly-ai/server/blob/8fd5d54c4a878bd2718a1bcb4b22de5662be090b/serverless/common/command/run_model.py#L68)
- **Log Uploads**: We run Julia as a subprocess and capture its output on the Python side. This means capturing cannot be done within `Coinfer.jl`.
    - [log_upload](https://github.com/vectorly-ai/server/blob/8fd5d54c4a878bd2718a1bcb4b22de5662be090b/serverless/common/command/run_model.py#L84)
    - [log_upload](https://github.com/vectorly-ai/server/blob/8fd5d54c4a878bd2718a1bcb4b22de5662be090b/serverless/common/command/run_model.py#L181)
    - [log_upload](https://github.com/vectorly-ai/server/blob/8fd5d54c4a878bd2718a1bcb4b22de5662be090b/serverless/common/command/run_model.py#L187)
