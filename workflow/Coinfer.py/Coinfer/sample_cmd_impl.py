import csv
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

import yaml

from .client import Client, RunInfoData
from .client_common import NEED_LOGIN_PROMPT, bool_sync, gen_batch_id, get_token

INTERVAL = int(os.environ.get("COINFER_DATA_SENDING_INTERVAL", "3"))

logger = logging.getLogger(__name__)

signal_handler_params = {
    "coinfer_server_endpoint": "",
    "coinfer_auth_token": "",
    "experiment_id": "",
    "batch_id": "",
    "run_id": "",
}


def signal_handler(signum: int, _: Any):
    logger.warning("received signal: %s %s", signum, signal_handler_params)
    exp_id = signal_handler_params["experiment_id"]
    token = signal_handler_params["coinfer_auth_token"]

    logger.warning("received signal: %s, exp_id=%s, token=%s", signum, exp_id, token)
    if signum not in (signal.SIGTERM, signal.SIGINT):
        return
    if not exp_id or not token:
        return

    group_name = f"object_{exp_id}"
    wdserver = Client(signal_handler_params["coinfer_server_endpoint"], token)
    wdserver.set_experiment_run_info(
        {
            "experiment_id": exp_id,
            "batch_id": signal_handler_params["batch_id"],
            "run_id": signal_handler_params["run_id"],
        }
    )
    logger.warning("update experiment status")
    wdserver.update_experiment(exp_id, {"status": "ERR"})
    logger.warning("send error message")
    wdserver.sendmsg(group_name, {"action": "experiment:error", "data": "server terminated"})
    logger.warning("finished signal handling")
    sys.exit(0)


def sample():
    settings = yaml.safe_load(open("workflow.yaml"))
    workflowdir = Path(os.getcwd())

    sampling = settings['sampling']
    coinfer = settings.get(sampling['sync'], {})
    is_sync = bool_sync(sampling['sync'])
    if is_sync:
        is_cloud = os.environ.get("ECS_AGENT_URI") or os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME")
        token = get_token()
        if not token:
            print(NEED_LOGIN_PROMPT)
            return

        client = Client(coinfer["endpoint"], token)
        serverless = settings['serverless']
        wf_id = coinfer["workflow_id"]
        exp_data, _ = client.ensure_experiment_for_workflow(wf_id, coinfer["experiment_name"], serverless["engine"])
        exp_id = exp_data["short_id"]
        batch_id = coinfer.get("batch_id", gen_batch_id())
        run_id = coinfer.get("run_id", gen_batch_id())
        if is_cloud:
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGHUP, signal_handler)
            cloudwatch_info = collect_cloudwatch_info()
            run_info: RunInfoData = {
                "experiment_id": exp_id,
                "batch_id": os.environ.get("COINFER_BATCH_ID", batch_id),
                "run_id": os.environ.get("COINFER_RUN_ID", run_id),
                "log_group": cloudwatch_info["group_name"],
                "log_stream": cloudwatch_info["stream_name"],
                "run_on": cloudwatch_info["engine_type"],
            }
            client.set_experiment_run_info(run_info)

        coinfer["experiment_id"] = exp_id
        client.set_experiment_run_info({"batch_id": batch_id, "run_id": run_id, "experiment_id": exp_id})
        client.update_experiment(exp_id, {"status": "RUN"})
        group_name = f"object_{exp_id}"
        client.sendmsg(group_name, {"action": "start"})
    else:
        group_name = ""
        client = None
        wf_id = ""
        exp_id = ""
        batch_id = ""
        run_id = ""

    status = None
    try:
        _run_data_script(settings, workflowdir, client, group_name)
        status = _run_model(settings, workflowdir, wf_id, exp_id, batch_id, run_id, client, group_name)
    except BaseException as e:
        if is_sync:
            logging.exception(f"failed to run experiment: {exp_id=}")
            group_name = f"object_{exp_id}"
            assert client
            client.sendmsg(group_name, {"action": "experiment:error", "data": str(e)})
            client.update_experiment(exp_id, {"status": "ERR"})
            sys.exit(-1)
    finally:
        signal_handler_params["coinfer_server_endpoint"] = ""
        signal_handler_params["coinfer_auth_token"] = ""
        signal_handler_params["experiment_id"] = ""
        signal_handler_params["batch_id"] = ""
        signal_handler_params["run_id"] = ""

    if status != 'SAMPLE_FIN':
        sys.exit(-1)


def _run_model(
    settings: dict[str, Any],
    workflowdir: Path,
    wf_id: str,
    exp_id: str,
    batch_id: str,
    run_id: str,
    client: Client | None,
    group_name: str,
):
    sampling = settings['sampling']

    pre_script = """
    using Pkg
    """
    if (workflowdir / "model" / "Manifest.toml").is_file():
        pre_script += """Pkg.resolve()"""
    else:
        pre_script += """Pkg.instantiate(;verbose=true)"""

    coinfer = settings.get(sampling['sync'], {})
    is_sync = bool_sync(sampling['sync'])
    modelmeta = json.loads(Path("model", ".metadata").read_text())
    model_entrance_file = modelmeta["entrance_file"]
    run_model_scripts = "\n".join(
        (
            pre_script,
            Path("model", model_entrance_file).read_text(),
            Path("model", "script.jl").read_text(),
        )
    )

    if exp_id:
        mcmc_data_path = workflowdir / sampling['mcmc_data'].get("directory", "mcmcdata") / exp_id
    else:
        mcmc_data_path = workflowdir / sampling['mcmc_data'].get("directory", "mcmcdata")

    envs: dict[str, Any] = os.environ | {
        "EXPERIMENT_ID": exp_id,
        "BATCH_ID": batch_id,
        "RUN_ID": run_id,
        "WORKFLOW_ID": wf_id,
        "COINFER_SYNC": "TRUE" if is_sync else "FALSE",
        "COINFER_AUTH_TOKEN": get_token(),
        "COINFER_SERVER_ENDPOINT": coinfer.get("endpoint", ""),
        "COINFER_MCMC_DATA_PATH": mcmc_data_path.as_posix(),
        "PATH": f"{os.environ.get('PATH', '')}:/usr/local/julia/bin",
    }
    cmd: list[str] = [
        "julia",
        *sampling.get("julia_args", []),
        "--project",
        "-e",
        run_model_scripts,
        (workflowdir / "client/Coinfer.jl").as_posix(),
    ]
    run_handler = ModelRunHandler(exp_id, batch_id, run_id, is_sync)
    status = run_handler.run_in_process(cmd, envs, workflowdir / "model", mcmc_data_path, client, group_name)
    if is_sync:
        assert client
        client.update_experiment(exp_id, {"status": status})
        client.sendmsg(group_name, {"action": "experiment:finish"})
    return status


def _mask_envs(envs: dict[str, str]) -> dict[str, str]:
    return {
        key: "*" if any(w in key.lower().split("_") for w in ["key", "secrets", "secret", "token"]) else val
        for key, val in envs.items()
    }


class PropagatingThread(threading.Thread):
    def run(self):
        self.exc = None
        try:
            if self._target is not None:  # type: ignore
                self._target(*self._args, **self._kwargs)  # type: ignore
        except BaseException as e:
            self.exc = e

    def join(self, timeout: float | None = None):
        super(PropagatingThread, self).join(timeout)
        if self.exc:
            raise self.exc


class ModelRunHandler:
    def __init__(self, exp_id: str, batch_id: str, run_id: str, is_sync: bool):
        self.exp_id = exp_id
        self.batch_id = batch_id
        self.run_id = run_id
        self.is_sync = is_sync

    def run_in_process(
        self,
        cmd: list[str],
        envs: dict[str, str],
        model_path: Path,
        mcmc_data_path: Path,
        client: Client | None,
        group_name: str,
    ):
        logger.info("Running sampling, sampling data will be saved to: %s", mcmc_data_path)
        logger.debug("sampling params: %s, %s", cmd, _mask_envs(envs))
        popen = subprocess.Popen(
            cmd,
            bufsize=1,
            env=envs,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            cwd=model_path,
        )
        if self.is_sync:
            sampling_finished_evt = threading.Event()
            thd = PropagatingThread(
                target=self._sync_mcmc_data, args=(mcmc_data_path, client, sampling_finished_evt), daemon=True
            )
            # thd = threading.Thread(target=self._sync_mcmc_data, args=(mcmc_data_path, client, evt))
            thd.start()
        assert popen.stdout is not None
        for stdout_line in iter(popen.stdout.readline, ""):
            logger.info("-->%s", stdout_line.rstrip())
            if client and not os.environ.get("JULIA_DEBUG"):
                client.sendmsg(group_name, {"action": "experiment:output", "data": stdout_line})
        popen.stdout.close()
        return_code = popen.wait()
        logger.debug("sampling process exit with code: %s", return_code)
        if self.is_sync:
            assert sampling_finished_evt  # type: ignore
            assert thd  # type: ignore
            sampling_finished_evt.set()
            logger.debug("sampling finished event set")
            try:
                thd.join()
            except Exception:
                logging.exception("Error syncing MCMC data: %s", thd.exc)
                return_code = -1
        if return_code:
            status = "ERR"
            logging.error("ERR: %s", return_code)
            if client:
                client.sendmsg(group_name, {"action": "experiment:error", "data": return_code})
        else:
            status = "SAMPLE_FIN"
            logger.info("Sampling data is saved to: %s", mcmc_data_path)
            if client:
                client.call_after_sample_lambda(self.exp_id, self.batch_id, self.run_id)
        return status

    @staticmethod
    def _merge_full_data(log_data, full_log_data, chain_iter_map, full_chain_iter_map):
        for chain_name, chain_data in log_data.items():
            for var_name, var_data in chain_data.items():
                full_log_data.setdefault(chain_name, {}).setdefault(var_name, []).extend(var_data)
        for chain_name, chain_iter in chain_iter_map.items():
            if chain_name in full_chain_iter_map:
                full_chain_iter_map[chain_name] = (
                    min(full_chain_iter_map[chain_name][0], chain_iter[0]),
                    max(full_chain_iter_map[chain_name][1], chain_iter[1]),
                )
            else:
                full_chain_iter_map.setdefault(chain_name, (chain_iter[0], chain_iter[1]))
        log_data.clear()
        chain_iter_map.clear()

    def _sync_mcmc_data(self, mcmc_data_path: Path, client: Client | None, sampling_finished_evt: threading.Event):
        logger.debug("syncing MCMC data")
        if not client:
            logger.debug("no client, quit syncing MCMC data")
            return
        while not mcmc_data_path.exists() and not sampling_finished_evt.is_set():
            time.sleep(1)

        logger.debug("%s %s", mcmc_data_path.exists(), sampling_finished_evt.is_set())
        # chain_name <--> (var_name <--> [values])
        log_data: dict[str, dict[str, list[Any]]] = {}
        full_log_data: dict[str, dict[str, list[Any]]] = {}

        # Handled file format:
        # file <--> [file_size, last_handled_line_number]
        handled_file = Path(mcmc_data_path, ".mcmc_data_handled")
        if handled_file.is_file():
            with open(handled_file) as fin:
                already_handled = json.load(fin)
        else:
            already_handled: dict[str, tuple[float, int]] = {}

        var_converter_map: dict[str, Callable[[str], Any]] = {}

        chain_iter_map: dict[str, tuple[int, int]] = {}
        full_chain_iter_map: dict[str, tuple[int, int]] = {}

        while True:
            if sampling_finished_evt.is_set():
                logger.debug("sampling finished event set, quit syncing MCMC data")
                self._merge_full_data(log_data, full_log_data, chain_iter_map, full_chain_iter_map)
                if full_log_data:
                    client.send_mcmc_data(
                        self.exp_id,
                        self.batch_id,
                        self.run_id,
                        {
                            "vars": full_log_data,
                            "iteration": full_chain_iter_map,
                        },
                    )
                    full_log_data.clear()
                    full_chain_iter_map.clear()
                break

            for mcmc_data_file in sorted(mcmc_data_path.iterdir()):
                if mcmc_data_file.suffix != ".csv":
                    continue

                fsize = mcmc_data_file.stat().st_size
                handled_data = already_handled.get(mcmc_data_file.name, (0.0, 0))
                if fsize <= handled_data[0]:
                    continue

                logger.debug("handling file: %s %s", mcmc_data_file.name, handled_data)
                last_handled_line_number = handled_data[1]
                lines = mcmc_data_file.read_text().splitlines()[last_handled_line_number:]

                csvreader = csv.reader(lines)
                chain_name = ""
                current_iteration = None
                for row in csvreader:
                    chain_name = row[0]
                    var_name = row[1]
                    iteration_number = int(row[2])
                    # the current implementation relies on the fact that the MCMC data is sorted by iteration_number
                    if current_iteration is not None and iteration_number > current_iteration:
                        self._merge_full_data(log_data, full_log_data, chain_iter_map, full_chain_iter_map)

                    current_iteration = iteration_number
                    if var_name in var_converter_map:
                        converter = var_converter_map[var_name]
                    else:
                        converter = self._guess_type(row[3])
                        var_converter_map[var_name] = converter
                    val = converter(row[3])
                    log_data.setdefault(chain_name, {}).setdefault(var_name, []).append(val)
                    if chain_name in chain_iter_map:
                        chain_iter_map[chain_name] = (
                            min(chain_iter_map[chain_name][0], iteration_number),
                            max(chain_iter_map[chain_name][1], iteration_number),
                        )
                    else:
                        chain_iter_map[chain_name] = (iteration_number, iteration_number)

                already_handled[mcmc_data_file.name] = (fsize, last_handled_line_number + len(lines))
                logger.debug("finish handle file: %s %s", mcmc_data_file.name, already_handled[mcmc_data_file.name])

            if full_log_data:
                client.send_mcmc_data(
                    self.exp_id,
                    self.batch_id,
                    self.run_id,
                    {
                        "vars": full_log_data,
                        "iteration": full_chain_iter_map,
                    },
                )
                full_log_data.clear()
                full_chain_iter_map.clear()

            time.sleep(INTERVAL)
        with open(handled_file, "w") as f:
            json.dump(already_handled, f)
        logger.debug("done syncing MCMC data")

    @staticmethod
    def _guess_type(value: str) -> Callable[[str], int | float | bool | str]:
        if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
            return lambda v: int(v)
        lower_val = value.lower()
        if lower_val in ["true", "false"]:
            return lambda v: v.lower() == "true"
        try:
            float(value)
            return lambda v: float(v)
        except ValueError:
            pass
        return lambda v: v


def _run_data_script(settings: dict[str, Any], rootdir: Path, client: Client | None, group_name: str):
    extra_envs: dict[str, str] = {
        "PYTHONPATH": Path(rootdir, "client", "Coinfer.py").as_posix(),
        "WORKFLOW_ID": settings.get("coinfer", {}).get("workflow_id", ""),
        "COINFER_SERVER_ENDPOINT": settings.get("coinfer", {}).get("endpoint", ""),
        "COINFER_AUTH_TOKEN": get_token(),
    }
    if EFS_DIR := os.environ.get("EFS_DIR"):
        extra_envs["UV_CACHE_DIR"] = f"{EFS_DIR}/uv_cache"

    popen = subprocess.Popen(
        ["uv", "run", "--script", "data.py"],
        bufsize=1,
        env=os.environ | extra_envs,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    assert popen.stdout is not None
    logger.info("Running script: data.py")
    for stdout_line in iter(popen.stdout.readline, ""):
        logger.info("-->" + stdout_line.rstrip())
        if client:
            client.sendmsg(group_name, {"action": "experiment:output", "data": stdout_line})
    popen.stdout.close()
    return_code = popen.wait()
    if not return_code == 0:
        raise RuntimeError(f"run data script failed: {return_code}")


def collect_cloudwatch_info():
    engine_type = ''
    group_name = ''
    stream_name = ''
    prno = os.environ.get('PRNO', '')

    if os.environ.get("ECS_AGENT_URI"):
        engine_type = 'fargate'
        group_name = f"/ecs/wd-run-model-pr{prno}"

        task_id = os.environ["ECS_AGENT_URI"].split("/")[-1].split('-')[0]
        # ref: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_awslogs.html#create_awslogs_logdriver_options
        # stream_name is <stream_prefix>/<container_name>/<task_id>
        # stream_prefix and container_name is defined in task definition
        stream_name = f"ecs/wd-run-model/{task_id}"
    elif os.environ.get('AWS_LAMBDA_LOG_STREAM_NAME'):
        engine_type = 'lambda'
        func_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'JuliaSampleFunction')
        group_name = f'/aws/lambda/{func_name}'
        stream_name = os.environ['AWS_LAMBDA_LOG_STREAM_NAME']

    return {'engine_type': engine_type, "group_name": group_name, "stream_name": stream_name}
