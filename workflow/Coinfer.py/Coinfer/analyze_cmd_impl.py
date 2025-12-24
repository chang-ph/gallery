import json
import logging
import os
import shutil
import subprocess
import threading
from pathlib import Path
from typing import IO, Any, Callable, cast

import yaml

from .client import Client
from .client_common import NEED_LOGIN_PROMPT, bool_sync, get_token

logger = logging.getLogger(__name__)
EFS_DIR = os.environ.get("EFS_DIR")
PROCESS_WAIT_TIMEOUT_SECONDS = int(os.environ.get("PROCESS_WAIT_TIMEOUT_SECONDS", 850))


def analyze():
    with open("workflow.yaml") as f:
        settings: dict[str, Any] = yaml.safe_load(f)
    analysis = settings.get("analysis", {})
    if not analysis:
        raise ValueError("analysis section is not provided in workflow.yaml")
    is_sync = bool_sync(analysis["sync"])
    token = get_token()
    if is_sync and not token:
        return print(NEED_LOGIN_PROMPT)

    return_code, errlines, result_file = _run(settings)
    if result_file:
        if Path(result_file).is_file():
            _save_analyzer_result(settings, return_code, errlines, result_file)
        else:
            logger.error(f"Analyzer result file {result_file} does not exist.")


def _save_analyzer_result(settings: dict[str, Any], return_code: int, errlines: list[str], result_file: str):
    analysis = settings.get("analysis", {})
    is_sync = bool_sync(analysis["sync"])
    if not is_sync:
        return
    local_settings = settings[analysis['sync']]
    client = Client(local_settings["endpoint"], get_token())
    client.save_analyzer_result(local_settings["workflow_id"], return_code, errlines, result_file)
    logger.info("Saved analyzer result to server.")


def _run(settings: dict[str, Any]) -> tuple[int, list[str], str]:
    workflow_dir = Path(os.getcwd())
    analysis: dict[str, Any] = settings.get("analysis", {})
    outputdir = workflow_dir / cast(str, analysis.get("output_dir", "analyzer_output"))
    os.makedirs(outputdir, exist_ok=True)
    coinfer = settings.get("coinfer", {})
    sampling = settings["sampling"]
    is_sync = bool_sync(analysis["sync"])

    working_dir = Path(workflow_dir / "analyzer")
    if not working_dir.exists():
        raise ValueError("No analyzer found. Attach an analyzer before call this command")

    if is_sync and coinfer.get("workflow_id"):
        client = Client(coinfer["endpoint"], get_token())
        wf_id = coinfer["workflow_id"]
        wf_rsp = client.get_object(wf_id)
        exp_id = wf_rsp["experiment_id"]
        client.post_object(wf_id, {"analyzer_result": '{"status": "running"}'})
    else:
        exp_id = ""

    if exp_id:
        mcmc_data_path = workflow_dir / sampling['mcmc_data'].get("directory", "mcmcdata") / exp_id
    else:
        mcmc_data_path = workflow_dir / sampling['mcmc_data'].get("directory", "mcmcdata")
    envs: dict[str, str] = os.environ | {
        "COINFER_ANALYSIS_SYNC": "TRUE" if is_sync else "FALSE",
        "WORKFLOW_ID": coinfer.get("workflow_id", ""),
        "COINFER_SERVER_ENDPOINT": coinfer.get("endpoint", ""),
        "COINFER_AUTH_TOKEN": get_token(),
        "WORKFLOW_DIR": workflow_dir.as_posix(),
        "COINFER_MCMC_DATA_PATH": mcmc_data_path.as_posix(),
        "COINFER_ANALYZE_OUTPUT_DIR": outputdir.as_posix(),
    }

    input_param_file = outputdir / "input_params"
    with open(input_param_file, 'wt') as ftmp:
        json.dump(
            {
                "coinfer_server_endpoint": coinfer.get("endpoint", ""),
                "experiment_id": exp_id,
            },
            ftmp,
        )

    metadata = json.loads(Path("analyzer", ".metadata").read_text())
    entrance_file = metadata['entrance_file']
    lang = 'python' if entrance_file.endswith('.py') else 'julia'

    cmd: list[str]
    if lang == 'python':
        cmd = ['uv', 'run', entrance_file, input_param_file.as_posix()]
        if EFS_DIR:
            envs["UV_CACHE_DIR"] = f"{EFS_DIR}/uv_cache"
    elif lang == 'julia':
        if EFS_DIR:
            envs["JULIA_DEPOT_PATH"] = f"{EFS_DIR}/julia_depot"
        cmd = ['julia', "--project", entrance_file, input_param_file.as_posix()]
    else:
        raise NotImplementedError(f"Unsupported language: {lang}")

    logger.debug('envs=%s', envs)
    shutil.copytree(f'{workflow_dir}/client/Coinfer.py/Coinfer', working_dir / "Coinfer", dirs_exist_ok=True)
    return_code, _, errlines = _run_command(cmd, env=envs, cwd=working_dir)
    if return_code != 0:
        logger.error("Analyzer failed with return code %d", return_code)
        return return_code, errlines, ""

    tmp_dir = Path(workflow_dir, "tmp")
    analyze_result_path = Path("analyzer", Path(tmp_dir, "analyze_result_path").read_text().strip())
    logger.info("Analyzer result saved to %s", analyze_result_path)

    return return_code, errlines, analyze_result_path.as_posix()


def _read_stream(stream: IO[str], callback: Callable[[str], None]):
    for line in iter(stream.readline, ''):
        callback(line.strip())
    stream.close()


def _run_command(command: list[str], env: dict[str, str], cwd: str | Path):
    logger.debug('run cmd: %s, cwd=%s', command, cwd)
    proc = subprocess.Popen(
        command,
        bufsize=1,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        cwd=cwd,
    )

    stdout_lines: list[str] = []
    stderr_lines: list[str] = []

    def handle_stdout(line: str):
        logger.info("stdout> %s", line)
        stdout_lines.append(line)

    def handle_stderr(line: str):
        logger.warning("stderr> %s", line)
        stderr_lines.append(line)

    stdout_thread = threading.Thread(target=_read_stream, args=(proc.stdout, handle_stdout))
    stderr_thread = threading.Thread(target=_read_stream, args=(proc.stderr, handle_stderr))

    stdout_thread.start()
    stderr_thread.start()

    logger.debug('before communication')
    proc.wait(timeout=PROCESS_WAIT_TIMEOUT_SECONDS)
    logger.debug('after communication')

    stdout_thread.join()
    stderr_thread.join()

    return proc.returncode, stdout_lines, stderr_lines
