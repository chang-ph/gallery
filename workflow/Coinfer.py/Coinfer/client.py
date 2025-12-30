import base64
import gzip
import json
import logging
import os
import urllib.parse
from pathlib import Path
from typing import Any, Required, TypedDict

from .logged_requests import requests, requests_lib

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RunInfoData(TypedDict, total=False):
    experiment_id: Required[str]
    batch_id: Required[str]
    run_id: Required[str]
    log_group: str
    log_stream: str
    run_on: str
    status: str


# chain-name <==> {chain-name <==> [chain-values, ...]}
ChainVarData = dict[str, dict[str, list[Any]]]
# chain-name <==> (min-iter, max-iter)
ChainIterMap = dict[str, tuple[int, int]]


class LogDataDict(TypedDict):
    vars: ChainVarData
    iteration: ChainIterMap


class Client:
    session = requests
    run_info: RunInfoData

    def __init__(self, endpoints: str, coinfer_auth_token: str):
        self.endpoints = endpoints.rstrip("/")
        self.coinfer_auth_token = coinfer_auth_token
        self.run_info = {
            "experiment_id": "",
            "batch_id": "",
            "run_id": "",
        }

    def endpoint(self, name: str, path: str) -> str:
        sep = "/"
        if name:
            return self.endpoints + sep + name + sep + path.lstrip("/")
        else:
            return self.endpoints + sep + path.lstrip("/")

    def headers_with_auth(self, **kwargs: Any) -> dict[str, Any]:
        headers = {"Authorization": f"bearer {self.coinfer_auth_token}"}
        headers.update(kwargs)
        return headers

    @staticmethod
    def response_data(resp: requests_lib.Response | None) -> dict[str, Any]:
        if resp is None:
            raise Exception("Empty response")
        if resp.status_code == 401:
            raise Exception("Unauthorized")
        rdata = resp.json()
        if rdata["status"] != "ok":
            msg = rdata["message"]
            logger.error(f"{msg}")
            raise Exception(msg)
        return rdata["data"] or {}

    def sendmsg(self, group: str, data: dict[str, Any], mtype: str = "object_broadcast"):
        url = self.endpoint("api", "/object/" + self.run_info["experiment_id"])
        data = {
            "payload": {
                "object_type": "experiment.text_message",
                "datas": [
                    {
                        "group": group,
                        "type": mtype,
                        "message": data,
                    }
                ],
            }
        }
        data["payload"].update(self.run_info)
        res = self.session.post(url, data=json.dumps(data), headers=self.headers_with_auth())
        return self.response_data(res)

    def update_experiment(self, exp_id: str, data: dict[str, Any]):
        if self.run_info:
            data.setdefault("meta", {}).setdefault("run_info", {}).update(self.run_info)
            if data.get("status"):
                data["meta"]["run_info"]["status"] = data["status"]
        url = self.endpoint("api", f"/object/{exp_id}")
        res = self.session.post(
            url,
            data=json.dumps({"payload": {"object_type": "experiment", **data}}),
            headers=self.headers_with_auth(),
        )
        return self.response_data(res)

    def create_experiment(
        self,
        model_id: str,
        workflow_id: str,
        input_id: str,
        meta: dict[str, Any],
        name: str = "",
        run_on: str = "",
    ):
        url = self.endpoint("api", "/object")
        data: dict[str, Any] = {
            "payload": {
                "object_type": "experiment",
                "model_id": model_id,
                "workflow_id": workflow_id,
                "input_id": input_id,
                "meta": meta,
                "name": name,
                "run_on": run_on,
            }
        }
        headers = self.headers_with_auth()
        headers["Content-Type"] = "application/json"
        res = self.session.post(url, headers=headers, json=data)
        return self.response_data(res)

    def get_experiment(self, exp_id: str, share_password: str = "", query_params=None):
        url = self.endpoint("api", f"/object/{exp_id}")
        if self.coinfer_auth_token:
            headers = self.headers_with_auth()
        elif share_password:
            headers = {"X-Share-Password": share_password}
        else:
            headers = {}

        query_params = query_params or {}
        res = self.session.get(url, headers=headers, params=query_params)
        ret = self.response_data(res)
        if not ret:
            logger.error("get experiment failed: %s", self.session.reqid)
            return ret
        return ret

    def set_experiment_run_info(self, run_info: RunInfoData):
        self.run_info = run_info

    def get_experiment_run_info(self, experiment_id: str, batch_id: str, run_id: str):
        url = self.endpoint(
            "api",
            f"/object/{experiment_id}?object_type=experiment&batch_id={batch_id}&run_id={run_id}",
        )
        headers = self.headers_with_auth()
        res = self.session.get(url, headers=headers)
        return self.response_data(res)

    def create_model(self, content: dict[str, Any], name: str = ""):
        url = self.endpoint("api", "/object")
        headers = self.headers_with_auth()
        res = self.session.post(
            url,
            headers=headers,
            json={"payload": {"object_type": "model", "name": name, "content": content}},
        )
        return self.response_data(res)["short_id"]

    def get_access_token(self) -> str:
        res = self.session.get(self.endpoint("base", "/access_token"), headers=self.headers_with_auth())
        return self.response_data(res)["access_token"]

    def get_object(self, object_id: str):
        url = self.endpoint("api", f"/object/{object_id}")
        headers = self.headers_with_auth()
        res = self.session.get(url, headers=headers)
        return self.response_data(res)

    def post_object(self, object_id: str, data: dict[str, Any]):
        url = self.endpoint("api", f"/object/{object_id}")
        headers = self.headers_with_auth()
        res = self.session.post(url, headers=headers, json={"payload": data})
        return self.response_data(res)

    def create_object(self, payload: dict[str, Any]):
        url = self.endpoint("api", "/object")
        headers = self.headers_with_auth()
        res = self.session.post(url, headers=headers, json={"payload": payload})
        return self.response_data(res)

    def download_workflow(self, workflow_id: str, is_cloud: bool = False):
        params: dict[str, Any] = {
            "is_cloud": is_cloud,
        }
        url = self.endpoint("", f"/download/{workflow_id}?fmt=tar.gz")
        headers = self.headers_with_auth()
        res = self.session.get(url, headers=headers, params=params, stream=True)
        if not res or res.status_code != 200:
            logger.error(
                "download workflow failed: %s %s",
                self.session.reqid,
                self.session.errmsg,
            )
            return None

        return res

    def send_mcmc_data(
        self,
        experiment_id: str,
        batch_id: str,
        run_id: str,
        log_data: LogDataDict,
    ):
        url = self.endpoint("api", f"/object/{experiment_id}")
        headers = self.headers_with_auth()
        headers["Content-Type"] = "application/json"
        body: dict[str, Any] = {
            "payload": {
                "object_type": "experiment.protobuf_message",
                "logs": log_data,
                "batch_id": batch_id,
                "run_id": run_id,
            }
        }
        for chain_name, chain_data in log_data['vars'].items():
            logger.info("send mcmc data: %s, %s", chain_name, len(chain_data.keys()))
        resp = self.session.post(url, headers=headers, data=json.dumps(body, allow_nan=True))
        self.response_data(resp)

    def save_analyzer_result(
        self,
        workflow_id: str,
        return_code: int,
        errlines: list[str],
        result_file: str,
    ):
        url = self.endpoint("api", f"/object/{workflow_id}")
        headers = self.headers_with_auth()
        headers["Content-Type"] = "application/json"
        result = ''
        if Path(result_file).exists():
            result = base64.b64encode(gzip.compress(Path(result_file).read_bytes())).decode()

        body: dict[str, Any] = {
            "payload": {
                "object_type": "workflow.analyzer_result",
                "return_code": return_code,
                "errlines": errlines,
                "result": result,
            }
        }
        resp = self.session.post(url, headers=headers, json=body)
        self.response_data(resp)

    def get_user_info(self):
        url = self.endpoint("base", "/user")
        headers = self.headers_with_auth()
        res = self.session.get(url, headers=headers)
        return self.response_data(res)

    def ensure_experiment_for_workflow(self, workflow_id: str, experiment_name: str, engine: str):
        url = self.endpoint("api", f"/object/{workflow_id}")
        headers = self.headers_with_auth()
        res = self.session.get(url, headers=headers)
        wf_data = self.response_data(res)
        if wf_data.get("experiment_id"):
            exp_rsp = self.get_object(wf_data["experiment_id"])
            return exp_rsp, wf_data

        exp_rsp = self.create_experiment(
            model_id=wf_data["model_id"],
            workflow_id=workflow_id,
            input_id=(wf_data["data_id"] or ""),
            meta={"status": "RUN"},
            name=experiment_name,
            run_on=engine,
        )
        wf_data["experiment_id"] = exp_rsp["short_id"]
        wf_data["experiment_name"] = experiment_name
        return exp_rsp, wf_data

    def config_url(self):
        config_url = os.getenv("CONFIG_URL")
        if config_url:
            return config_url

        url_parts = urllib.parse.urlparse(self.endpoints)
        if not url_parts.hostname:
            raise RuntimeError(f"Invalid endpoints: {self.endpoints}")

        config_file_name = url_parts.hostname.replace('.', '-')
        return f'https://coinfer.ai/config/{config_file_name}.json'

    def call_after_sample_lambda(self, exp_id: str, batch_id: str, run_id: str):
        url = self.config_url()
        logger.info('get lambda url from: %s', url)
        rsp = self.session.get(url)
        if self.session.errmsg:
            logger.error("Failed to get lambda url: %s", self.session.errmsg)
            return
        assert rsp
        url = rsp.json()["data"]["run_model_url"]
        logger.info('lambda url: %s', url)

        self.session.post(
            url,
            headers={"Content-Type": "application/json"},
            json={
                "cmd": "after_sample",
                "experiment_id": exp_id,
                "wd_auth_token": self.coinfer_auth_token,
                "coinfer_server_endpoint": self.endpoints,
                "batch_id": batch_id,
                "run_id": run_id,
            },
        )
        if self.session.errmsg:
            logger.error("Failed to call after_sample lambda: %s", self.session.errmsg)
            return
