import base64
import logging
import pathlib
import uuid
from functools import lru_cache
from typing import Required, TypedDict, cast

import yaml
from ruamel.yaml import YAML  # uses as writer to keep user comments

logger = logging.getLogger(__name__)


def bool_sync(sync: str | bool) -> bool:
    # off is turned into False by yaml
    # 'off' is kept as 'off'
    if isinstance(sync, str):
        return sync != "off"
    return sync


@lru_cache(maxsize=1)
def get_token() -> str:
    config_file = pathlib.Path.home() / ".config" / "coinfer" / "config.yaml"
    if not config_file.is_file():
        # fallback to /tmp/ directory for serverless run as home may be readonly
        config_file = pathlib.Path("/tmp") / "coinfer" / 'config.yaml'
        if not config_file.is_file():
            return ""
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        token = config.get("auth", {}).get('token')
    return token


def base62(n: int) -> str:
    if n == 0:
        return "0"

    base62_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    result: list[str] = []

    while n > 0:
        n, remainder = divmod(n, 62)
        result.append(base62_chars[remainder])

    return "".join(result[::-1])


def gen_batch_id():
    return base62(cast(int, uuid.uuid1().int))


NEED_LOGIN_PROMPT = """
You are not logged in. Please run `inv login` first.
Or you can change to 'sync: off' in workflow.yaml to disable sync with cloud.
"""


def set_token(token: str):
    config_file = pathlib.Path.home() / ".config" / "coinfer" / "config.yaml"
    if config_file.is_file():
        with open(config_file, 'r') as f:
            config = YAML().load(f)  # type: ignore
    else:
        config = {}
    if 'auth' not in config:
        config['auth'] = {}
    config['auth']['token'] = token
    with open(config_file, 'w') as f:
        YAML().dump(config, f)  # type: ignore
    return token


class UnifiedTreeNode(TypedDict, total=False):
    name: Required[str]
    type: Required[str]
    content: str
    children: list["UnifiedTreeNode"]


def extract_files_from_json(json_data: list[UnifiedTreeNode], current_dir: pathlib.Path):
    for item in json_data:
        if item['type'] == "file":
            file: pathlib.Path = current_dir / item['name']
            if not file.exists():
                file.write_bytes(base64.b64decode(item['content']))  # type: ignore
        else:
            (current_dir / item['name']).mkdir(exist_ok=True)
            extract_files_from_json(item['children'], current_dir / item['name'])  # type: ignore
