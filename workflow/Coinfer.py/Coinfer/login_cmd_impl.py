import logging
import dataclasses

import yaml

from .client import Client
from .client_common import get_token, set_token

logger = logging.getLogger(__name__)


def login(token: str):
    if token:
        set_token(token)

    if token := get_token():
        try:
            userinfo = _get_user_info(token)
        except Exception as e:
            if e.args[0] == "Unauthorized":
                logger.error(f"Token invalid.\n{_login_prompt}")
                return
            else:
                raise
        logger.info(f"You're logged in as {userinfo.name}.")
    else:
        logger.info(_login_prompt)


@dataclasses.dataclass
class _UserInfo:
    name: str


def _get_user_info(token: str) -> _UserInfo:
    with open("workflow.yaml", "r") as f:
        workflow_settings = yaml.safe_load(f)

    client = Client(workflow_settings['coinfer']['endpoint'], token)
    rsp = client.get_user_info()
    return _UserInfo(rsp['username'])


_login_prompt = """
To run the sample or analyze command with result sending to server, you need a token.

You can get a token at https://coinfer.ai/bayes/#/home > Profile.

Then run `inv login --token <token>` to set the token.
"""
