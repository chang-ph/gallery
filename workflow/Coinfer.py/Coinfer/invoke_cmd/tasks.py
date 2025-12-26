# ruff: noqa: E402
import logging
import os
import sys

old_factory = logging.getLogRecordFactory()


def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    record.ctx_id = os.environ.get("COINFER_CTXID", "")
    return record


logging.setLogRecordFactory(record_factory)
if level_env := os.environ.get('COINFER_LOG_LEVEL'):
    level = logging.getLevelNamesMapping()[level_env.upper()]
else:
    level = logging.INFO
logging.basicConfig(
    stream=sys.stdout,
    level=level,
    format="{levelname} {ctx_id} {filename}:{lineno} {message}",
    style='{',
)


from invoke.tasks import task

logger = logging.getLogger(__name__)


@task(aliases=['sampling'])
def sample(c):
    """Run MCMC sampling."""
    sys.path.append('client/Coinfer.py/')

    from Coinfer.sample_cmd_impl import sample as sample_impl

    sample_impl()


@task(aliases=['analyse', 'analysis', 'analyzer', 'analyser'])
def analyze(c):
    """Run the analyze script on the MCMC data."""
    sys.path.append('client/Coinfer.py/')

    from Coinfer.analyze_cmd_impl import analyze as analyze_impl

    analyze_impl()


@task(aliases=['auth'], help={'token': 'The token you want to set.'})
def login(c, token: str = ""):
    """Login to Coinfer server."""
    sys.path.append('client/Coinfer.py/')

    from Coinfer.login_cmd_impl import login as login_impl

    login_impl(token)


@task
def clean(c):
    """Clean up the generated data."""

    sys.path.append('client/Coinfer.py/')

    from Coinfer.clean_cmd_impl import clean as clean_impl

    clean_impl()
