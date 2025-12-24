import logging
import shutil
import pathlib

import yaml

logger = logging.getLogger(__name__)


def clean():
    with open("workflow.yaml", "r") as f:
        workflow_settings = yaml.safe_load(f)

    mcmc_data_dir = pathlib.Path(workflow_settings['sampling']['mcmc_data']['directory'])
    if mcmc_data_dir.is_dir():
        shutil.rmtree(mcmc_data_dir)
        logger.info("Cleaned MCMC data directory: %s", mcmc_data_dir)

    analysis_output_dir = pathlib.Path(workflow_settings['analysis']['output_dir'])
    if analysis_output_dir.is_dir():
        shutil.rmtree(analysis_output_dir)
        logger.info("Cleaned analysis output directory: %s", analysis_output_dir)

    tmp_dir = pathlib.Path("tmp")
    if tmp_dir.is_dir():
        shutil.rmtree(tmp_dir)
        logger.info("Cleaned tmp directory: %s", tmp_dir)
