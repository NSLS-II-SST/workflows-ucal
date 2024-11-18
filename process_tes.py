from prefect import flow, get_run_logger
from export_tools import get_proposal_path, initialize_tiled_client
from autoprocess.statelessAnalysis import handle_run
from os.path import join, dirname
import os
import pickle


@flow(log_prints=True)
def process_tes(uid, beamline_acronym="ucal", reprocess=False):
    logger = get_run_logger()
    catalog = initialize_tiled_client(beamline_acronym)
    run = catalog[uid]
    if "primary" not in run:
        logger.info(f"No Primary stream for {run.start['scan_id']}")
        return False
    logger.info(f"In TES Exporter for {run.start['uid']}")

    save_directory = join(get_proposal_path(run), "ucal_processing")

    processing_info = handle_run(uid, catalog, save_directory)
    # Save calibration information
    config_path = "/nsls2/data/sst/shared/config/data_processing_info.pkl"
    os.makedirs(dirname(config_path), exist_ok=True)
    with open(config_path, "wb") as f:
        pickle.dump(processing_info, f)
    return processing_info
