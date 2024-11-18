from prefect import flow, get_run_logger
from export_tools import get_proposal_path, initialize_tiled_client
from autoprocess.statelessAnalysis import handle_run
from autoprocess.utils import get_processing_info_file
from os.path import dirname, join
import os
import pickle


@flow(log_prints=True)
def process_tes(uid, beamline_acronym="ucal", reprocess=False):
    """
    Process TES data and save processing information.

    Parameters
    ----------
    uid : str
        Unique identifier for the run to process
    beamline_acronym : str, optional
        Beamline identifier
    reprocess : bool, optional
        If True, force reprocessing even if data already exists

    Returns
    -------
    dict
        Processing information dictionary
    """
    logger = get_run_logger()
    catalog = initialize_tiled_client(beamline_acronym)
    run = catalog[uid]

    if "primary" not in run:
        logger.info(f"No Primary stream for {run.start['scan_id']}")
        return False

    logger.info(f"In TES Exporter for {run.start['uid']}")
    save_directory = join(get_proposal_path(run), "ucal_processing")

    # Process the run
    processing_info = handle_run(uid, catalog, save_directory)
    # Save calibration information
    config_path = "/nsls2/data/sst/shared/config/ucal/processing_status"
    os.makedirs(dirname(config_path), exist_ok=True)
    with open(config_path, "wb") as f:
        pickle.dump(processing_info, f)

    if "data_calibration_info" in processing_info:
        cal_path = get_processing_info_file(config_path, "calibration")
        os.makedirs(dirname(cal_path), exist_ok=True)

        with open(cal_path, "wb") as f:
            pickle.dump(processing_info["data_calibration_info"], f)
        logger.info(f"Saved calibration info to {cal_path}")

    # Save processing info if it exists
    if "data_processing_info" in processing_info:
        proc_path = get_processing_info_file(config_path, "processing")
        os.makedirs(dirname(proc_path), exist_ok=True)

        with open(proc_path, "wb") as f:
            pickle.dump(processing_info["data_processing_info"], f)
        logger.info(f"Saved processing info to {proc_path}")
    return processing_info
