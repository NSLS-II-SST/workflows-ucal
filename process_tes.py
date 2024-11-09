from prefect import flow, get_run_logger, task
from export_tools import initialize_tiled_client, get_proposal_path
from autoprocess.statelessAnalysis import get_data, handle_calibration_run, handle_science_run
from autoprocess.utils import get_filename
from os.path import join


@flow(log_prints=True)
def process_tes(uid, beamline_acronym="ucal"):
    catalog = initialize_tiled_client(beamline_acronym)
    logger = get_run_logger()

    logger.info(f"In TES Exporter for {uid}")

    run = catalog[uid]

    # Check if run contains TES data
    if "tes" not in run.start.get("detectors", []):
        logger.info("No TES in run, skipping!")
        return False

    logger.info(f"Loading TES Data from {get_filename(run)}")
    # Get data files
    data = get_data(run)
    data.verbose = False
    save_directory = join(get_proposal_path(run), "ucal_processing")
    logger.info("TES Data loaded")
    # Handle calibration runs first
    if run.start.get("scantype", "") == "calibration":
        return handle_calibration_run(run, data, catalog, save_directory)
    else:
        return handle_science_run(run, data, catalog, save_directory)
