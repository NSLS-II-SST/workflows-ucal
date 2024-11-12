from prefect import flow, get_run_logger
from export_tools import get_proposal_path, initialize_tiled_client
from autoprocess.statelessAnalysis import handle_run
from os.path import join


@flow(log_prints=True)
def process_tes(uid, beamline_acronym="ucal", reprocess=False):
    logger = get_run_logger()
    catalog = initialize_tiled_client(beamline_acronym)
    run = catalog[uid]

    logger.info(f"In TES Exporter for {run.start['uid']}")

    save_directory = join(get_proposal_path(run), "ucal_processing")

    handle_run(run, catalog, save_directory)
