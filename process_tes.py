from prefect import flow, get_run_logger
from export_tools import initialize_tiled_client, get_proposal_path
from autoprocess.statelessAnalysis import handle_run
from os.path import join


@flow(log_prints=True)
def process_tes(uid, beamline_acronym="ucal", reprocess=False):
    catalog = initialize_tiled_client(beamline_acronym)
    logger = get_run_logger()

    logger.info(f"In TES Exporter for {uid}")

    run = catalog[uid]
    save_directory = join(get_proposal_path(run), "ucal_processing")

    handle_run(uid, catalog, save_directory)
