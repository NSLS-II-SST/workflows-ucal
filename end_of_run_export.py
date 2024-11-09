from prefect import flow, get_run_logger, task
from os.path import exists, join
import os
from export_to_athena import exportToAthena
from export_to_xdi import exportToXDI
from export_tools import initialize_tiled_client, get_proposal_path
import datetime


def get_export_path(run):
    proposal_path = get_proposal_path(run)

    visit_date = datetime.datetime.fromisoformat(run.start.get("start_datetime", datetime.datetime.today().isoformat()))
    visit_dir = visit_date.strftime("%Y%m%d_export")

    export_path = join(proposal_path, visit_dir)
    return export_path


@task(retries=2, retry_delay_seconds=10)
def export_all_streams(uid, beamline_acronym="ucal"):
    logger = get_run_logger()
    tiled_client = initialize_tiled_client(beamline_acronym)
    run = tiled_client[uid]
    export_path = get_export_path(run)
    logger.info(f"Generating Export for uid {run.start['uid']}")
    logger.info(f"Export Data to {export_path}")
    export_path_exists = exists(export_path)
    if not export_path_exists:
        os.makedirs(export_path, exist_ok=True)
        logger.info(f"Export path does not exist, making {export_path}")

    logger.info("Exporting XDI")
    exportToXDI(export_path, run)
    logger.info("Exporting Athena")
    exportToAthena(export_path, run)


@flow
def general_data_export(uid, beamline_acronym="ucal"):
    export_all_streams(uid, beamline_acronym)
