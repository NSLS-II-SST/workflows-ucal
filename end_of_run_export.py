import time


from prefect import flow, get_run_logger, task
from tiled.client import from_profile
from os.path import exists, join
import os
from export_to_athena import exportToAthena
from export_tools import get_header_and_data
import datetime


def initialize_tiled_client():
    return from_profile("nsls2")


@task(retries=2, retry_delay_seconds=10)
def export_all_streams(uid, beamline_acronym="ucal"):
    logger = get_run_logger()
    tiled_client = initialize_tiled_client()
    run = tiled_client[beamline_acronym]["raw"][uid]
    proposal = run.start.get('proposal', {}).get('proposal_id', None)
    is_commissioning = "commissioning" in run.start.get("proposal", {}).get("type", "").lower()
    cycle = run.start.get('cycle', None)
    if proposal is None or cycle is None:
        raise ValueError("Proposal Metadata not Loaded")
    visit_date = datetime.datetime.fromisoformat(run.start.get('start_datetime', datetime.datetime.today().isoformat()))
    visit_dir = visit_date.strftime("%Y%m%d_export")
    if is_commissioning:
        export_path = f"/nsls2/data/sst/proposals/commissioning/pass-{proposal}/"
    else:
        export_path = f"/nsls2/data/sst/proposals/{cycle}/pass-{proposal}/"
    export_path = join(export_path, visit_dir)
    logger.info(f"Generating Export for uid {run.start['uid']}")
    logger.info(f"Export Data to {export_path}")
    export_path_exists = exists(export_path)
    if not export_path_exists:
        os.makedirs(export_path, exists_ok=True)
        logger.info(f"Export path does not exist, making {export_path}")

    header, data = get_header_and_data(run)
    exportToAthena(export_path, data, header)

    
@flow
def general_data_export(uid, beamline_acronym="ucal"):
    export_all_streams(uid, beamline_acronym)
