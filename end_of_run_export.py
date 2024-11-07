import time


from prefect import flow, get_run_logger, task
from tiled.client import from_profile
from os.path import exists, join
import os
from export_to_athena import exportToAthena
from export_to_xdi import exportToXDI
import datetime


def initialize_tiled_client():
    return from_profile("nsls2")


def get_proposal_path(run):
    proposal = run.start.get("proposal", {}).get("proposal_id", None)
    is_commissioning = "commissioning" in run.start.get("proposal", {}).get("type", "").lower()
    cycle = run.start.get("cycle", None)
    if proposal is None or cycle is None:
        raise ValueError("Proposal Metadata not Loaded")
    if is_commissioning:
        proposal_path = f"/nsls2/data/sst/proposals/commissioning/pass-{proposal}/"
    else:
        proposal_path = f"/nsls2/data/sst/proposals/{cycle}/pass-{proposal}/"
    return proposal_path


def get_export_path(run):
    proposal_path = get_proposal_path(run)

    visit_date = datetime.datetime.fromisoformat(run.start.get("start_datetime", datetime.datetime.today().isoformat()))
    visit_dir = visit_date.strftime("%Y%m%d_export")

    export_path = join(proposal_path, visit_dir)
    return export_path


@task(retries=2, retry_delay_seconds=10)
def export_all_streams(uid, beamline_acronym="ucal"):
    logger = get_run_logger()
    tiled_client = initialize_tiled_client()
    run = tiled_client[beamline_acronym]["raw"][uid]
    export_path = get_export_path(run)
    logger.info(f"Generating Export for uid {run.start['uid']}")
    logger.info(f"Export Data to {export_path}")
    export_path_exists = exists(export_path)
    if not export_path_exists:
        os.makedirs(export_path, exist_ok=True)
        logger.info(f"Export path does not exist, making {export_path}")

    exportToXDI(export_path, run)
    exportToAthena(export_path, run)


@task(retries=2, retry_delay_seconds=10)
def export_tes(uid, beamline_acronym="ucal"):
    try:
        from ucalpost.databroker.run import get_config_dict

        tiled_client = initialize_tiled_client()
        run = tiled_client[beamline_acronym]["raw"][uid]
        if "tes" not in run.start.get("detectors", []):
            print("No TES in run, skipping!")
            return
        else:
            print(f'Noise UID: {get_config_dict(run)["tes_noise_uid"]}')
            print(f'Cal UID: {get_config_dict(run)["tes_calibration_uid"]}')
    except ImportError:
        print("Cannot import ucalpost!")


@flow
def general_data_export(uid, beamline_acronym="ucal"):
    export_all_streams(uid, beamline_acronym)
    export_tes(uid, beamline_acronym)
