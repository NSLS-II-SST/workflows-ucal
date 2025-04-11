from prefect import flow, get_run_logger, task
from data_validation import general_data_validation
from end_of_run_export import general_data_export
from process_tes import process_tes
from export_tools import initialize_tiled_client


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc, reprocess_tes=False):
    uid = stop_doc["run_start"]
    logger = get_run_logger()

    general_data_validation(uid)
    catalog = initialize_tiled_client("ucal")
    run = catalog[uid]
    if run.start.get("data_session", "") == "":
        logger.info("No data session found, skipping export")
        return

    process_tes(uid, reprocess=reprocess_tes)
    # Here is where exporters could be added
    exit_status = stop_doc.get("exit_status", "No Status")
    if exit_status == "success":
        general_data_export(uid)
    else:
        logger.info(f"Run had exit status: {exit_status}, skipping export")

    log_completion()
