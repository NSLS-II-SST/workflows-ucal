import numpy as np
from os.path import exists, join
from export_tools import get_with_fallbacks, get_run_data, add_comment_to_lines
from datetime import datetime


def get_config(config, keys, default=None):
    item = get_with_fallbacks(config, keys)
    try:
        return item.read()
    except:
        return default


def get_xdi_run_header(run):
    metadata = {}
    metadata["Facility.name"] = "NSLS-II"
    metadata["Facility.xray_source"] = "EPU60 Undulator"

    metadata["Beamline.name"] = "7-ID-1"
    metadata["Beamline.chamber"] = "NEXAFS"

    metadata["Mono.stripe"] = str(get_config(run.baseline.config, ["en", "en_monoen_gratingx_setpoint"], [""])[0])

    metadata["Sample.name"] = run.start.get("sample_name", "")
    metadata["Sample.id"] = run.start.get("sample_id", "")

    metadata["Scan.id"] = run.start["scan_id"]
    metadata["Scan.uid"] = run.start["uid"]
    metadata["Scan.command"] = run.start.get("plan_name", "")
    metadata["Scan.start_time"] = datetime.fromtimestamp(run.start["time"]).isoformat()
    metadata["Scan.type"] = run.start.get("scantype", "unknown")
    metadata["Scan.motors"] = run.start.get("motors", ["time"])[0]

    metadata["Element.symbol"] = run.start.get("edge", "")
    metadata["Element.edge"] = run.start.get("edge", "")

    proposal = run.start.get("proposal", {})
    metadata["Proposal.id"] = proposal.get("proposal_id", "")
    metadata["Proposal.pi"] = proposal.get("pi_name", "")
    metadata["Proposal.cycle"] = run.start.get("cycle", "")
    metadata["Proposal.start"] = run.start.get("start_datetime", "")

    baseline = run.baseline.data.read()
    metadata["Motors.exslit"] = float(
        get_with_fallbacks(baseline, "eslit", "Exit Slit of Mono Vertical Gap", default=[0])[0]
    )
    metadata["Motors.manipx"] = float(get_with_fallbacks(baseline, "manip_x", "Manipulator_x", default=[0])[0])
    metadata["Motors.manipy"] = float(get_with_fallbacks(baseline, "manip_y", "Manipulator_y", default=[0])[0])
    metadata["Motors.manipz"] = float(get_with_fallbacks(baseline, "manip_z", "Manipulator_z", default=[0])[0])
    metadata["Motors.manipr"] = float(get_with_fallbacks(baseline, "manip_r", "Manipulator_r", default=[0])[0])
    metadata["Motors.tesz"] = float(get_with_fallbacks(baseline, "tesz", default=[0])[0])
    return metadata


def exportToXDI(
    folder,
    run,
    headerUpdates={},
    strict=False,
    verbose=True,
    increment=True,
):
    """
    Export data to the XAS-Data-Interchange (XDI) ASCII format.

    Parameters
    ----------
    folder : str
        Export directory where the XDI file will be saved.
    data : np.ndarray
        Numpy array containing the data with dimensions (npts, ncols).
    header : dict
        Dictionary containing   metadata', 'motors', and 'channelinfo' sub-dictionaries.
    namefmt : str, optional
        Format string for the output filename, default is "scan_{scan}.xdi".
    exit_slit : str, optional
        Exit slit information to include as an extension field.
    scan_uid : str, optional
        Unique identifier for the scan to include as an extension field.
    facility : str, optional
        Name of the facility, default is "NSLS-II".
    beamline : str, optional
        Name of the beamline, default is "7ID1 (NEXAFS)".
    xray_source : str, optional
        Description of the X-ray source, default is "EPU60 Undulator".
    headerUpdates : dict, optional
        Dictionary of additional header fields to update or add.
    strict : bool, optional
        If True, ensures certain header fields are not lists.
    verbose : bool, optional
        If True, prints export status messages.
    increment : bool, optional
        If True, increments the filename if it already exists to prevent overwriting.

    Returns
    -------
    None
    """

    metadata = get_xdi_run_header(run)
    metadata.update(headerUpdates)

    file_parts = ["scan"]
    file_parts.append(str(metadata.get("Scan.id")))
    if metadata.get("Sample.name", "") != "":
        file_parts.append(metadata.get("Sample.name"))
    if metadata.get("Element.symbol", "") != "":
        file_parts.append(metadata.get("Element.symbol"))

    filename = join(folder, "_".join(file_parts) + ".xdi")

    if verbose:
        print(f"Exporting to {filename}")

    columns, run_data = get_run_data(run, omit=["tes_scan_point_start", "tes_scan_point_end"])
    # Rename energy columns if present
    if "en_energy" in columns:
        columns[columns.index("en_energy")] = "energy"
    if "en_energy_setpoint" in columns:
        columns[columns.index("en_energy_setpoint")] = "energy_setpoint"
    data = np.vstack(run_data).T
    colStr = " ".join(columns)

    header_lines = ["# XDI/1.0 SST-1-NEXAFS/1.0"]
    for key, value in metadata.items():
        header_lines.append(f"# {key}: {value}")
    header_lines.append("# ///")
    header_lines.append(add_comment_to_lines(run.start.get("comment", "")))
    header_lines.append("#" + "-" * 50)
    header_lines.append("# " + colStr)
    header_string = "\n".join(header_lines)

    with open(filename, "w") as f:
        f.write(header_string)
        f.write("\n")
        np.savetxt(f, data, fmt="%8.8e", delimiter=" ")

    if verbose:
        print(f"Exported XDI ASCII file to {filename}")