import numpy as np
from os.path import exists, join
from export_tools import get_with_fallbacks, get_run_data, add_comment_to_lines
from datetime import datetime
from prefect import get_run_logger


def get_config(config, keys, default=None):
    item = get_with_fallbacks(config, keys)
    try:
        return item.read()
    except:
        return default


def get_xdi_run_header(run):
    baseline = run.baseline.data.read()
    metadata = {}
    metadata["Facility.name"] = "NSLS-II"
    metadata["Facility.xray_source"] = "EPU60 Undulator"
    metadata["Facility.current"] = float(get_with_fallbacks(baseline, "NSLS-II Ring Current", default=[400])[0])

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

    metadata["Element.symbol"] = run.start.get("element", "")
    metadata["Element.edge"] = run.start.get("edge", "")
    # This is just a kludge for re-export of old data where we used edge, not element in run.start
    if metadata["Element.symbol"] == "" and metadata["Element.edge"] != "":
        element = metadata["Element.edge"]
        metadata["Element.symbol"] = element
        metadata["Element.edge"] = ""  # Because it was really the element symbol
        if element.lower() in ["c", "n", "o", "f", "na", "mg", "al", "si"]:
            metadata["Element.edge"] = "K"
        elif element.lower() in ["ca", "sc", "ti", "v", "cr", "mn", "fe", "co", "ni", "cu", "zn"]:
            metadata["Element.edge"] = "L"
        elif element.lower() in ["ce"]:
            metadata["Element.edge"] = "M"

    proposal = run.start.get("proposal", {})
    metadata["Proposal.id"] = proposal.get("proposal_id", "")
    metadata["Proposal.pi"] = proposal.get("pi_name", "")
    metadata["Proposal.cycle"] = run.start.get("cycle", "")
    metadata["Proposal.start"] = run.start.get("start_datetime", "")

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
    logger = get_run_logger()

    metadata = get_xdi_run_header(run)
    metadata.update(headerUpdates)
    logger.info("Got XDI Metadata")
    file_parts = ["scan"]
    file_parts.append(str(metadata.get("Scan.id")))
    if metadata.get("Sample.name", "") != "":
        file_parts.append(metadata.get("Sample.name"))
    if metadata.get("Element.symbol", "") != "":
        file_parts.append(metadata.get("Element.symbol"))

    filename = join(folder, "_".join(file_parts) + ".xdi")

    if verbose:
        print(f"Exporting to {filename}")

    columns, run_data, tes_rois = get_run_data(run, omit=["tes_scan_point_start", "tes_scan_point_end"])
    for c in columns:
        if c in tes_rois:
            metadata[f"{c}.roi"] = "{:.2f} {:.2f}".format(*tes_rois[c])
    logger.info("Got XDI Data")
    # Rename energy columns if present
    if "en_energy" in columns:
        columns[columns.index("en_energy")] = "energy"
    if "en_energy_setpoint" in columns:
        columns[columns.index("en_energy_setpoint")] = "energy_setpoint"

    fmtStr = generate_format_string(run_data)
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
    logger.info(f"Writing to {filename}")
    with open(filename, "w") as f:
        f.write(header_string)
        f.write("\n")
        np.savetxt(f, data, fmt=fmtStr, delimiter=" ")


def generate_format_string(data):
    """
    Generate a format string for numpy.savetxt based on data type and average value.

    Parameters
    ----------
    data : np.ndarray
        The input data array.

    Returns
    -------
    str
        A format string for numpy.savetxt.
    """
    formats = []
    for column_data in data:
        if np.issubdtype(column_data.dtype, np.integer):
            width = len(str(np.max(np.abs(column_data))))
            formats.append(f"%{width}d")
        else:
            avg_value = np.mean(column_data)
            max_value = np.max(np.abs(column_data))
            if np.abs(avg_value) < 1:
                formats.append("%.4e")
            else:
                width = len(str(int(max_value))) + 4  # Add 4 for decimal point and 3 decimals
                formats.append(f"%{width}.3f")

    return " ".join(formats)
