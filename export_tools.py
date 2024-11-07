import datetime
import numpy as np


def get_with_fallbacks(thing, *possible_names, default=None):
    for name in possible_names:
        if isinstance(name, (list, tuple)):
            for subname in name:
                if subname in thing:
                    thing = thing[subname]
                    found_thing = True
                else:
                    found_thing = False
            if found_thing:
                return thing
        elif name in thing:
            return thing[name]
    return default


def get_header_and_data(run):
    cols, run_data = get_run_data(run)
    header = get_run_header(run)
    header["channelinfo"]["cols"] = cols
    data = np.vstack(run_data).T
    return header, data


def get_run_header(run):
    metadata = {}
    scaninfo = {}
    scaninfo["scan"] = run.start["scan_id"]
    scaninfo["date"] = datetime.datetime.fromtimestamp(run.start["time"]).isoformat()
    scaninfo["sample"] = run.start.get("sample_name", "")
    scaninfo["loadid"] = run.start.get("sample_id", "")

    scaninfo["command"] = get_with_fallbacks(run.start, "command", "plan_name", default=None)
    scaninfo["motor"] = run.start.get("motors", ["time"])[0]
    scankeys = [
        "time",
        "data_session",
        "cycle",
        "start_datetime",
        "repeat",
    ]
    for k in scankeys:
        if k in run.start:
            scaninfo[k] = run.start[k]
    if "ref_args" in run.start:
        scaninfo["ref_edge"] = run.start["ref_args"]["i0up_multimesh_sample_sample_name"]["value"]
        scaninfo["ref_id"] = run.start["ref_args"]["i0up_multimesh_sample_sample_id"]["value"]
    scaninfo["uid"] = run.start["uid"]
    motors = {}
    baseline = run.baseline.data.read()
    motors["exslit"] = get_with_fallbacks(baseline, "eslit", "Exit Slit of Mono Vertical Gap")[0].item()
    motors["manipx"] = float(get_with_fallbacks(baseline, "manip_x", "Manipulator_x", default=[0])[0])
    motors["manipy"] = float(get_with_fallbacks(baseline, "manip_y", "Manipulator_y", default=[0])[0])
    motors["manipz"] = float(get_with_fallbacks(baseline, "manip_z", "Manipulator_z", default=[0])[0])
    motors["manipr"] = float(get_with_fallbacks(baseline, "manip_r", "Manipulator_r", default=[0])[0])
    motors["samplex"] = float(get_with_fallbacks(baseline, "manip_sx", "Manipulator_sx", default=[0])[0])
    motors["sampley"] = float(get_with_fallbacks(baseline, "manip_sy", "Manipulator_sy", default=[0])[0])
    motors["samplez"] = float(get_with_fallbacks(baseline, "manip_sz", "Manipulator_sz", default=[0])[0])
    motors["sampler"] = float(get_with_fallbacks(baseline, "manip_sr", "Manipulator_sr", default=[0])[0])
    motors["tesz"] = float(get_with_fallbacks(baseline, "tesz", default=[0])[0])
    metadata["scaninfo"] = scaninfo
    metadata["motors"] = motors
    metadata["channelinfo"] = {}
    return metadata


def get_run_data(run, omit=[]):
    natural_order = [
        "en_energy",
        "en_energy_setpoint",
        "time",
        "seconds",
        "nexafs_i0up",
        "nexafs_i1",
        "nexafs_ref",
        "nexafs_sc",
        "nexafs_pey",
    ]
    config = run.primary.descriptors[0]["configuration"]
    exposure = get_with_fallbacks(
        config,
        ["nexafs_i0up", "data", "nexafs_i0up_exposure_time"],
        ["nexafs_i1", "data", "nexafs_i0up_exposure_time"],
        ["nexafs_sc", "data", "ucal_sc_exposure_time"],
    )
    if exposure is None:
        exposure = 0
    exposure = float(exposure)
    columns = []
    datadict = {}
    known_array_keys = ["tes_mca_spectrum", "spectrum"]

    keys = run.primary.data.keys()
    usekeys = []

    for key in keys:
        if key in known_array_keys:
            continue
        usekeys.append(key)
    data = run.primary.data.read(usekeys)
    for key in usekeys:
        if len(data[key].shape) == 1:
            datadict[key] = data[key].data
    if "seconds" not in datadict:
        datadict["seconds"] = np.zeros_like(datadict[key]) + exposure
    for k in natural_order:
        if k in datadict.keys() and k not in omit:
            columns.append(k)
    for k in datadict.keys():
        if k not in columns and k not in omit:
            columns.append(k)
    data = [datadict[k] for k in columns]
    return columns, data


def add_comment_to_lines(multiline_string, comment_char="#"):
    """
    Adds a comment character to the beginning of each line in a multiline string.

    Parameters
    ----------
    multiline_string : str
        The input multiline string.

    Returns
    -------
    str
        The multiline string with comment characters added to each line.
    """
    commented_lines = [f"{comment_char} " + line for line in multiline_string.split("\n")]
    return "\n".join(commented_lines)
