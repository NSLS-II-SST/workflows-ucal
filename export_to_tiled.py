from export_to_xdi import get_xdi_normalized_data, get_xdi_run_header, make_filename
import xarray as xr


def transform_header(metadata):
    """
    Transform the metadata from an XDI format to a nested dictionary format.
    Keys "Namespace.key" become "namespace/key" in the nested dictionary.
    """
    transformed = {}
    for key, value in metadata.items():
        if "." in key:
            namespace, key = key.split(".")
            if namespace not in transformed:
                transformed[namespace] = {}
            transformed[namespace][key] = value
        else:
            transformed[key] = value
    return transformed


def export_to_tiled(run, header_updates={}):
    """
    Export a run to a tiled catalog.

    Parameters
    ----------
    """

    if "primary" not in run:
        print(f"Tiled Export does not support streams other than Primary, skipping {run.start['scan_id']}")
        return False
    metadata = get_xdi_run_header(run, header_updates)
    print("Got XDI Metadata")

    columns, run_data, metadata = get_xdi_normalized_data(run, metadata, omit_array_keys=False)

    da_dict = {}
    for name, data in zip(columns, run_data):
        if name == "rixs":
            if len(data) == 3:
                counts, mono_grid, energy_grid = data
                rixs = xr.DataArray(
                    counts.T, coords={"emission": energy_grid[:, 0]}, dims=("time", "emission"), name=name
                )
            else:
                rixs = xr.DataArray(data, dims=("time", "emission"), name=name)
            da_dict[name] = rixs
        else:
            da_dict[name] = xr.DataArray(data, dims=("time",), name=name)

    if "time" in da_dict:
        time_coord = da_dict.pop("time")
        for name, da in da_dict.items():
            da.coords["time"] = time_coord
    da = xr.merge(da_dict.values())
    metadata = transform_header(metadata)
    data_session = run.start.get("data_session", None)
    if data_session is not None:
        metadata["data_session"] = data_session
    return da, metadata
