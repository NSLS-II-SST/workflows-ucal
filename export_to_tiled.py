from export_to_xdi import get_xdi_normalized_data, get_xdi_run_header, make_filename
import xarray as xr


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
                rixs = xr.DataArray(counts.T, coords={"emission": energy_grid[:, 0]}, dims=("time", "emission"))
            else:
                rixs = xr.DataArray(data, dims=("time", "emission"))
            da_dict[name] = rixs
        else:
            da_dict[name] = xr.DataArray(data, dims=("time",))

    if "time" in da_dict:
        time_coord = da_dict.pop("time")
        for name, da in da_dict.items():
            da.coords["time"] = time_coord
    da = xr.merge(da_dict.values())
    return da
