import h5py

from export_to_xdi import get_xdi_normalized_data, get_xdi_run_header, make_filename


def exportToHDF5(folder, run, header_updates={}):
    """
    Export a run to an HDF5 file.

    Parameters
    ----------
    run : Run
    folder : str
    """

    if "primary" not in run:
        print(f"HDF5 Export does not support streams other than Primary, skipping {run.start['scan_id']}")
        return False
    metadata = get_xdi_run_header(run, header_updates)
    print("Got XDI Metadata")
    filename = make_filename(folder, metadata, "hdf5")
    print(f"Exporting to {filename}")

    columns, run_data, metadata = get_xdi_normalized_data(run, metadata, omit_array_keys=False)

    with h5py.File(filename, "w") as f:
        for name, data in zip(columns, run_data):
            if name == "rixs":
                if len(data) == 3:
                    counts, mono_grid, energy_grid = data
                    g = f.create_group("rixs")
                    g.create_dataset("motor_values", data=mono_grid[0, :])
                    g.create_dataset("emission_energies", data=energy_grid[:, 0])
                    g.create_dataset("counts", data=counts)
                else:
                    f.create_dataset(name, data=data)
            else:
                f.create_dataset(name, data=data)
        for key, value in metadata.items():
            f.attrs[key] = value

    return True
