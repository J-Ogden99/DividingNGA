import json
import os
import shutil
from glob import glob

import geopandas as gpd
import pandas as pd

from AdjoinAsGraph import create_adjoint_dict


def divide_nga(stream_file: str, out_dir: str, region: str, n: int = 10, max_size: int = None, adjoint_tree: str = None):
    """
    Splits a network file from the NGA delineation into smaller pieces and writes them to parquets, for ease of
    computation. Pieces are composed of complete adjoint basins, but the constituent basins within a piece don't
    necessarily border each other geographically.
    Args:
        stream_file (str): Path to network file. Can be .csv, .parquet, .shp, or .gpkg.
        out_dir (str): Path to directory which will contain the output pieces.
        region (str): Unique region/VPU name or id, to be used in naming output files
        n (int, optional): Number of parts to break region into. Defaults to 10.
        max_size (int, optional): If defined, gives a maximum number of stream segments that a piece will contain,
                                  and n is recalculated based on this number. Defaults to None.
        adjoint_tree (str, optional): If defined, gives the path to pre-made dictionary created from AdjoinAsGraph and
                                      stored in .json format. Otherwise this dictionary will be created during the
                                      course of the run. Defaults to None.
    """
    # Read file if of acceptable type
    ext = os.path.splitext(stream_file)[1]
    if ext == ".shp" or ext == ".gpkg":
        streams = gpd.read_file(stream_file)
    elif ext == ".csv":
        streams = pd.read_csv(stream_file)
    elif ext == ".parquet":
        streams = pd.read_parquet(stream_file)
    else:
        print("Invalid file type")
        return

    # Read tree if provided, otherwise create it
    if adjoint_tree:
        with open(adjoint_tree) as f:
            streams_tree = json.load(f)
    else:
        streams_tree = create_adjoint_dict(streams)

    # Create TERMINALPA column, get basin sizes
    drain_to_ocean = streams[streams['DSLINKNO'] == -1]['LINKNO'].tolist()
    full_basins = {}
    basin_sizes = {}
    for id in drain_to_ocean:
        full_basins[str(id)] = streams_tree[str(id)]
        basin_sizes[str(id)] = len(full_basins[str(id)])

    # Add adjoint basin size to df, sort by that, also create empty region_lists to contain ids of rivers in each piece
    if max_size:
        new_n = int(len(streams) / max_size) + 1
        n = new_n if new_n > n else n  # Only change if new number of parts is more than the defined amount
    region_lists = [[] for i in range(n)]
    terminal_at = {}
    streams['AdjSize'] = \
        streams.apply(
            lambda row: basin_sizes[str(int(row['TerminalNode']))] if int(row['TerminalNode']) != int(
                row['LINKNO']) else 1, axis=1)
    streams = streams.sort_values('AdjSize', ascending=False).reset_index(drop=True)

    # For each segment (row), spread the ids across the region_lists to evenly disperse basins by size, putting ids from
    # the same adjoint basins together.
    for i in range(len(streams)):
        link_no = int(streams.iloc[i]['LINKNO'])
        t_pa = int(streams.iloc[i]['TerminalNode'])
        region_no = i % n
        if t_pa == -1:
            region_lists[region_no].append(link_no)
            continue
        if str(t_pa) in terminal_at:
            region_lists[terminal_at[str(t_pa)]].append(link_no)
        else:
            # print(len(region_lists[region_no]))
            if len(region_lists[region_no]) > len(streams) / n:
                j = 1
                while len(region_lists[(i + j) % n]) > len(streams) / n:
                    j += 1
                region_lists[(i + j) % n].append(link_no)
                terminal_at[str(t_pa)] = (i + j) % n
            else:
                region_lists[region_no].append(link_no)
                terminal_at[str(t_pa)] = region_no

    # Start with fresh directory, select pieces of streams using the region_lists and write each piece separately
    if os.path.exists(out_dir):
        for file in glob(os.path.join(out_dir, f'{region}_*.parquet')):
            os.remove(file)
    else:
        os.makedirs(out_dir)

    for i, lst in enumerate(region_lists):
        streams.loc[streams['LINKNO'].isin(lst)].to_parquet(os.path.join(out_dir, f'{region}_{i + 1}.parquet'))
        print(f'{region} {i + 1} Size: {len(lst)}')
