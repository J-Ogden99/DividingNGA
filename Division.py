import json
import os
import shutil

import geopandas as gpd
import pandas as pd

from AdjoinAsGraph import create_adjoint_dict


def divide_nga(stream_file: str, out_dir: str, code: str, region: str, n: int = 10, adjoint_tree: str = None):
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
    streams['TERMINALPA'] = -1
    full_basins = {}
    basin_sizes = {}
    for id in drain_to_ocean:
        full_basins[str(id)] = streams_tree[str(id)]
        streams.loc[streams['LINKNO'].isin(streams_tree[str(id)]), 'TERMINALPA'] = id
        basin_sizes[str(id)] = len(full_basins[str(id)])

    # Add adjoint basin size to df, sort by that
    region_lists = [[] for i in range(n)]
    terminal_at = {}
    streams['AdjSize'] = streams['TERMINALPA'].apply(lambda x: basin_sizes[str(x)] if x != -1 else 1)
    streams = streams.sort_values('AdjSize', ascending=False).reset_index(drop=True)
    for i in range(len(streams)):
        link_no = streams.loc[i, 'LINKNO']
        t_pa = streams.loc[i, 'TERMINALPA']
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

    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir)

    for i, lst in enumerate(region_lists):
        streams.loc[streams['LINKNO'].isin(lst)].to_parquet(os.path.join(out_dir, f'{region}_{code}_{i + 1}.parquet'))
        print(f'{region} {i + 1} Size: {len(lst)}')

    # gdf = gpd.read_file(gdf_path)
    # gdf['TERMINALPA'] = streams['TERMINALPA']
    # gdf.to_file(f'TDX_streamreach_basins_{code}.gpkg', driver="GPKG")
