import os
import json
import numpy as np
import pandas as pd
import geopandas as gpd

from AdjointCatchments.AdjoinUpdown import create_adjoint_dict

aus_code = 5020049720
af_code = 1020027430

aus_stream = pd.read_parquet(os.path.join('DividingNGA', 'tdxhydro_streams_50s_australia',
                                          f'rapid_inputs_master.parquet'))
with open(os.path.join('DividingNGA', 'tdxhydro_streams_50s_australia', 'adjoint_tree.json')) as f:
    aus_stream_dict = json.load(f)
af_stream = pd.read_parquet(os.path.join('DividingNGA', 'tdxhydro_streams_10s_africa',
                                         f'rapid_inputs_master.parquet'))
print("here")
with open(os.path.join('DividingNGA', 'tdxhydro_streams_10s_africa', 'adjoint_tree.json')) as f:
    af_stream_dict = json.load(f)
print("here")

for df, adj_dict, code, region in zip([aus_stream, af_stream], [aus_stream_dict, af_stream_dict], [aus_code, af_code],
                                      ['50s_australia', '10s_africa']):
    drain_to_ocean = df[df['DSLINKNO'] == -1]['LINKNO'].tolist()
    df['TERMINALPA'] = -1
    full_basins = {}
    basin_sizes = {}
    for id in drain_to_ocean:
        full_basins[str(id)] = adj_dict[str(id)]
        df.loc[df['LINKNO'].isin(adj_dict[str(id)]), 'TERMINALPA'] = id
        basin_sizes[str(id)] = len(full_basins[str(id)])
    sorted_basin_sizes = dict(sorted(basin_sizes.items(), key=lambda x: x[1], reverse=True))
    # print(sorted_basin_sizes)
    n = 10
    region_lists = [[] for i in range(n)]
    terminal_at = {}
    df['AdjSize'] = df['TERMINALPA'].apply(lambda x: basin_sizes[str(x)] if x != -1 else 1)
    df = df.sort_values('AdjSize', ascending=False)
    for i in range(len(df)):
        link_no = df.loc[i, 'LINKNO']
        t_pa = df.loc[i, 'TERMINALPA']
        region_no = i % n
        if t_pa == -1:
            region_lists[region_no].append(link_no)
            continue
        if str(t_pa) in terminal_at:
            region_lists[terminal_at[str(t_pa)]].append(link_no)
        else:
            # print(len(region_lists[region_no]))
            if len(region_lists[region_no]) > len(df) / n:
                j = 1
                while len(region_lists[(i + j) % n]) > len(df) / n:
                    j += 1
                region_lists[(i + j) % n].append(link_no)
                terminal_at[str(t_pa)] = (i + j) % n
            else:
                region_lists[region_no].append(link_no)
                terminal_at[str(t_pa)] = region_no

    for i, lst in enumerate(region_lists):
        df.loc[df['LINKNO'].isin(lst)].to_parquet(os.path.join('WithTermPA', f'{region}_{code}_{i + 1}.parquet'))
        print(f'Region {i + 1} Size: {len(lst)}')

    # gdf_path = os.path.join('DividingNGA', f'tdxhydro_basins_{region}', f'TDX_streamreach_basins_{code}_01.gpkg')
    # print(gdf_path)
    # gdf = gpd.read_file(gdf_path)
    # print("here")
    # gdf['TERMINALPA'] = df['TERMINALPA']
    # gdf.to_file(f'TDX_streamreach_basins_{code}.gpkg', driver="GPKG")
