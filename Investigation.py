import os

from Division import divide_nga
from AdjoinAsGraph import create_adjoint_dict

code = '7020038340'
region = '50s_australia'
out_dir = f'{region}_divided'
tree_path = f'{region}_tree.json'
stream_file = os.path.join('DividingNGA', f'tdxhydro_streams_{region}', f'rapid_inputs_master.parquet')
# gdf_path = os.path.join('DividingNGA', f'tdxhydro_basins_{region}', f'TDX_streamreach_basins_{code}_01.gpkg')

# create_adjoint_dict(stream_file, out_file=tree_path) # Precompile and write adjoint dictionary for use in division
# divide_nga(stream_file, out_dir, region, max_size=50_000, adjoint_tree=tree_path)
divide_nga(stream_file, out_dir, region, max_size=50_000)
