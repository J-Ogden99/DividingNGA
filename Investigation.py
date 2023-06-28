import os

from Division import divide_nga

code = '7020038340'
region = '70s_northamerica'
out_dir = f'{region}_divided'
stream_gpkg = os.path.join('DividingNGA', f'tdxhydro_streams_{region}', f'TDX_streamnet_{code}_01.gpkg')
# gdf_path = os.path.join('DividingNGA', f'tdxhydro_basins_{region}', f'TDX_streamreach_basins_{code}_01.gpkg')

divide_nga(stream_gpkg, out_dir, code, region)
