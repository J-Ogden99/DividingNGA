import pandas as pd
import geopandas as gpd

df = pd.read_parquet('WithTermPA/10s_africa_1020027430_1.parquet')
link_nos = ', '.join([f"{x}" for x in df["LINKNO"]])
print(link_nos)

gdf = gpd.read_file('DividingNGA/tdxhydro_streams_50s_australia/TDX_streamnet_5020049720_01.gpkg',
                    where=f"LINKNO IN ({link_nos})")
gdf.to_file('5020049720_1.gpkg')
