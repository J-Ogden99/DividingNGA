import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr

old_del_published = '/Volumes/EB406_T7_2/geoglows_hindcast/20220430_netcdf/africa-geoglows/Qout_era5_t640_24hr_19790101to20220430.nc'
# new_sim = '/Volumes/EB406_T7_1/RAPIDIO/output/fixed_prj_dis_inp/Qout_era5_t640_24hr_19790101to19791231.nc'
# new_sim = '/Users/rchales/NewRapid/TDX_py_generated/Qout_era5_t640_24hr_19790101to19790301.nc'
# new_sim = '/Volumes/EB406_T7_1/RAPIDIO/output/TDX_py_generated/Qout_era5_t640_24hr_19790101to19790301.nc'
# new_sim = '/Volumes/EB406_T7_1/RAPIDIO/output/TDX_py_generated/Qout_era5_t640_24hr_19790101to19791231.nc'
# new_sim = '/Volumes/EB406_T7_1/RAPIDIO/output/TDX_py_generated/Qout_era5_t640_24hr_19790101to19840101.nc'
code = 1020021940
new_sim = f'/Volumes/EB406_T7_2/geoglows2-hindcast/{code}/Qout_era5_t1800_24hr_19500101to20221231.nc'
old_del_pub_ds = xr.open_dataset(old_del_published)
new_ds = xr.open_dataset(new_sim)


def graph_comparison(old_id, new_id):
    df_pub = old_del_pub_ds.sel(rivid=old_id).Qout.to_dataframe()[['Qout']].round(2)
    df_new = new_ds.sel(rivid=new_id).Qout.to_dataframe()[['Qout']].round(2)
    df_pub.columns = [f'Q Pub {old_id}']
    df_new.columns = [f'Q New {new_id}']
    df_new.index = pd.date_range('1950-01-01', '2022-12-31', freq='D')
    # df = df_pub.join(df_new, how='inner', lsuffix='_old_pub', rsuffix='_new')
    df = df_pub.join(df_new, how='outer', lsuffix='_old_pub', rsuffix='_new')
    df = df[df.index.year >= 1990]
    df = df[df.index.year <= 2010]
    df.plot(figsize=(14, 8))
    plt.title(f'Old ID: {old_id} New ID: {new_id}')
    plt.show()
    old_sum = df[f'Q Pub {old_id}'].sum()
    new_sum = df[f'Q New {new_id}'].sum()
    print(f'Pub Sum: {old_sum}')
    print(f'New Sum: {new_sum}')
    print(f'Old Sum / New Sum: {old_sum / new_sum}')
# id_pairs = [
#     # (946916, 100509),
#     # (946305, 105880),
#     # (945746, 50189),
#     # (946839, 97819, 5865643124.29, 118560176),  # wrong - example of old outlets to ocean, new knows that it drains to lake
#     (946916, 99742, 1159947185.66, 1019367936),
#     (946915, 94750, 208114277.40, 178054096),
#     (946911, 88606, 86498346.64, 86956856),
#     (944306, 43640, 734688625, 631298048),
#     (946361, 90519, 231822063.52, 207799920)
# ]
id_pair_df = pd.read_csv('OldVsNewIds.csv')
id_pairs = result = id_pair_df[id_pair_df["NGA Region"] == code].apply(lambda row: tuple(row[['HydroID', 'LINKNO', 'Tot_Drain', 'USContArea']]), axis=1).tolist()

for old_id, new_id, old_area, new_area in id_pairs:
    graph_comparison(old_id, new_id)
    print(f'Old ID: {old_id}')
    print(f'New ID: {new_id}')
    print(f'Old Area: {old_area}')
    print(f'New Area: {new_area}')
    print(f'Old Area / New Area: {old_area / new_area}')
    print()
