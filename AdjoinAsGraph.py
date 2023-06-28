import json
import os

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


def create_adjoint_dict(network_df, out_file: str = None, stream_id_col: str = "LINKNO",
                        next_down_id_col: str = "DSLINKNO", order_col: str = "strmOrder", trace_up: bool = True,
                        order_filter: int = 0):
    """
        Creates a dictionary where each unique id in a stream network is assigned a list of all ids upstream or downstream
        of that stream, as specified. By default is designed to trace upstream on GEOGloWS Delineation Catchment shapefiles,
        but can be customized for other files with column name parameters, customized to trace down, or filtered by stream
        order. If filtered by stream order, the dictionary will only contain ids of the given stream order, with the
        upstream or downstream ids for the other streams in the chain that share that stream order.
        Args:
            network_df:  geopandas geodataframe that contains the stream network or path to read for it. This file
                         must contain attributes for a unique id and a next down id, and if filtering by order number is
                         specified, it must also contain a column with stream order values.
            out_file: a path to an output file to write the dictionary as a .json, if desired.
            stream_id_col: the name of the column that contains the unique ids for the stream segments
            next_down_id_col: the name of the column that contains the unique id of the next down stream for each row, the
                              one that the stream for that row feeds into.
            order_col: name of the column that contains the stream order
            trace_up: if true, trace up from each stream, otherwise trace down.
            order_filter: if set to number other than zero, limits values traced to only ids that match streams with that
                          stream order

        Returns:
            The dictionary
    """
    if isinstance(network_df, str):
        ext = os.path.splitext(network_df)[1]
        if ext == ".shp" or ext == ".gpkg":
            network_df = gpd.read_file(network_df)
        elif ext == ".csv":
            network_df = pd.read_csv(network_df)
        elif ext == ".parquet":
            network_df = pd.read_parquet(network_df)
        else:
            print("Invalid file type")
            return {}
    elif not (isinstance(network_df, pd.DataFrame) or isinstance(network_df, gpd.GeoDataFrame)):
        print("network_df is invalid type")
        return {}
    columns_to_search = [stream_id_col, next_down_id_col]
    if order_filter != 0:
        columns_to_search.append(order_col)
    for col in columns_to_search:
        if col not in network_df.columns:
            print(f"Column {col} not present")
            return {}

    g = nx.DiGraph()

    network_df.apply(lambda row: g.add_edge(row[next_down_id_col], row[stream_id_col]), axis=1)

    upstream_dict = {}

    if order_filter != 0:
        if trace_up:
            upstream_dict = {str(node): [int(node), ] + list(map(int, nx.descendants(g, node))) for node in
                             network_df[(network_df[order_col] == order_filter)][stream_id_col]}
        else:
            upstream_dict = {str(node): [int(node), ] + list(map(int, nx.ancestors(g, node))) for node in
                             network_df[network_df[order_col] == order_filter][stream_id_col]}
    else:
        if trace_up:
            upstream_dict = {str(node): [int(node), ] + list(map(int, nx.descendants(g, node))) for node in
                             network_df[stream_id_col]}
            for node in g.nodes:
                if node != -1:
                    upstream_dict[str(int(node))] = [int(node), ] + list(map(int, nx.descendants(g, node)))
        else:
            # upstream_dict = {str(node): [int(node), ] + list(map(int, nx.ancestors(g, node))) for node in
            #                  network_df[stream_id_col]}
            for node in g.nodes:
                if node != -1:
                    upstream_dict[str(int(node))] = [int(node), ] + list(map(int, nx.ancestors(g, node)))
    if out_file is not None:
        if not os.path.exists(out_file):
            with open(out_file, "w") as f:
                json.dump(upstream_dict, f, cls=NpEncoder)
        else:
            print("File already created")
            return {}
    return upstream_dict


if __name__ == "__main__":
    adj_dict_2 = create_adjoint_dict('DividingNGA/tdxhydro_streams_50s_australia/rapid_inputs_master.parquet',
                                     stream_id_col="LINKNO",
                                     next_down_id_col="DSLINKNO",
                                     order_col="strmOrder",
                                     out_file="")

    with open("DividingNGA/tdxhydro_streams_10s_africa/adjoint_tree.json") as f:
        adj_dict = json.load(f)

    for id in adj_dict_2:
        if id not in adj_dict:
            print(id)


    # outlets = df.loc[df['DSLINKNO'] == -1, 'LINKNO'].values
    # for outlet in outlets
    #       list_of_ancestors = nx.ancestors(g, outlet)
    #       df[df['LINKNO'].isin(list_of_ancestors), 'TERMINALID'] = outlet

