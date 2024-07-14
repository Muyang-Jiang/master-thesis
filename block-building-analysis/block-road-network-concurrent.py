import geopandas as gpd
import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import networkx as nx
import osmnx as ox
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed

accessible_block_save_path = "/data-sat-img/postprocessed/mujiang/master-thesis/accessible_block/"
block_categorized_savepath = "/data-sat-img/RAW/Block/result/block_categorized/"
osmnx_graph_path = "/data-sat-img/postprocessed/mujiang/master-thesis/osmnx_graph/"

def create_accessible_blocks(city, export_file_name, search_distance=1500, buffer_distance=10):
    # read clustered block
    year = 2020
    block_file = os.path.join(block_categorized_savepath, city, f"{city}-{year}-block_categorized.shp")
    
    try:
        block_gdf = gpd.read_file(block_file)
    except Exception as e:
        print(f"Error in reading {city} block file: {e}")
        return
    
    # read road network nodes and edges in gpkg format
    try:
        osmnx_graph_nodes = gpd.read_file(os.path.join(osmnx_graph_path, f"{city}_graph.gpkg"), layer="nodes")
        # osmnx_graph_edges = gpd.read_file(os.path.join(osmnx_graph_path, f"{city}_graph.gpkg"), layer="edges")
    except Exception as e:
        print(f"Error in reading {city} graph file: {e}")
        return

    # read road network graph
    try:
        osmnx_graph = ox.load_graphml(os.path.join(osmnx_graph_path, f'{city}_graph.graphml'))
    except Exception as e:
        print(f"Error in reading {city} graph file: {e}")
        return

    # Pre-compute buffered geometries and join results
    buffered_geometries = block_gdf['geometry'].buffer(buffer_distance)
    buffered_blocks_gdf = block_gdf.copy()
    buffered_blocks_gdf['geometry'] = buffered_geometries
    buffered_blocks_gdf = buffered_blocks_gdf.to_crs("4326")
    joined_result = gpd.sjoin(osmnx_graph_nodes, buffered_blocks_gdf, how="inner", predicate='within')
    
    # Create osmid_block_list and block_osmid_list
    osmid_block_list = joined_result[["osmid", "block_id"]].groupby('osmid').agg(list).reset_index()
    block_osmid_list = joined_result[["osmid", "block_id"]].groupby('block_id').agg(list).reset_index()

    # Create accessible blocks
    accessible_blocks_dist_dict_list = []

    # Pre-compute subgraphs for each node
    print("Create subgraphs for each node")
    subgraphs = {}
    for osmid in tqdm(osmid_block_list["osmid"]):
        subgraphs[osmid] = nx.ego_graph(osmnx_graph, osmid, radius=search_distance, distance='length')

    print("Find accessible blocks and calculate distances")
    for index, row in tqdm(block_osmid_list.iterrows(), total=len(block_osmid_list)):
        osmid_list = row["osmid"]
        access_dist_dict = dict()

        for block_osmid in osmid_list:
            subgraph = subgraphs[block_osmid]
            accessible_nodes = subgraph.nodes()

            for accessible_node in accessible_nodes:
                access_distance = nx.shortest_path_length(osmnx_graph, source=block_osmid, target=accessible_node, weight='length')
                accessible_blocks = list(osmid_block_list[osmid_block_list['osmid'].isin([accessible_node])]["block_id"])

                if accessible_blocks:
                    accessible_blocks = accessible_blocks[0]
                    for accessible_block in accessible_blocks:
                        if accessible_block not in access_dist_dict:
                            access_dist_dict[accessible_block] = []
                        access_dist_dict[accessible_block].append(access_distance)

        if access_dist_dict:
            mean_access_dist_dict = {key: statistics.mean(values) for key, values in access_dist_dict.items()}
            accessible_blocks_dist_dict_list.append(mean_access_dist_dict)
        else:
            accessible_blocks_dist_dict_list.append({})

    block_osmid_list["accessible_blocks"] = accessible_blocks_dist_dict_list

    # Save the result
    block_osmid_list.to_csv(export_file_name)
    print("Exported result to", export_file_name)

def process_city(city):
    search_distance = 1500
    buffer_distance = 10
    export_file_name = os.path.join(accessible_block_save_path, str(search_distance), f"{city}-accessible_blocks.csv")
    if not os.path.exists(export_file_name) and city == "Shanghai":
        print(f"Creating accessible blocks for {city}")
        create_accessible_blocks(city, export_file_name, search_distance, buffer_distance)

if __name__ == "__main__":
    city_list = os.listdir(block_categorized_savepath)
    city_list = [city for i, city in enumerate(city_list) if 21 <= i < 44]
    print(city_list)

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(process_city, city): city for city in city_list}
        for future in as_completed(futures):
            city = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Error processing {city}: {e}")
