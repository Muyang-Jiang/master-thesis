import geopandas as gpd
import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import networkx as nx
import osmnx as ox
import statistics
import csv

accessible_block_save_path = "/data-sat-img/postprocessed/mujiang/master-thesis/accessible_block/"
block_categorized_savepath = "/data-sat-img/RAW/Block/result/block_categorized/"
osmnx_graph_path = "/data-sat-img/postprocessed/mujiang/master-thesis/osmnx_graph/"

def create_accessible_blocks(city, export_file_name, search_distance=1500, buffer_distance=10):
    # read clustered block
    year = 2020
    block_file = os.path.join(block_categorized_savepath, city, f"{city}-{year}-block_categorized.shp")
    
    try:
        block_gdf = gpd.read_file(block_file)
    except:
        print(f"Error in reading {city} block file")
        return
    
    # read road network nodes and edges in gpkg format
    try:
        osmnx_graph_nodes = gpd.read_file(os.path.join(osmnx_graph_path, f"{city}_graph.gpkg"), layer="nodes")
    except:
        print(f"Error in reading {city} graph file")
        return

    # read road network graph
    try:
        osmnx_graph = ox.load_graphml(os.path.join(osmnx_graph_path, f'{city}_graph.graphml'))
    except:
        print(f"Error in reading {city} graph file")
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

    # Pre-compute subgraphs for each node
    print("Creating subgraphs for each node")
    subgraphs = {}
    for osmid in tqdm(osmid_block_list["osmid"]):
        subgraphs[osmid] = nx.ego_graph(osmnx_graph, osmid, radius=search_distance, distance='length')

    # Open the CSV file in append mode
    with open(export_file_name, mode='a', newline='') as file:
        writer = csv.writer(file)
        created_blocks = list(pd.read_csv(export_file_name)["block_id"])
        
        # Write the header if the file is empty
        if os.stat(export_file_name).st_size == 0:
            writer.writerow(['block_id', 'accessible_blocks'])

        print("Finding accessible blocks and calculating distances")
        for index, row in tqdm(block_osmid_list.iterrows(), total=len(block_osmid_list)):            
            if not row["block_id"] in created_blocks:
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
                    # Take mean of the distances
                    mean_access_dist_dict = {key: statistics.mean(values) for key, values in access_dist_dict.items()}
                else:
                    mean_access_dist_dict = {}

                # Append the result to the CSV file
                if mean_access_dist_dict:                
                    while True:
                        try:
                            writer.writerow([row['block_id'], mean_access_dist_dict])
                            break
                        except BlockingIOError:
                            continue  # Retry until the write succeeds

        print("Exported result to", export_file_name)

# create main method
if __name__ == "__main__":
    search_distance = 1500
    buffer_distance = 10
    
    city_list = os.listdir(block_categorized_savepath)
    print(city_list)
    for i, city in enumerate(city_list): 
        export_file_name = os.path.join(accessible_block_save_path, str(search_distance), f"{city}-accessible_blocks-continous.csv")
        if (i < 44) & (i >= 21):
            print(f"Creating accessible blocks for {city}")
            if city == "Shanghai":
                create_accessible_blocks(city, export_file_name, search_distance=1500, buffer_distance=10)
