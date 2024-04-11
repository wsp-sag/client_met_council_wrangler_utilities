import geopandas as gpd
import pandas as pd

from scipy.spatial import cKDTree
from pyproj import CRS
from shapely.geometry import Point, LineString

from .logger import WranglerLogger


def add_opposite_direction_to_link(
    link_gdf, 
    nodes_df,
    links_df):

    """
    create and add the opposite direction of links to a dataframe

    Args:
        links_gdf: Input link dataframe with A and B node information
        nodes_df: Input Wrangler roadway network nodes_df
        links_df: Input Wrangler roadway network links_df

    Returns:
        roadway object
    """

    link_gdf = pd.concat(
        [
            link_gdf,
            link_gdf.rename(columns = {'A' : 'B', 'B' : 'A'})
        ],
        sort = False, 
        ignore_index = True
    )

    link_gdf = pd.merge(
        link_gdf,
        nodes_df[["model_node_id", "X", "Y"]].rename(columns = {"model_node_id" : "A", "X": "A_X", "Y" : "A_Y"}),
        how = "left",
        on = "A"
    )

    link_gdf = pd.merge(
        link_gdf,
        nodes_df[["model_node_id", "X", "Y"]].rename(columns = {"model_node_id" : "B", "X": "B_X", "Y" : "B_Y"}),
        how = "left",
        on = "B"
    )

    link_gdf["geometry"] = link_gdf.apply(
        lambda g: LineString([Point(g.A_X, g.A_Y), Point(g.B_X, g.B_Y)]),
        axis = 1
    )

    link_gdf = gpd.GeoDataFrame(
        link_gdf,
        geometry = link_gdf['geometry'],
        crs = links_df.crs
    )
    
    for c in links_df.columns:
        if c not in link_gdf.columns:
            if c not in ['county', 'shstGeometryId', 'cntype']:
                link_gdf[c] = 0
            else:
                link_gdf[c] = ''
    
    link_gdf['A'] = link_gdf['A'].astype(int)
    link_gdf['B'] = link_gdf['B'].astype(int)
    link_gdf = link_gdf.drop(['A_X','A_Y','B_X','B_Y'], axis=1)
    link_gdf = link_gdf.reset_index(drop=True)
    
    return link_gdf



def build_pnr_connections(
    roadway_network = None,
    parameters = None,
    build_pnr_taz_connector:bool = False,
    output_proj = None,
):
    """
    (1) add pnr nodes;
    (2) build links connecting pnr nodes and nearest walk and drive nodes;
    (3) build pnr taz connectors.

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        parameters (Parameters): Lasso parameters object
        build_pnr_taz_connector (Bool): True if building pnr taz connectors. Default to False.
        output_epsg (int): epsg number of output network.

    Returns:
        roadway object
    """

    WranglerLogger.info(
        "Building PNR connections"
    )

    """
    Verify inputs
    """

    output_proj = output_proj if output_proj else parameters.output_proj

    """
    Start actual process
    """

    orig_crs = roadway_network.nodes_df.crs # record original crs
    interim_crs = CRS('epsg:26915') # crs for nearest calculation

    roadway_network.links_df = roadway_network.links_df.to_crs(interim_crs)
    roadway_network.shapes_df = roadway_network.shapes_df.to_crs(interim_crs)
    roadway_network.nodes_df = roadway_network.nodes_df.to_crs(interim_crs)
    roadway_network.nodes_df["X"] = roadway_network.nodes_df["geometry"].x
    roadway_network.nodes_df["Y"] = roadway_network.nodes_df["geometry"].y

    # (1) add pnr nodes
    # read pnr parking location
    pnr_col = ['Zone','X','Y']
    pnr_nodes_df = pd.read_csv(parameters.pnr_node_location)[pnr_col]
    pnr_nodes_df = gpd.GeoDataFrame(
                        pnr_nodes_df, 
                        geometry=gpd.points_from_xy(pnr_nodes_df['X'], pnr_nodes_df['Y']),
                        crs=parameters.output_proj)
    pnr_nodes_df = pnr_nodes_df.to_crs(interim_crs)
    pnr_nodes_df["X"] = pnr_nodes_df["geometry"].apply(lambda g: g.x)
    pnr_nodes_df["Y"] = pnr_nodes_df["geometry"].apply(lambda g: g.y)

    # reformat pnr_nodes_df, add missing columns
    for c in roadway_network.nodes_df.columns:
        if c not in pnr_nodes_df.columns:
            if c in ['drive_access', 'walk_access']:
                pnr_nodes_df[c] = 1
            elif c not in ['county']:
                pnr_nodes_df[c] = 0
            else:
                pnr_nodes_df[c] = ''

    # assign a model_node_id to pnr parking node
    pnr_nodes_df['model_node_id'] = pnr_nodes_df['Zone'] + roadway_network.nodes_df.model_node_id.max()
    # add pnr flag attribute
    pnr_nodes_df['pnr'] = 1

    # add pnr parking nodes to node_df
    roadway_network.nodes_df['pnr'] = 0
    roadway_network.nodes_df = pd.concat([roadway_network.nodes_df, pnr_nodes_df.drop(['Zone'], axis=1)], 
                                sort = False, 
                                ignore_index = True)


    # (2) build links connecting pnr nodes and nearest walk and drive nodes
    # select walk and drive nodes, save to separate lists
    dr_wlk_nodes_df = roadway_network.nodes_df[
            ((roadway_network.nodes_df.drive_access == 1) & (roadway_network.nodes_df.walk_access == 1) ) & 
            ~(roadway_network.nodes_df.model_node_id.isin(pnr_nodes_df['model_node_id'].to_list())) &
            ~(roadway_network.nodes_df.model_node_id.isin(parameters.taz_N_list + parameters.tap_N_list + parameters.maz_N_list))
        ].copy()

    # for each pnr nodes, search for the nearest walk and bike nodes
    dr_wlk_nodes_df = dr_wlk_nodes_df.to_crs(interim_crs)
    dr_wlk_nodes_df['X'] = dr_wlk_nodes_df.geometry.map(lambda g:g.x)
    dr_wlk_nodes_df['Y'] = dr_wlk_nodes_df.geometry.map(lambda g:g.y)
    dr_wlk_node_ref = dr_wlk_nodes_df[['X', 'Y']].values
    tree = cKDTree(dr_wlk_node_ref)

    pnr_nodes_df = pnr_nodes_df.to_crs(interim_crs)
    pnr_nodes_df['X'] = pnr_nodes_df['geometry'].apply(lambda p: p.x)
    pnr_nodes_df['Y'] = pnr_nodes_df['geometry'].apply(lambda p: p.y)

    for index, row in pnr_nodes_df.iterrows():
        point = row[['X', 'Y']].values
        dd, ii = tree.query(point, k = 1)
        pnr_nodes_df.loc[index,'A'] = dr_wlk_nodes_df.iloc[ii].model_node_id 

    # create links between pnr nodes and their nearest walk and bike nodes
    if len(pnr_nodes_df)>0 and ('A' in pnr_nodes_df.columns): #'A' is the nearest walk and drive node
        pnr_nodes_df = pnr_nodes_df[pnr_nodes_df['A'].notna()]
        pnr_link_gdf = pnr_nodes_df[['A', 'model_node_id']].copy()
        pnr_link_gdf.rename(columns = {'model_node_id' : 'B'}, inplace = True)
        
        pnr_link_gdf = add_opposite_direction_to_link(pnr_link_gdf, nodes_df=roadway_network.nodes_df, links_df=roadway_network.links_df)
        
        # specify link variables
        pnr_link_gdf['model_link_id'] = max(roadway_network.links_df['model_link_id']) + pnr_link_gdf.index + 1
        pnr_link_gdf['shstGeometryId'] = pnr_link_gdf.index + 1
        pnr_link_gdf['shstGeometryId'] = pnr_link_gdf['shstGeometryId'].apply(lambda x: "pnr" + str(x))
        pnr_link_gdf['id'] = pnr_link_gdf['shstGeometryId']
        pnr_link_gdf['roadway'] = 'pnr'
        pnr_link_gdf['lanes'] = 1
        pnr_link_gdf['walk_access'] = 1
        pnr_link_gdf['drive_access'] = 1
        pnr_link_gdf['ft'] = 99

        roadway_network.links_df = pd.concat([roadway_network.links_df, pnr_link_gdf], 
                                        sort = False, 
                                        ignore_index = True)
        roadway_network.links_df.drop_duplicates(subset = ['A', 'B'], inplace = True)

        # update shsapes_df
        pnr_shape_df = pnr_link_gdf.copy()
        pnr_shape_df = pnr_shape_df[['id', 'geometry']]
        roadway_network.shapes_df = pd.concat([roadway_network.shapes_df, pnr_shape_df]).reset_index(drop=True)


    # (3) build PNR TAZ connectors
    if build_pnr_taz_connector:
        # select centroids
        centroids_df = roadway_network.nodes_df[roadway_network.nodes_df.model_node_id.isin(parameters.taz_N_list)].copy()
        centroids_df = centroids_df.to_crs(interim_crs)

        # for each centroid, draw a buffer,
        # connect the centroid to all pnr parking nodes that fall in the buffer
        centroid_node_id = []
        pnr_node_id = []

        for index, row in centroids_df.iterrows():
            buffer = row.geometry.buffer(parameters.drive_buffer*1609.34)
            pnr_in_buffer = pnr_nodes_df[pnr_nodes_df.geometry.within(buffer)]

            if len(pnr_in_buffer)>0 and ('A' in pnr_nodes_df.columns): #'A' is the nearest walk and drive node
                for i in range(len(pnr_in_buffer)):
                    centroid_node_id.append(row.model_node_id)
                    pnr_node_id.append(pnr_in_buffer.iloc[i].model_node_id) # pnr connector will be connected to the parking node

        # create links between tazs and pnr nodes in the buffer
        if len(centroid_node_id)>0 and len(pnr_node_id)>0:
            pnr_connector_gdf = pd.DataFrame(list(zip(centroid_node_id, pnr_node_id)), columns=['A','B'])
            pnr_connector_gdf = add_opposite_direction_to_link(pnr_connector_gdf, nodes_df=roadway_network.nodes_df, links_df=roadway_network.links_df)
            
            # specify link variables
            pnr_connector_gdf['model_link_id'] = max(roadway_network.links_df['model_link_id']) + pnr_connector_gdf.index + 1
            pnr_connector_gdf['shstGeometryId'] = pnr_connector_gdf.index + 1
            pnr_connector_gdf['shstGeometryId'] = pnr_connector_gdf['shstGeometryId'].apply(lambda x: "pnrtaz" + str(x))
            pnr_connector_gdf['id'] = pnr_connector_gdf['shstGeometryId']
            pnr_connector_gdf['roadway'] = "pnr"
            pnr_connector_gdf['lanes'] = 1
            pnr_connector_gdf['drive_access'] = 1
            pnr_connector_gdf['ft'] = 100 # ft=100 is pnr taz connector
            
            roadway_network.links_df = pd.concat([roadway_network.links_df, pnr_connector_gdf], 
                                            sort = False, 
                                            ignore_index = True)
            roadway_network.links_df.drop_duplicates(subset = ['A', 'B'], inplace = True)

            # update shsapes_df
            pnr_connector_shape_df = pnr_connector_gdf.copy()
            pnr_connector_shape_df = pnr_connector_shape_df[['id', 'geometry']]
            roadway_network.shapes_df = pd.concat([roadway_network.shapes_df, pnr_connector_shape_df]).reset_index(drop=True)

    roadway_network.links_df = roadway_network.links_df.to_crs(orig_crs)
    roadway_network.shapes_df = roadway_network.shapes_df.to_crs(orig_crs)
    roadway_network.nodes_df = roadway_network.nodes_df.to_crs(orig_crs)
    roadway_network.nodes_df["X"] = roadway_network.nodes_df["geometry"].x
    roadway_network.nodes_df["Y"] = roadway_network.nodes_df["geometry"].y

    return roadway_network