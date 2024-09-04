"""Setup Emme project, database (Emmebank) and import network data."""


import os as _os
from collections import defaultdict as _defaultdict
from copy import deepcopy as _copy
from pathlib import Path

from pandas.core.frame import DataFrame

from geopandas.geodataframe import GeoDataFrame
from osgeo import ogr as _ogr
from osgeo import osr as _osr

import inro.emme.database.emmebank as _eb
import inro.emme.desktop.app as _app
import inro.emme.network as _network

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point
from typing import Optional, Union
import numpy as np
import math

from scipy.spatial import cKDTree
from pyproj import CRS

from .roadway import ModelRoadwayNetwork
from .parameters import Parameters
from .logger import WranglerLogger

try:
    import ranch
except ImportError:
    WranglerLogger.warning("'Ranch' is not installed in this environment, if you wish to rebuild connectors when writing the emme network you will need to install ranch https://github.com/BayAreaMetro/Ranch")
    ranch = None

from .mtc import _is_express_bus, _special_vehicle_type

from lasso import StandardTransit

from lasso import mtc

_join = _os.path.join
_dir = _os.path.dirname

def _norm(path):
    return _os.path.normcase(_os.path.normpath(path))


def create_emme_network(
    roadway_network: Optional[ModelRoadwayNetwork] = None,
    transit_network: Optional[StandardTransit] = None,
    include_transit: Optional[bool] =False,
    links_df: Optional[GeoDataFrame]=None,
    nodes_df: Optional[GeoDataFrame]=None,
    shapes_df: Optional[GeoDataFrame]=None,
    name: Optional[str]="",
    path: Optional[str]="",
    write_drive_network: bool = False,
    write_maz_active_modes_network: bool = False,
    write_tap_transit_network: bool = False,
    write_taz_transit_network: bool = False,
    parameters: Union[Parameters, dict] = {},
    polygon_file_to_split_active_modes_network: Optional[str] = None,
    polygon_variable_to_split_active_modes_network: Optional[str] = None,
    regenerate_connectors: Optional[bool] = False,
    taz_zone_shapefile_path: Optional[str] = None,
    maz_zone_shapefile_path: Optional[str] = None,
):
    """
    method that calls emme to write out EMME network from Lasso network


    Arguments:
        roadway_network: lasso roadway network object, which has the model network for writting out.
                        if roadway_network is not given, then use links_df and nodes_df
        transit_network: lasso transit network object, which has the model network for writting out.
        include_transit: should the emme network include transit, default to False
        links_df: model links database for writting out, if not given, use roadway_network
        nodes_df: model nodes database for writting out, if not given, use roadway_network
        shape_df: model shapes data set for writing out, if not given use roadway_network
        name: scenario name prefix
        path: output dir for emme
        write_drive_network: boolean, True if writing out drive network
        write_maz_active_modes_network: boolean, True if writing out MAZ scale walk and bike network
        write_tap_transit_network: boolean, True if writing out TAP sclae transit network
        parameters: lasso parameters
        polygon_file_to_split_active_modes_network: polygon file for dividing the active modes network into subareas
                                due to emme's limitation on number of links and nodes
        polygon_variable_to_split_active_modes_network: unqiue key for each active modes subarea polygon, 
                                will be used to name the emme file
        regenerate_connectors: boolean, if True connectors will be regenerated using Ranch - Note Ranch must be installed
                                to envoke this option https://github.com/wsp-sag/Ranch.git
        taz_zone_shapefile_path: Path to shapefile containing taz level zones for generating taz centroid connectors
        maz_zone_shapefile_path: Path to shapefile containing maz level zones for generating maz centroid connectors

    Return:
        None. Write out emme networks to the output dir
    """

    _NAME = name

    out_dir = path
    
    model_tables = {}


    if roadway_network:
        links_df = roadway_network.links_mtc_df.copy()
        nodes_df = roadway_network.nodes_mtc_df.sort_values("N").reset_index(drop=True).copy()
        shapes_df = roadway_network.shapes_df.copy()

    elif (len(links_df)>0) & (len(nodes_df)>0):
        links_df = links_df.copy()
        nodes_df = nodes_df.sort_values("N").reset_index(drop=True).copy()
        if shapes_df is not None:
            shapes_df = shapes_df.copy()

    else:
        msg = "Missing roadway network to write to emme, please specify either model_net or links_df and nodes_df"
        WranglerLogger.error(msg)
        raise ValueError(msg)

    if include_transit:
        if not transit_network:
            msg = "Missing transit network to write to emme, please specify transit_network."
            WranglerLogger.error(msg)
            raise ValueError(msg)

    WranglerLogger.info("Creating shapes for backward directions on two-way links")
    # create reverse shapes
    links_df = pd.merge(
        links_df,
        nodes_df[["N", "X", "Y"]].rename(columns = {"N" : "A", "X": "A_X", "Y" : "A_Y"}),
        how = "left",
        on = "A"
    )

    links_df = pd.merge(
        links_df,
        nodes_df[["N", "X", "Y"]].rename(columns = {"N" : "B", "X": "B_X", "Y" : "B_Y"}),
        how = "left",
        on = "B"
    )

    links_df["geometry"] = links_df.apply(
        lambda g: LineString(list(g["geometry"].coords)[::-1]) if list(g["geometry"].coords)[0][0] == g["B_X"] else g["geometry"],
        axis = 1
    )

    WranglerLogger.info("Make sure the CRS of input network is correct")

    # problem: reprojection causes inf in lat/long when converting from WGS 4326
    # links_df.crs = CRS("epsg:4269")  
    # nodes_df.crs = CRS("epsg:4269")
    links_df = links_df.to_crs(parameters.output_proj)
    nodes_df = nodes_df.to_crs(parameters.output_proj)

    nodes_df["X"] = nodes_df["geometry"].apply(lambda g: g.x)
    nodes_df["Y"] = nodes_df["geometry"].apply(lambda g: g.y)

    WranglerLogger.info("Converting geometry into wkt geometry")
    # geometry to wkt geometry
    length_gdf = links_df.copy()
    length_gdf = length_gdf.to_crs(epsg=26915)
    length_gdf['distance'] = length_gdf.geometry.length / 1609.34
    links_df["length"] = length_gdf["distance"]
    links_df["geometry_wkt"] = links_df["geometry"].apply(lambda x: x.wkt)

    # create taz_zone_id field
    nodes_df['taz_zone_id'] = np.where(
        nodes_df['N'].isin(parameters.taz_N_list),
        nodes_df['N'],
        0
    )

    # create maz_zone_id field
    nodes_df['maz_zone_id'] = np.where(
        nodes_df['N'].isin(parameters.maz_N_list),
        nodes_df['N'],
        0
    )

    # create tap_zone_id field
    nodes_df['tap_zone_id'] = np.where(
        nodes_df['N'].isin(parameters.tap_N_list),
        nodes_df['N'],
        0
    )

    # create node_type field
    nodes_df['node_type'] = np.where(
        nodes_df['N'].isin(parameters.taz_N_list),
        'taz',
        np.where(
            nodes_df['N'].isin(parameters.maz_N_list),
            'maz',
            np.where(
                nodes_df['N'].isin(parameters.tap_N_list),
                'tap',
                'network'
            )
        )
    )

    if write_drive_network:
        _NAME = "emme_drive_network"
        include_transit = False
        model_tables = prepare_table_for_drive_network(
            nodes_df=nodes_df,
            links_df=links_df,
            shapes_df=shapes_df,
            transit_network=transit_network,
            parameters=parameters,
            regenerate_connectors=regenerate_connectors,
            taz_zone_shapefile_path=taz_zone_shapefile_path,
            maz_zone_shapefile_path=maz_zone_shapefile_path,
        )

        setup = SetupEmme(model_tables, out_dir, _NAME, include_transit, parameters)
        setup.run()

    if write_maz_active_modes_network:
        include_transit = False
        model_tables = prepare_table_for_maz_active_modes_network(
            nodes_df=nodes_df,
            links_df=links_df,
            parameters=parameters,
            subregion_boundary_file = polygon_file_to_split_active_modes_network,
            subregion_boundary_id_variable = polygon_variable_to_split_active_modes_network
        )

        for key, value in model_tables.items():
            _NAME = "emme_maz_active_modes_network"
            _NAME = _NAME + '_' + key
            _model_tables = value

            setup = SetupEmme(_model_tables, out_dir, _NAME, include_transit, parameters)
            setup.run()
    
    if write_tap_transit_network:
        _NAME = "emme_tap_transit_network"
        include_transit = True
        model_tables = prepare_table_for_tap_transit_network(
            nodes_df=nodes_df,
            links_df=links_df,
            transit_network=transit_network,
            parameters=parameters,
            output_dir=out_dir,
        )

        setup = SetupEmme(model_tables, out_dir, _NAME, include_transit, parameters)
        setup.run()

    if write_taz_transit_network:
        _NAME = "emme_taz_transit_network"
        include_transit = True
        model_tables = prepare_table_for_taz_transit_network(
            nodes_df=nodes_df,
            links_df=links_df,
            transit_network=transit_network,
            parameters=parameters,
            output_dir=out_dir,
        )

        setup = SetupEmme(model_tables, out_dir, _NAME, include_transit, parameters)
        setup.run()

#%%
def extract_bus_shapes(transit_network: StandardTransit, route_type_bus_id:int = 3):
    """Returns a list of Nodes that contain bus routes"""
    
    shapes_df = transit_network.feed["shapes"].copy()
    trips_df = transit_network.feed["trips"].copy()
    routes_df = transit_network.feed["routes"].copy()

    bus_routes_df = routes_df.loc[routes_df["route_type"].isin([route_type_bus_id]), "route_id"]
    bus_trips_df = trips_df.loc[trips_df["route_id"].isin(bus_routes_df), "shape_id"]
    bus_shapes_df = shapes_df[shapes_df["shape_id"].isin(bus_trips_df)]
    return bus_shapes_df

#%%
def tag_if_link_contains_bus(links_df: gpd.GeoDataFrame, gtfs_shape_bus_routes: gpd.GeoDataFrame):
    """tags nodes where the starting node and the end node are bus route nodes"""
    links_df = links_df.copy()
    gtfs_shape_bus_routes = gtfs_shape_bus_routes.copy()

    # links are defined as pairs of nodes, so get next node (this assumes order)
    gtfs_shape_bus_routes["shape_id"] = pd.to_numeric(gtfs_shape_bus_routes["shape_id"])
    gtfs_shape_bus_routes["shape_pt_sequence"] = pd.to_numeric(gtfs_shape_bus_routes["shape_pt_sequence"])
    gtfs_shape_bus_routes = gtfs_shape_bus_routes.sort_values(by=["shape_id", "shape_pt_sequence"])

    gtfs_shape_bus_routes["prev_node_id"] = gtfs_shape_bus_routes["shape_model_node_id"].shift(1)
    
    # since we are keeping previous node, the first pair in each sequence is nonsence
    gtfs_shape_bus_routes = gtfs_shape_bus_routes[(gtfs_shape_bus_routes["shape_pt_sequence"] != 1)]
    gtfs_shape_bus_routes = gtfs_shape_bus_routes.drop_duplicates(subset=["shape_model_node_id", "prev_node_id"])
    gtfs_shape_bus_routes["has_bus_on_link"] = True
    gtfs_shape_bus_routes["shape_model_node_id"] = pd.to_numeric(gtfs_shape_bus_routes["shape_model_node_id"])
    gtfs_shape_bus_routes["prev_node_id"] = pd.to_numeric(gtfs_shape_bus_routes["prev_node_id"])

    # We direction is important, if its not, there would need ot be a second merge
    links_df = pd.merge(links_df, gtfs_shape_bus_routes[["shape_model_node_id", "prev_node_id", "has_bus_on_link"]], 
        left_on=["A", "B"],
        right_on=[ "prev_node_id", "shape_model_node_id"],
        how="left"
    ).drop(columns=["shape_model_node_id", "prev_node_id"])
    links_df["has_bus_on_link"] = links_df["has_bus_on_link"].fillna(False)

    return links_df

    #%%
def find_parent_ranch_dir(ranch_file_path: Union[Path, str]):
    """
    In Case __init__ file moves ect, we will take any file in the ranch directory and return the parent ranch dir
    """
    path = Path(ranch_file_path)
    while True:
        prev_path=path
        path=path.parent
        if path.name != "ranch" and prev_path.name == "ranch":
            return prev_path
        
        if len(path.parts) == 1:
            msg = f"Expected Ranch Directory above '{ranch_file_path}' but could not find directory"
            raise RuntimeError(msg)

def prepare_table_for_drive_network(
    nodes_df: pd.DataFrame,
    links_df: pd.DataFrame,
    transit_network: StandardTransit,
    shapes_df: Optional[GeoDataFrame],
    parameters: Parameters,
    maximum_ft: int=7,
    regenerate_connectors: bool=False,
    taz_zone_shapefile_path: Optional[str] = None,
    maz_zone_shapefile_path: Optional[str] = None,
):

    """
    prepare model table for drive network (including taz and maz nodes), in which taz/maz nodes are centroids and 
    connected to the remaining network. 
    The conditions for a link to be kept in the network is if it meets any of the below conditions:
     - ft <= 7
     - links containing bus routes acording to transit_network.feed
     - toll booths >= 1
     - toll sag >= 1 
     - link is a managed road - connector

    Arguments:
        nodes_df -- node database
        links_df -- link database
        transit_network -- Standard Transit network containing bus route information
        shapes_df -- shapes database
        parameters -- emmebanke creation paramerters parameters
        maximum_ft -- maximum Facility type to include 
        regenerate_connectors -- Bool: option to regenerate connectors after the relevent links have been dropped 
        taz_zone_shapefile_path -- path to shapefiles containing taz zones for centroid connectors 
        maz_zone_shapefile_path -- path to shapefiles containing maz zones for centroid connectors 
    
    Return:
        dictionary of model network settings
    """


    model_tables = dict()

    # use taz as centroids, drop maz nodes and connectors
    model_tables["centroid_table"] = nodes_df[
        nodes_df.N.isin(parameters.taz_N_list)
    ].to_dict('records')

    # get the links where buses drive on the network
    gtfs_shape_bus_routes = extract_bus_shapes(transit_network)

    links_df = tag_if_link_contains_bus(links_df, gtfs_shape_bus_routes)
    
    # filter drive links by condition mentioned in the doc string
    managed_nodes = list(set(links_df[links_df["managed"] == 1]["A"]) | set(links_df[links_df["managed"] == 1]["B"]))
    links_df["managed_lane_connector"] = (links_df["ft"] == 8) & (links_df["A"].isin(managed_nodes) | links_df["B"].isin(managed_nodes))

    drive_links_df = links_df[
        (
            # just Remove taz connectors, maz connectors / nodes will remain as regular nodes
            ~(links_df.A.isin(parameters.taz_N_list)) & 
            ~(links_df.B.isin(parameters.taz_N_list)) &
            ( # ft <= 7 should be kept in the nework
                (
                    (links_df.drive_access == 1) & 
                    (links_df.ft <= maximum_ft)
                ) |
                ( # is a tollsegment, should be kept within the network
                    (links_df.tollseg != 0) |
                    (links_df.tollbooth != 0)
                ) 
            )
        ) | links_df["has_bus_on_link"] | links_df["managed_lane_connector"] | (links_df["cntype"] == "MAZ") # Maz is filtered out by links_df.fg <= maximum_ft 
    ].copy()

    if not regenerate_connectors:
        model_tables["connector_table"] = links_df[
            (links_df.A.isin(parameters.taz_N_list)) | 
            (links_df.B.isin(parameters.taz_N_list))
        ].to_dict('records')
    else:
        # check ranch is installed
        raise NotImplementedError("Currently Waiting On task Order 6 to finish implementation")
        if ranch is None:
            raise ImportError("package 'ranch' is not installed, please go to https://github.com/BayAreaMetro/Ranch for install instructions")
        if shapes_df is None:
            raise ValueError("shapes_df must be provided to regenerate connectors")
        if taz_zone_shapefile_path is None:
            raise ValueError("taz_zone_shapefile_path must be passed in when regenerate_connectors=True")
        if maz_zone_shapefile_path is None:
            raise ValueError("maz_zone_shapefile_path must be passed in when regenerate_connectors=True")

        # regenerate connectors
        ranch_dir = find_parent_ranch_dir(ranch.__file__)
        ranch_params=ranch.Parameters(
            ranch_base_dir=ranch_dir,
        )
        
        drive_links_df = drive_links_df[drive_links_df["cntype"] != "MAZ"]
        ranch_roadway = ranch.Roadway(nodes_df, drive_links_df.drop(columns=["index_left", "index_right"], errors="ignore"), shapes_df, ranch_params)
        buid_taz_node_id
        ranch_roadway.build_centroid_connectors(build_taz_active_modes=True, build_maz_drive=True, 
            input_taz_polygon_file=taz_zone_shapefile_path, 
            input_maz_polygon_file=maz_zone_shapefile_path, 
        )


        model_tables["connector_table"] = ranch_roadway.links_df[
            (ranch_roadway.links_df.A.isin(parameters.taz_N_list)) | 
            (ranch_roadway.links_df.B.isin(parameters.taz_N_list))
        ].to_dict('records')

        # add the new maz links back in 
        drive_links_df = pd.concat(
            [
                drive_links_df, 
                ranch_roadway.links_df[
                    (ranch_roadway.links_df.A.isin(parameters.maz_N_list)) | 
                    (ranch_roadway.links_df.B.isin(parameters.maz_N_list))
                ]
            ]
        )



    model_tables["link_table"] = drive_links_df.to_dict('records')

    drive_nodes_df = nodes_df[
        (nodes_df.N.isin(drive_links_df.A.tolist()) + nodes_df.N.isin(drive_links_df.B.tolist()))
    ].copy()

    model_tables["node_table"] = drive_nodes_df.to_dict('records')

    return model_tables


def prepare_table_for_maz_active_modes_network(
    nodes_df,
    links_df,
    parameters,
    subregion_boundary_file: Optional[str] = None,
    subregion_boundary_id_variable: Optional[str] = None
):

    """
    prepare model table for maz-scale active modes network, in which there are no centroids
    keep links that are walk_access == 1 and bike_access == 1
    tap links are walk_access == 1
    need to keep taz links as well

    Arguments:
        nodes_df -- node database
        links_df -- link database
    
    Return:
        dictionary of model network settings
    """

    # active mode links
    active_mode_links_df = links_df[
        (links_df.walk_access == 1) | 
        (links_df.bike_access == 1) | 
        ((links_df.A.isin(parameters.taz_N_list)) | (links_df.B.isin(parameters.taz_N_list)))
    ].copy()

    # TODO: remove TAP links in the TAZ approach

    # add the reverse direction links for one-way streets
    active_mode_links_df = add_reverse_direction_for_one_way_streets(active_mode_links_df)

    # divide active mode links and nodes into subnetworks
    # due to the emme limitations on network #link and #node

    if subregion_boundary_file:
        subregion_boundary_for_active_modes_file = subregion_boundary_file
        subregion_boundary_id_variable = subregion_boundary_id_variable
    else:
        subregion_boundary_for_active_modes_file = parameters.subregion_boundary_file
        subregion_boundary_id_variable = parameters.subregion_boundary_id_variable

    WranglerLogger.info("Spliting activate mode network into subnetworks using {}".format(subregion_boundary_for_active_modes_file))
    subregion_boundary_gdf = gpd.read_file(subregion_boundary_for_active_modes_file)

    subregion_boundary_gdf = subregion_boundary_gdf.to_crs(active_mode_links_df.crs)

    # sjoin activate links with subregion polygon
    activate_mode_links_df = gpd.sjoin(
        active_mode_links_df,
        subregion_boundary_gdf,
        how = 'left',
        op = 'intersects'
    )

    # fill na for links outside of the polygon
    activate_mode_links_df[subregion_boundary_id_variable].fillna(0, inplace = True)

    model_tables_dict = dict()

    for i in activate_mode_links_df[subregion_boundary_id_variable].unique():
        
        # no centroids

        model_tables = dict()
    
        model_tables["centroid_table"] = []

        model_tables["connector_table"] = []

        subregion_links_df = activate_mode_links_df[
            activate_mode_links_df[subregion_boundary_id_variable] == i
        ].copy()

        model_tables["link_table"] = subregion_links_df.to_dict('records')

        subregion_nodes_df = nodes_df[
            nodes_df.N.isin(subregion_links_df.A.tolist() + subregion_links_df.B.tolist())
        ].copy()

        model_tables["node_table"] = subregion_nodes_df.to_dict('records')

        if type(i) == 'str':
            model_tables_dict['subregion_'+i] = model_tables
        else:
            model_tables_dict['subregion_'+str(i)] = model_tables

    return model_tables_dict

def add_reverse_direction_for_one_way_streets(
    active_modes_network_links_df
):
    """
    for active modes network, need to make sure one-way streets have connections 
    in both directions JUST FOR WALK, if missing one direction, duplicate the link and reverse it

    Arguments:
        active_modes_network_links_df -- active modes links in the base network

    Return:
        updated_active_modes_network_links_df
    """
    # create 'A-B' string field by 'min(A,B)-max(A,B)'
    updated_active_modes_network_links_df = active_modes_network_links_df.copy()

    updated_active_modes_network_links_df['A-B'] = updated_active_modes_network_links_df.apply(
        lambda x: '{}-{}'.format(min(x['A'], x['B']), max(x['A'], x['B'])),
        axis = 1
    )

    # group the input links by 'A-B'
    # count the number of records
    count_connections_df =  updated_active_modes_network_links_df.groupby(
        ['A-B']
    )['A'].count().reset_index()

    # get 'A-B' rows that has only 1 record
    # duplicate such rows, switch the A and B value
    # append new rows to input
    one_way_connections_AB_list = count_connections_df[
        count_connections_df['A'] == 1
    ]['A-B'].tolist()

    one_way_active_modes_network_links_df = updated_active_modes_network_links_df[
        updated_active_modes_network_links_df['A-B'].isin(
            one_way_connections_AB_list
        )
    ].copy()

    one_way_active_modes_network_links_df.rename(
        columns = {
            'A' : 'B',
            'B' : 'A'
        },
        inplace = True
    )
    
    # reverse the geometry
    one_way_active_modes_network_links_df["geometry"] = one_way_active_modes_network_links_df["geometry"].apply(
        lambda g: LineString(list(g.coords)[::-1])
    )
    
    updated_active_modes_network_links_df = updated_active_modes_network_links_df.append(
        one_way_active_modes_network_links_df,
        sort = False,
        ignore_index = True
    )

    return updated_active_modes_network_links_df

def prepare_table_for_tap_transit_network(
    nodes_df,
    links_df,
    transit_network,
    parameters,
    output_dir: str,
):

    """
    prepare model table for tap-scale transit network, in which taps are centroids, drop taz and maz
    keep links that are drive_access, bus_only, rail_only

    Arguments:
        nodes_df -- node database
        links_df -- link database
        transit_network -- transit network object
    
    Return:
        dictionary of model network settings
    """

    model_tables = dict()

    # taps are centroids, drop taz and maz

    model_tables["centroid_table"] = nodes_df[
        nodes_df.N.isin(parameters.tap_N_list)
    ].to_dict('records')

    model_tables["connector_table"] = links_df[
        (links_df.A.isin(parameters.tap_N_list)) | (links_df.B.isin(parameters.tap_N_list))
    ].to_dict('records')

    transit_links_df = links_df[
        ~(links_df.A.isin(parameters.taz_N_list + parameters.tap_N_list + parameters.maz_N_list)) & 
        ~(links_df.B.isin(parameters.taz_N_list + parameters.tap_N_list + parameters.maz_N_list)) & 
        ((links_df.drive_access == 1) | (links_df.bus_only == 1) | (links_df.rail_only == 1))
    ].copy()

    model_tables["link_table"] = transit_links_df.to_dict('records')

    transit_nodes_df = nodes_df[
        ~(nodes_df.N.isin(parameters.tap_N_list + parameters.taz_N_list + parameters.maz_N_list)) &
        (nodes_df.N.isin(transit_links_df.A.tolist() + transit_links_df.B.tolist()))
    ].copy()

    model_tables["node_table"] = transit_nodes_df.to_dict('records')

    # read vehicle type table
    veh_cap_crosswalk = pd.read_csv(parameters.veh_cap_crosswalk_file)

    # gtfs trips
    trips_df = route_properties_gtfs_to_emme(
        transit_network=transit_network,
        parameters=parameters,
        output_dir = output_dir
        )

    itinerary_df=pd.DataFrame()
    WranglerLogger.info("Creating itinerary table for each transit trip")
    for index, row in trips_df.iterrows():
        #WranglerLogger.info("Creating itinerary table for trip {}".format(row.line_id))
        trip_itinerary_df = shape_gtfs_to_emme(
            transit_network=transit_network,
            trip_row=row
        )
        itinerary_df = itinerary_df.append(trip_itinerary_df, sort =False, ignore_index=True)
    WranglerLogger.info("Finished creating itinerary table for each transit trip")

    model_tables["line_table"] = trips_df.to_dict('records')

    model_tables['itinerary_table'] = itinerary_df.to_dict('records')

    model_tables["vehicle_table"] = veh_cap_crosswalk.to_dict('records')
    model_tables["vehicle_table"] = [
        {
            "id": 1,
            "mode": "b",
            "total_capacity": 70,
            "seated_capacity": 35,
            "auto_equivalent": 2.5
        },
    ]

    return model_tables

def prepare_table_for_taz_transit_network(
    nodes_df,
    links_df,
    transit_network,
    parameters,
    output_dir: str,
):

    """
    prepare model table for taz-scale transit network, in which tazs are centroids, drop tap and maz
    keep links that are drive_access, bus_only, rail_only

    Arguments:
        nodes_df -- node database
        links_df -- link database
        transit_network -- transit network object
    
    Return:
        dictionary of model network settings
    """

    model_tables = dict()

    if "model_node_id" not in nodes_df.columns:
        nodes_df["model_node_id"] = nodes_df["N"]

    # taps are centroids, drop taz and maz

    model_tables["centroid_table"] = nodes_df[
        nodes_df.N.isin(parameters.taz_N_list)
    ].to_dict('records')

    connectors_df = links_df[
        links_df['roadway'].isin(['taz'])
    ].copy()
    connectors_df['drive_access'] = 1
    
    # need to grab walk access links from street walk node to rail stop nodes
    # those links have walk_access == 1
    # maybe also make them drive_access == 1?

    # rail/ferry routes, non-bus routes
    rail_routes_df = transit_network.feed.routes[
        transit_network.feed.routes.route_type != 3
    ].copy()

    rail_trips_df = transit_network.feed.trips[
        transit_network.feed.trips.route_id.isin(rail_routes_df.route_id.tolist())
    ].copy()

    rail_stops_df = transit_network.feed.stop_times[
        transit_network.feed.stop_times.trip_id.isin(rail_trips_df.trip_id.tolist())
    ].copy()

    rail_nodes_id_list = transit_network.feed.stops[
        transit_network.feed.stops.stop_id.isin(rail_stops_df.stop_id.tolist())
    ]['model_node_id'].astype(float).astype(int).tolist()   # in case model_node_id is string representation of a float

    ###############
    # temporary fix
    
    # walk links with just one of a/b nodes as rail_stop_nodes, make drive_access == 1
    links_df.loc[
        (
            (links_df['A'].isin(rail_nodes_id_list)) | 
            (links_df['B'].isin(rail_nodes_id_list))
        ) &
        (links_df['rail_only'] == 0),
        "drive_access"
    ] = 1
    rail_nodes_df = nodes_df[nodes_df.N.isin(rail_nodes_id_list)].copy()


    ### label walk and bike links within 1 mile buffer area of rail stations
    rail_nodes_union = rail_nodes_df.geometry.unary_union
    rail_nodes_buffer = rail_nodes_union.buffer(parameters.transfer_buffer*5280) # mile to feet
    rail_nodes_buffer = gpd.GeoDataFrame(geometry= [rail_nodes_buffer], crs='EPSG:2875') 
    rail_nodes_buffer['added_walk_link'] = 1

    links_df = gpd.sjoin(links_df, rail_nodes_buffer, how='left').drop(columns='index_right')
    links_df.loc[(~((links_df.drive_access.isin([1,2,3])) | 
                (links_df.bus_only == 1) | 
                (links_df.rail_only == 1)) & 
                (links_df.added_walk_link == 1)), "walk_access"] = 2 # walk_access=2 walk transfer only links

    ### PNR edits -- create PNR dummy links, walk connectors, transfer links

    ### 1. create PNR dummy links
    # read pnr parking location
    pnr_nodes_df = pd.read_csv(parameters.pnr_node_location)[['Zone','Station_Type','X','Y','Vehicle_Cap','Headway','Station_Name','Distance','Fare_System']]
    pnr_nodes_df = gpd.GeoDataFrame(
        pnr_nodes_df, 
        geometry=gpd.points_from_xy(pnr_nodes_df['X'], pnr_nodes_df['Y']),
        crs=parameters.output_proj)
    pnr_nodes_df = pnr_nodes_df.to_crs(parameters.output_proj)
    pnr_nodes_df["X"] = pnr_nodes_df["geometry"].apply(lambda g: g.x)
    pnr_nodes_df["Y"] = pnr_nodes_df["geometry"].apply(lambda g: g.y)

    # reformat pnr_nodes_df, add missing columns
    for c in nodes_df.columns:
        if c not in pnr_nodes_df.columns:
            if c not in ['county']:
                pnr_nodes_df[c] = 0
            else:
                pnr_nodes_df[c] = ''
                
    # assign a node id "N" to pnr parking node
    # pnr node: drive access = 2
    pnr_nodes_df['N'] = pnr_nodes_df['Zone']+nodes_df.N.max()
    pnr_nodes_df['drive_access'] = 2

    # add pnr parking nodes to node_df
    nodes_df = pd.concat([nodes_df, pnr_nodes_df.drop(['Zone','Station_Type','Vehicle_Cap','Headway','Station_Name','Distance','Fare_System'], axis=1)], 
        sort = False, 
        ignore_index = True)

    # pnr vehicle type, will be added to the vehicle_table later
    pnr_vehicle_table = []
    for i in pnr_nodes_df.Vehicle_Cap.unique():
        mode_dict = {}
        mode_dict['id'] = 500+i
        mode_dict['mode'] = "p"
        mode_dict['total_capacity'] = i
        mode_dict['seated_capacity'] = i
        mode_dict['auto_equivalent'] = 2.5 
        pnr_vehicle_table.append(mode_dict)
        
    # add egress vehicle
    pnr_egress_vechile = {'id': 555,
                        'mode': 'p',
                        'total_capacity': 10000,
                        'seated_capacity': 10000,
                        'auto_equivalent': 2.5}
    pnr_vehicle_table.append(pnr_egress_vechile)

    # select rail stops by route type, save to separate lists
    pnr_rail_routes_df = transit_network.feed.routes[
        transit_network.feed.routes.route_type.isin([0,1,2,4])
    ].copy() # 0-light rail, 1-heavey rail, 2-commuter rail, 4-ferry

    pnr_rail_trips_df = transit_network.feed.trips.copy()
    pnr_rail_trips_df = pnr_rail_trips_df.merge(pnr_rail_routes_df[['route_id','route_type']], on='route_id', how='inner')

    pnr_rail_stops_df = transit_network.feed.stop_times.copy()
    pnr_rail_stops_df =  pnr_rail_stops_df.merge(pnr_rail_trips_df[['trip_id','route_type']], on='trip_id', how='inner')

    lr_nodes_id_list = transit_network.feed.stops[
        transit_network.feed.stops.stop_id.isin(pnr_rail_stops_df[pnr_rail_stops_df['route_type']==0].stop_id.tolist())
    ]['model_node_id'].astype(float).astype(int).tolist()   # in case model_node_id is string representation of a float

    hr_nodes_id_list = transit_network.feed.stops[
        transit_network.feed.stops.stop_id.isin(pnr_rail_stops_df[pnr_rail_stops_df['route_type']==1].stop_id.tolist())
    ]['model_node_id'].astype(float).astype(int).tolist()   # in case model_node_id is string representation of a float

    cr_nodes_id_list = transit_network.feed.stops[
        transit_network.feed.stops.stop_id.isin(pnr_rail_stops_df[pnr_rail_stops_df['route_type']==2].stop_id.tolist())
    ]['model_node_id'].astype(float).astype(int).tolist()   # in case model_node_id is string representation of a float

    fy_nodes_id_list = transit_network.feed.stops[
        transit_network.feed.stops.stop_id.isin(pnr_rail_stops_df[pnr_rail_stops_df['route_type']==4].stop_id.tolist())
    ]['model_node_id'].astype(float).astype(int).tolist()   # in case model_node_id is string representation of a float
   
    rail_nodes_df = rail_nodes_df.to_crs(CRS('epsg:26915'))
    rail_nodes_df['X'] = rail_nodes_df.geometry.map(lambda g:g.x)
    rail_nodes_df['Y'] = rail_nodes_df.geometry.map(lambda g:g.y)

    lr_rail_nodes_df = rail_nodes_df[rail_nodes_df.N.isin(lr_nodes_id_list)]
    hr_rail_nodes_df = rail_nodes_df[rail_nodes_df.N.isin(hr_nodes_id_list)]
    cr_rail_nodes_df = rail_nodes_df[rail_nodes_df.N.isin(cr_nodes_id_list)]
    fy_rail_nodes_df = rail_nodes_df[rail_nodes_df.N.isin(fy_nodes_id_list)]

    # for each pnr parking node, search for its nearest rail stop, 
    # save the parking node id and rail stop id, create a two-way dummy link between them
    lr_tree = cKDTree(lr_rail_nodes_df[['X', 'Y']].values)
    hr_tree = cKDTree(hr_rail_nodes_df[['X', 'Y']].values)
    cr_tree = cKDTree(cr_rail_nodes_df[['X', 'Y']].values)
    fy_tree = cKDTree(fy_rail_nodes_df[['X', 'Y']].values)

    pnr_nodes_df = pnr_nodes_df.to_crs(CRS('epsg:26915'))
    pnr_nodes_df['X'] = pnr_nodes_df['geometry'].apply(lambda p: p.x)
    pnr_nodes_df['Y'] = pnr_nodes_df['geometry'].apply(lambda p: p.y)

    for index, row in pnr_nodes_df.iterrows():
        point = row[['X', 'Y']].values

        if row['Station_Type'] == 'L':
            dd, ii = lr_tree.query(point, k = 1)
            if (len(lr_rail_nodes_df)>0) & (dd<=1609.34*parameters.walk_buffer):
                pnr_nodes_df.loc[index,'A'] = lr_rail_nodes_df.iloc[ii].N 
                    
        elif row['Station_Type'] == 'H':
            dd, ii = hr_tree.query(point, k = 1)  
            if (len(hr_rail_nodes_df)>0) & (dd<=1609.34*parameters.walk_buffer):
                pnr_nodes_df.loc[index,'A'] = hr_rail_nodes_df.iloc[ii].N  
            
        elif row['Station_Type'] == 'C':
            dd, ii = cr_tree.query(point, k = 1)
            if (len(cr_rail_nodes_df)>0) & (dd<=1609.34*parameters.walk_buffer):
                pnr_nodes_df.loc[index,'A'] = cr_rail_nodes_df.iloc[ii].N 
            
        elif row['Station_Type'] == 'F':
            dd, ii = fy_tree.query(point, k = 1) 
            if (len(fy_rail_nodes_df)>0) & (dd<=1609.34*parameters.walk_buffer):
                pnr_nodes_df.loc[index,'A'] = fy_rail_nodes_df.iloc[ii].N 
            
    if len(pnr_nodes_df)>0 and ('A' in pnr_nodes_df.columns): #'A' is the nearest rail stop
        pnr_nodes_df = pnr_nodes_df[pnr_nodes_df['A'].notna()]
        pnr_dummy_link_gdf = pnr_nodes_df[['A', 'N']].copy()
        pnr_dummy_link_gdf.rename(columns = {'N' : 'B'}, inplace = True)
        
    # add the opposite_direction
        pnr_dummy_link_gdf = add_opposite_direction_to_link(pnr_dummy_link_gdf, nodes_df=nodes_df, links_df=links_df)

        for index, row in pnr_dummy_link_gdf.iterrows():
            if row['A'] in (pnr_nodes_df.N.to_list()):
                pnr_dummy_link_gdf.loc[index,'N_ref'] = row['A']

            if row['B'] in (pnr_nodes_df.N.to_list()):
                pnr_dummy_link_gdf.loc[index,'N_ref'] = row['B']

        pnr_dummy_link_gdf = pnr_dummy_link_gdf.merge(pnr_nodes_df[['N','Distance']], 
                                                        left_on='N_ref', 
                                                        right_on='N', 
                                                        how='left')
        pnr_dummy_link_gdf['distance'] = pnr_dummy_link_gdf['Distance']
        pnr_dummy_link_gdf = pnr_dummy_link_gdf.drop(['N','N_ref','Distance'], axis=1)

        # update pnr dummy link attributes
        pnr_dummy_link_gdf['lanes_EA'] = 1
        pnr_dummy_link_gdf['lanes_AM'] = 1
        pnr_dummy_link_gdf['lanes_MD'] = 1
        pnr_dummy_link_gdf['lanes_PM'] = 1
        pnr_dummy_link_gdf['lanes_EV'] = 1
        pnr_dummy_link_gdf['ft'] = 99
        pnr_dummy_link_gdf["geometry_wkt"] = pnr_dummy_link_gdf["geometry"].apply(lambda x: x.wkt)
        pnr_dummy_link_gdf['drive_access'] = 2

        # add pnr dummy links to link_df
        links_df = pd.concat([links_df, pnr_dummy_link_gdf], 
            sort = False, 
            ignore_index = True)
        links_df.drop_duplicates(subset = ['A', 'B'], inplace = True)
    else:
        pnr_dummy_link_gdf = None

    # ### 2. add knr dummy links
    exp_bus_trips_df = transit_network.feed.trips.copy()
    exp_bus_trips_df = pd.merge(exp_bus_trips_df, transit_network.feed.routes, how="left", on=["route_id","agency_raw_name"]) 
    exp_bus_trips_df = pd.merge(exp_bus_trips_df, transit_network.feed.agency[["agency_name", "agency_raw_name", "agency_id"]], how = "left", on = ["agency_raw_name", "agency_id"])
    exp_bus_trips_df["is_express_bus"] = exp_bus_trips_df.apply(lambda x: _is_express_bus(x), axis = 1)
    exp_bus_trips_df= exp_bus_trips_df[exp_bus_trips_df["is_express_bus"]==1]

    exp_bus_stops_df = transit_network.feed.stop_times.copy()
    exp_bus_stops_df = exp_bus_stops_df[exp_bus_stops_df["trip_id"].isin(exp_bus_trips_df.trip_id.to_list())]

    exp_nodes_id_list = transit_network.feed.stops[
        transit_network.feed.stops.stop_id.isin(exp_bus_stops_df.stop_id.tolist())
    ]['model_node_id'].astype(float).astype(int).tolist()

    sf_county = gpd.read_file(parameters.sf_county)
    sf_county = sf_county.to_crs(parameters.output_proj)

    exp_nodes_df = nodes_df[nodes_df.N.isin(exp_nodes_id_list)].copy()
    exp_nodes_df_inside = exp_nodes_df.sjoin(sf_county, how="inner")
    exp_nodes_df_outside = exp_nodes_df[~exp_nodes_df["model_node_id"].isin(exp_nodes_df_inside.model_node_id.to_list())]

    rail_nodes_df = nodes_df[nodes_df.N.isin(rail_nodes_id_list)].copy()

    # knr access stations
    knr_nodes_df = rail_nodes_df.copy()
    knr_nodes_df = knr_nodes_df.append(exp_nodes_df_outside)
    knr_dummy_nodes_df = knr_nodes_df.copy()
    knr_dummy_nodes_df["X_dummy"] =  knr_dummy_nodes_df["X"] - 20
    knr_dummy_nodes_df["Y_dummy"] =  knr_dummy_nodes_df["Y"] - 20
    knr_dummy_nodes_df = gpd.GeoDataFrame(
        knr_dummy_nodes_df, 
        geometry=gpd.points_from_xy(knr_dummy_nodes_df['X_dummy'], knr_dummy_nodes_df['Y_dummy']),
        crs=parameters.output_proj).reset_index()  
    
    # add knr dummy nodes to node_df
    knr_dummy_nodes_df['N'] = knr_dummy_nodes_df.index + 1 + nodes_df.N.max()
    knr_dummy_nodes_df['X'] = knr_dummy_nodes_df['geometry'].apply(lambda p: p.x)
    knr_dummy_nodes_df['Y'] = knr_dummy_nodes_df['geometry'].apply(lambda p: p.y)
    knr_dummy_nodes_df['drive_access'] = 3

    nodes_df = pd.concat([nodes_df, knr_dummy_nodes_df.drop(['index', 'X_dummy','Y_dummy'], axis=1)], 
        sort = False, 
        ignore_index = True)

    # create connections between knr dummy nodes and stations
    knr_dummy_nodes_df = knr_dummy_nodes_df.rename(columns={"N":"N_dummy"})
    knr_dummy_nodes_df = knr_dummy_nodes_df.merge(knr_nodes_df[['model_node_id','N']], on='model_node_id', how='left')

    knr_dummy_link_df = knr_dummy_nodes_df[['N_dummy', 'N']].copy()
    knr_dummy_link_df.rename(columns = {'N_dummy' : 'A', 'N' : 'B'}, inplace = True)
    knr_dummy_link_df = add_opposite_direction_to_link(knr_dummy_link_df, nodes_df=nodes_df, links_df=links_df)
    knr_dummy_link_df['drive_access'] = 3
    knr_dummy_link_df['lanes_EA'] = 1
    knr_dummy_link_df['lanes_AM'] = 1
    knr_dummy_link_df['lanes_MD'] = 1
    knr_dummy_link_df['lanes_PM'] = 1
    knr_dummy_link_df['lanes_EV'] = 1
    knr_dummy_link_df['ft'] = 99
    knr_dummy_link_df["geometry_wkt"] = knr_dummy_link_df["geometry"].apply(lambda x: x.wkt)
    knr_dummy_link_df['distance'] = 0.01

    # add knr connection to link_df
    links_df = pd.concat([links_df, knr_dummy_link_df], 
        sort = False, 
        ignore_index = True)
    links_df.drop_duplicates(subset = ['A', 'B'], inplace = True)


    # ### 3. add links between stops and the closest drive node
    drive_access_station_df = nodes_df[nodes_df["drive_access"].isin([2,3])].copy() # pnr/knr nodes

    drive_nodes_df = nodes_df[
        (nodes_df.drive_access == 1) & 
        ~(nodes_df.N.isin(parameters.taz_N_list + parameters.tap_N_list + parameters.maz_N_list)) &
        ~(nodes_df.N.isin(rail_nodes_id_list)) &
        ~(nodes_df.N.isin(exp_nodes_id_list))
    ].copy()

    drive_nodes_df = drive_nodes_df.to_crs(CRS('epsg:26915'))
    drive_nodes_df['X'] = drive_nodes_df.geometry.map(lambda g:g.x)
    drive_nodes_df['Y'] = drive_nodes_df.geometry.map(lambda g:g.y)
    inventory_node_ref = drive_nodes_df[['X', 'Y']].values
    tree = cKDTree(inventory_node_ref)

    drive_access_station_df = drive_access_station_df.to_crs(CRS('epsg:26915'))
    drive_access_station_df['X'] = drive_access_station_df['geometry'].apply(lambda p: p.x)
    drive_access_station_df['Y'] = drive_access_station_df['geometry'].apply(lambda p: p.y)

    for i in range(len(drive_access_station_df)):
        point = drive_access_station_df.iloc[i][['X', 'Y']].values
        dd, ii = tree.query(point, k = 1)
        add_snap_gdf = gpd.GeoDataFrame(drive_nodes_df.iloc[ii]).transpose().reset_index(drop = True)
        add_snap_gdf['A'] = drive_access_station_df.iloc[i]['N']
        if i == 0:
            new_link_gdf = add_snap_gdf.copy()
        else:
            new_link_gdf = new_link_gdf.append(add_snap_gdf, ignore_index=True, sort=False)

    if len(drive_access_station_df) > 0:
        new_link_gdf = new_link_gdf[['A', 'N']].copy()
        new_link_gdf.rename(columns = {'N' : 'B'}, inplace = True)
        new_link_gdf = add_opposite_direction_to_link(new_link_gdf, nodes_df=nodes_df, links_df=links_df)

        for c in links_df.columns:
            if c not in new_link_gdf.columns:
                if c not in ['county', 'shstGeometryId', 'cntype']:
                    new_link_gdf[c] = 0
                else:
                    new_link_gdf[c] = ''
        new_link_gdf['drive_access'] = 1
        new_link_gdf['walk_access'] = 1
        new_link_gdf['bike_access'] = 1
        new_link_gdf['ft'] = 99

        length_gdf = new_link_gdf.copy()
        length_gdf = length_gdf.to_crs(epsg=26915)
        length_gdf['distance'] = length_gdf.geometry.length / 1609.34

        new_link_gdf['distance'] = length_gdf['distance']
        new_link_gdf["geometry_wkt"] = new_link_gdf["geometry"].apply(lambda x: x.wkt)
        links_df = pd.concat([links_df, new_link_gdf], sort = False, ignore_index = True)
        links_df.drop_duplicates(subset = ['A', 'B'], inplace = True)


    ### 4. create walk connectors
    # select centroids
    centroids_df = nodes_df[nodes_df.N.isin(parameters.taz_N_list)]
    centroids_df = centroids_df.to_crs(CRS('epsg:26915')) 

    # select all transit stops
    transit_nodes_id_list = transit_network.feed.stops['model_node_id'].astype(float).astype(int).tolist()   # in case model_node_id is string representation of a float
    transit_nodes_df = nodes_df[nodes_df.N.isin(transit_nodes_id_list)].copy()
    transit_nodes_df = transit_nodes_df.to_crs(CRS('epsg:26915'))
    # for each centroid, draw a buffer,
    # connect the centroid to all transit stops that fall in the buffer
    centroid_node_id = []
    walk_node_id = []

    for index, row in centroids_df.iterrows():
        buffer = row.geometry.buffer(parameters.walk_buffer*1609.34)
        walk_in_buffer = transit_nodes_df[transit_nodes_df.geometry.within(buffer)]
        
        for i in range(len(walk_in_buffer)):
            centroid_node_id.append(row.N)
            walk_node_id.append(walk_in_buffer.iloc[i].N)
       
    if len(centroid_node_id)>0 and len(walk_node_id)>0:
        walk_connector_gdf = pd.DataFrame(list(zip(centroid_node_id, walk_node_id)), columns=['A','B'])
        walk_connector_gdf = add_opposite_direction_to_link(walk_connector_gdf, nodes_df=nodes_df, links_df=links_df)
        walk_connector_gdf['walk_access'] = 1
        walk_connector_gdf['drive_access'] = 0
        walk_connector_gdf["geometry_wkt"] = walk_connector_gdf["geometry"].apply(lambda x: x.wkt)
        
        # add walk connectors to connectors_df
        connectors_df = pd.concat([connectors_df, walk_connector_gdf], 
            sort = False, 
            ignore_index = True)
        connectors_df.drop_duplicates(subset = ['A', 'B'], inplace = True)

    # use the shape length as distacne for now, make sure distance is in miles 
    # need to change it to represent the real distance or
    # update the walk time/drive time based on the real distance
        length_gdf = connectors_df.copy()
        length_gdf = length_gdf.to_crs(epsg=26915)
        length_gdf['distance'] = length_gdf.geometry.length / 1609.34
        connectors_df['distance'] = length_gdf['distance']
        connectors_df["geometry_wkt"] = connectors_df["geometry"].apply(lambda x: x.wkt)

    # make sure distance is in miles
    length_gdf = links_df.copy()
    length_gdf = length_gdf.to_crs(epsg=26915)
    length_gdf['distance'] = length_gdf.geometry.length / 1609.34
    links_df['distance'] = length_gdf['distance']
    links_df["geometry_wkt"] = links_df["geometry"].apply(lambda x: x.wkt)

    # /temporary fix
    ###############

    # drive_access = 1 : drive link
    # drive_access = 2 : pnr link
    # drive_access = 3 : knr link
    transit_links_df = links_df[
        ~(links_df.A.isin(parameters.taz_N_list + parameters.tap_N_list + parameters.maz_N_list)) & 
        ~(links_df.B.isin(parameters.taz_N_list + parameters.tap_N_list + parameters.maz_N_list)) & 
        ((links_df.drive_access.isin([1,2,3])) | (links_df.bus_only == 1) | (links_df.rail_only == 1) | (links_df.added_walk_link == 1))
    ].copy()

    model_tables["link_table"] = transit_links_df.to_dict('records')

    transit_nodes_df = nodes_df[
        ~(nodes_df.N.isin(parameters.tap_N_list + parameters.taz_N_list + parameters.maz_N_list)) &
        (nodes_df.N.isin(transit_links_df.A.tolist() + transit_links_df.B.tolist()))
    ].copy()

    model_tables["node_table"] = transit_nodes_df.to_dict('records')

    model_tables["connector_table"] = connectors_df.to_dict('records')

    # gtfs trips
    trips_df = route_properties_gtfs_to_emme(
        transit_network=transit_network,
        parameters=parameters,
        output_dir = output_dir
        )

    ### PNR edits -- add PNR transit routes
    # create line_id
    # add vechile_cap, headway, and tod
    if pnr_dummy_link_gdf is not None:
        pnr_trips_ref = pnr_nodes_df[['Zone','Vehicle_Cap','Headway','N','Station_Name']].copy()
        pnr_trips_ref['vehtype_num'] = pnr_trips_ref['Vehicle_Cap']+500

        pnr_trips_df = pnr_dummy_link_gdf.copy()

        if pnr_dummy_link_gdf is not None:
            pnr_trips_df = pnr_dummy_link_gdf.copy()

            for index, row in pnr_trips_df.iterrows():

                if row['A'] in (pnr_trips_ref.N.to_list()):
                    pnr_trips_df.loc[index,'N_ref'] = row['A']
                    pnr_trips_df.loc[index,'direction'] = 'acc'
                    
                if row['B'] in (pnr_trips_ref.N.to_list()):
                    pnr_trips_df.loc[index,'N_ref'] = row['B']
                    pnr_trips_df.loc[index,'direction'] = 'egr'
                
        pnr_trips_df = pnr_trips_df.merge(pnr_trips_ref[['N','vehtype_num','Headway','Station_Name']], left_on='N_ref', right_on='N', how='left')
        pnr_trips_df["line_id"] = pnr_trips_df.apply(
                                                        lambda x: str('pnr')
                                                        + "_"
                                                        + str(x.Station_Name)
                                                        + "_"
                                                        + str(x.direction),
                                                        axis=1,
                                                        )
        pnr_trips_df['vehtype_num'] = np.where(pnr_trips_df['direction']=='egr', 555, pnr_trips_df['vehtype_num'])  # 555 is the vechile with very large capactity to simulate pnr egress 
        pnr_trips_df['headway_minutes'] = pnr_trips_df['Headway']
        pnr_trips_df['route_long_name'] = pnr_trips_df['Station_Name']
        pnr_trips_df['tod_name'] = 'AM'
        pnr_trips_df = pnr_trips_df[['line_id','headway_minutes','vehtype_num','route_long_name','tod_name','A','B']]
        pnr_trips_df_temp = pnr_trips_df.copy()

        # add routes in all time periods
        for t in ['EA','MD','PM','EV']:
            for index, row in pnr_trips_df_temp.iterrows():
                if t == "PM":
                    row['tod_name'] = t
                else:  # PNR parking capacity should be unlimited for the off peak hours
                    row['tod_name'] = t
                    row['vehtype_num'] = 555
                pnr_trips_df = pnr_trips_df.append([row])

        # update line_id
        pnr_trips_df["line_id"] = pnr_trips_df.apply(
            lambda x: str(x.line_id)
            + "_"
            + str(x.tod_name),
            axis=1,
        )

        pnr_trips_df["TM2_mode"]= 11 # fix it later
        pnr_trips_df["vehicle_type"]= pnr_trips_df["vehtype_num"]  # vehtype_num will be used to get the vechile capcity information, might not need vehicle_type any more
        pnr_trips_df["faresystem"]=99

        for c in trips_df.columns:
            if c not in pnr_trips_df.columns:
                    pnr_trips_df[c] = ''

        # add dummy transit routes to trips_df
        trips_df = pd.concat([trips_df, pnr_trips_df.drop(['A','B'], axis=1)], 
            sort = False, 
            ignore_index = True)
    else:
        pnr_trips_df = None

    itinerary_df=pd.DataFrame()
    WranglerLogger.info("Creating itinerary table for each transit trip")
    for index, row in trips_df.iterrows():
        #WranglerLogger.info("Creating itinerary table for trip {}".format(row.line_id))
        trip_itinerary_df = shape_gtfs_to_emme(
            transit_network=transit_network,
            trip_row=row
        )
        itinerary_df = itinerary_df.append(trip_itinerary_df, sort =False, ignore_index=True)
   
   ### PNR edits -- add PNR itinerary
    if pnr_trips_df is not None:
        # create itinerary for pnr dummy transit routes
        line_id = []
        node_id = []
        stop_order = []

        for index, row in pnr_trips_df.iterrows():
            line_id.append(row['line_id'])
            node_id.append(row['A'])
            stop_order.append(1)
            
            line_id.append(row['line_id'])
            node_id.append(row['B'])
            stop_order.append(2)

        # create itinerary dataframe
        pnr_trip_node_dict = {'line_id': line_id, 'node_id': node_id, 'stop_order': stop_order} 
        pnr_trip_node_df = pd.DataFrame(pnr_trip_node_dict)

        # update attribute values
        pnr_trip_node_df['allow_alightings'] = 1
        pnr_trip_node_df['allow_boardings'] = 1
        pnr_trip_node_df['time_minutes'] = 0

        for c in itinerary_df.columns:
            if c not in pnr_trip_node_df.columns:
                    pnr_trip_node_df[c] = ''
                    
        # add dummy transit itinerary to itinerary_df
        itinerary_df = pd.concat([itinerary_df, pnr_trip_node_df], 
            sort = False, 
            ignore_index = True)


    WranglerLogger.info("Finished creating itinerary table for each transit trip")

    model_tables["line_table"] = trips_df.to_dict('records')

    model_tables['itinerary_table'] = itinerary_df.to_dict('records')

    # read vehicle type table
    veh_cap_crosswalk = pd.read_csv(parameters.veh_cap_crosswalk_file)
    veh_cap_crosswalk = veh_cap_crosswalk[['100%Capacity','seatcap','vehtype_num','veh_mode','auto_equivalent']]
    
    model_tables["vehicle_table"] = veh_cap_crosswalk.rename(columns={'vehtype_num':'id',
                                                                        'veh_mode':'mode',
                                                                        '100%Capacity':"total_capacity",
                                                                        'seatcap':'seated_capacity'}).to_dict('records')

    model_tables["vehicle_table"].extend(pnr_vehicle_table)  # add pnr vehicle type to vehicle_table

    return model_tables

def route_properties_gtfs_to_emme(
    transit_network = None,
    parameters = None,
    output_dir: str = None
):
    """
    Prepare gtfs for cube lin file.

    Does the following operations:
    1. Combines route, frequency, trip, and shape information
    2. Converts time of day to time periods
    3. Calculates cube route name from gtfs route name and properties
    4. Assigns a cube-appropriate mode number
    5. Assigns a cube-appropriate operator number

    Returns:
        trip_df (DataFrame): DataFrame of trips with cube-appropriate values for:
            - NAME
            - ONEWAY
            - OPERATOR
            - MODE
            - HEADWAY
    """
    WranglerLogger.info(
        "Converting GTFS Standard Properties to MTC's Emme Standard"
    )

    if 'faresystem_crosswalk.txt' in _os.listdir(output_dir):
        faresystem_crosswalk_file = _os.path.join(output_dir, "faresystem_crosswalk.txt")
    else:
        faresystem_crosswalk_file = parameters.faresystem_crosswalk_file
    
    WranglerLogger.info(
        "Reading faresystem from {}".format(faresystem_crosswalk_file)
    )

    shape_df = transit_network.feed.shapes.copy()
    trip_df = transit_network.feed.trips.copy()

    WranglerLogger.info(
        "Reading mode crosswalk from {}".format(parameters.mode_crosswalk_file)
    )

    mode_crosswalk = pd.read_csv(parameters.mode_crosswalk_file)
    mode_crosswalk.drop_duplicates(subset = ["agency_raw_name", "route_type", "is_express_bus"], inplace = True)

    faresystem_crosswalk = pd.read_csv(
        faresystem_crosswalk_file,
        dtype = {"route_id" : "object"}
    )

    WranglerLogger.info(
        "Reading vehicle capacity table from {}".format(parameters.veh_cap_crosswalk_file)
    )
    
    veh_cap_crosswalk = pd.read_csv(parameters.veh_cap_crosswalk_file)

    """
    Add information from: routes, frequencies, and routetype to trips_df
    """
    # trip_df = pd.merge(trip_df, transit_network.feed.routes.drop("agency_raw_name", axis = 1), how="left", on="route_id")
    trip_df = pd.merge(trip_df, transit_network.feed.routes, how="left", on=["route_id","agency_raw_name"]) # in case there are duplicate route_ids

    trip_df = pd.merge(trip_df, transit_network.feed.frequencies, how="left", on="trip_id")

    trip_df["tod"] = trip_df.start_time.apply(transit_network.time_to_cube_time_period, as_str = False)
    trip_df["tod_name"] = trip_df.start_time.apply(transit_network.time_to_cube_time_period)

    # add shape_id to name when N most common pattern is used for routes*tod*direction
    trip_df["shp_id"] = trip_df.groupby(["route_id", "tod", "direction_id"]).cumcount()
    trip_df["shp_id"] = trip_df["shp_id"].astype(str)
    trip_df["shp_id"] = "s" + trip_df["shp_id"]

    trip_df["route_short_name"] = trip_df["route_short_name"].str.replace("-", "_").str.replace(" ", ".").str.replace(",", "_").str.slice(stop = 50)

    trip_df["route_long_name"] = trip_df["route_long_name"].str.replace(",", "_").str.slice(stop = 50)

    # make tri-delta-transit name shorter
    trip_df["agency_id"] = np.where(
        trip_df.agency_id == "tri-delta-transit",
        "tri-delta",
        trip_df.agency_id
    )

    trip_df["LONGNAME"] = trip_df["route_long_name"]
    trip_df["headway_minutes"] = (trip_df["headway_secs"] / 60).astype(int)

    trip_df = pd.merge(trip_df, transit_network.feed.agency[["agency_name", "agency_raw_name", "agency_id"]], how = "left", on = ["agency_raw_name", "agency_id"])

    # identify express bus
    trip_df["is_express_bus"] = trip_df.apply(lambda x: _is_express_bus(x), axis = 1)
    trip_df.drop("agency_name", axis = 1 , inplace = True)

    trip_df = pd.merge(
        trip_df,
        mode_crosswalk.drop("agency_id", axis = 1),
        how = "left",
        on = ["agency_raw_name", "route_type", "is_express_bus"]
    )

    trip_df['TM2_mode'].fillna(11, inplace = True)
    trip_df['mode'] = trip_df['TM2_mode'].astype(int)

    trip_df["agency_id"].fillna("", inplace = True)

    trip_df["line_id"] = trip_df.apply(
        lambda x: str(int(x.TM2_operator))
        + "_"
        + str(x.agency_id)
        + "_"
        + str(x.route_id)[:5]
        + "_"
        + x.tod_name
        + "_"
        + "d"
        + str(int(x.direction_id))
        + "_s"
        + x.shape_id,
        axis=1,
    )

    # trip_df["line_id"] = trip_df["line_id"].str.slice(stop = 28)
    # faresystem
    agency_fare_dict = faresystem_crosswalk[
        (faresystem_crosswalk.route_id.isnull()) |
        (faresystem_crosswalk.route_id=="0")
    ].copy()
    agency_fare_dict = dict(zip(agency_fare_dict.agency_raw_name, agency_fare_dict.faresystem))

    trip_df = pd.merge(
        trip_df,
        faresystem_crosswalk,
        how = "left",
        on = ["agency_raw_name", "route_id"]
    )

    trip_df["faresystem"] = np.where(
        trip_df["faresystem"].isnull(),
        trip_df["agency_raw_name"].map(agency_fare_dict),
        trip_df["faresystem"]
    )

    # GTFS fare info is incomplete
    trip_df["faresystem"].fillna(99,inplace = True)
    
    # special vehicle types
    trip_df["VEHTYPE"] = trip_df.apply(lambda x: _special_vehicle_type(x), axis = 1)

    # get vehicle capacity
    trip_df = pd.merge(trip_df, veh_cap_crosswalk[['VEHTYPE','veh_mode']], how = "left", on = "VEHTYPE")
    trip_df['VEHTYPE'] = np.where(
        (trip_df['is_express_bus']==1) & (trip_df['veh_mode']=="b"), trip_df['VEHTYPE']+' Express', trip_df['VEHTYPE']
        )
    trip_df['veh_mode'] = np.where(
        (trip_df['is_express_bus']==1) & (trip_df['veh_mode']=="b"), "x", trip_df['veh_mode']
        )
    trip_df = pd.merge(trip_df, veh_cap_crosswalk[['VEHTYPE','vehtype_num']], how = "left", on = "VEHTYPE")
    trip_df['vehtype_num'] = trip_df['vehtype_num'].fillna(36) # For those don't have vehtype, assign 36-Motor Articulated Bus
    trip_df["vehicle_type"] = trip_df["vehtype_num"]

    trip_df['USERA1'] = trip_df.apply(lambda row: row.agency_id if row.agency_id != "" else row.agency_raw_name, axis = 1)
    trip_df['USERA2'] = trip_df.apply(lambda row: row.TM2_line_haul_name, axis = 1)
    trip_df['OPERATOR'] = trip_df.apply(lambda row: int(row.TM2_operator) if ~math.isnan(row.TM2_operator) else 99, axis = 1)

    return trip_df

def shape_gtfs_to_emme(transit_network, trip_row):
        """
        Creates transit segment for the trips in appropriate
        emme format.

        Args:
            row: DataFrame row with both shape_id and trip_id

        Returns: a dataframe representation of the transit segment
            for a trip in emme format.

        """
        trip_stop_times_df = transit_network.feed.stop_times.copy()
        trip_stop_times_df = trip_stop_times_df[
            (trip_stop_times_df.trip_id == trip_row.trip_id)
        ]

        trip_node_df = transit_network.feed.shapes.copy()
        trip_node_df = trip_node_df[trip_node_df.shape_id == trip_row.shape_id]
        trip_node_df.sort_values(by = ["shape_pt_sequence"], inplace = True)

        trip_stop_times_df = pd.merge(
            trip_stop_times_df, transit_network.feed.stops, how="left", on="stop_id"
        )

        trip_stop_times_df["model_node_id"] = pd.to_numeric(trip_stop_times_df["model_node_id"]).astype(int)
        trip_node_df["shape_model_node_id"] = pd.to_numeric(trip_node_df["shape_model_node_id"]).astype(int)

        stop_node_id_list = trip_stop_times_df["model_node_id"].tolist()
        trip_node_list = trip_node_df["shape_model_node_id"].tolist()

        trip_stop_times_df.sort_values(by = ["stop_sequence"], inplace = True)
        # sometimes GTFS `stop_sequence` does not start with 1, e.g. SFMTA light rails
        trip_stop_times_df["internal_stop_sequence"] = range(1, 1+len(trip_stop_times_df))
        # sometimes GTFS `departure_time` is not recorded for every stop, e.g. VTA light rails
        trip_stop_times_df["departure_time"].fillna(method = "ffill", inplace = True)
        trip_stop_times_df["departure_time"].fillna(0, inplace = True)
        trip_stop_times_df["NNTIME"] = trip_stop_times_df["departure_time"].diff() / 60
        # CUBE NNTIME takes 2 decimals
        trip_stop_times_df["NNTIME"] = trip_stop_times_df["NNTIME"].round(2)
        trip_stop_times_df["NNTIME"].fillna(-1, inplace = True)

        # node list
        stop_seq = 0
        nntimes = []
        allow_alightings=[]
        allow_boardings=[]
        stop_names=[]

        if trip_row.TM2_line_haul_name in ["Light rail", "Heavy rail", "Commuter rail", "Ferry service"]:
            add_nntime = True
        else:
            add_nntime = False

        for nodeIdx in range(len(trip_node_list)):
            
            if trip_node_list[nodeIdx] in stop_node_id_list:
                # in case a route stops at a stop more than once, e.g. circular route
                stop_seq += 1
                
                if (add_nntime) & (stop_seq > 1):
                    if len(trip_stop_times_df[
                        trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]]) > 1:
                    
                        nntime_v = trip_stop_times_df.loc[
                            (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]) &
                            (trip_stop_times_df["internal_stop_sequence"] == stop_seq),
                            "NNTIME"].iloc[0]
                    else:
                        nntime_v = trip_stop_times_df.loc[
                            (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]),"NNTIME"].iloc[0]

                    nntimes.append(nntime_v)
                else:
                    nntimes.append(0)

                pickup_type = trip_stop_times_df.loc[
                    (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]),"pickup_type"].iloc[0]
                if pickup_type in [1, "1"]:
                    allow_alightings.append(0)
                else:
                    allow_alightings.append(1)

                drop_off_type = trip_stop_times_df.loc[
                    (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]),"drop_off_type"].iloc[0]
                if drop_off_type in [1, "1"]:
                    allow_boardings.append(0)
                else:
                    allow_boardings.append(1)

                stop_name = trip_stop_times_df.loc[
                    (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]),"stop_name"].iloc[0]
                stop_names.append(stop_name)
                
            else:
                nntimes.append(0)
                allow_alightings.append(0)
                allow_boardings.append(0)
                stop_names.append("")

        trip_node_df['time_minutes'] = nntimes
        trip_node_df['allow_alightings'] = allow_alightings
        trip_node_df['allow_boardings'] = allow_boardings
        trip_node_df['stop_name'] = stop_names
        trip_node_df['line_id'] = trip_row['line_id']
        trip_node_df['node_id'] = trip_node_df['shape_model_node_id'].astype(float).astype(int) # in case model_node_id is string representation of a float
        trip_node_df['stop_order'] = trip_node_df['shape_pt_sequence']

        return trip_node_df

### PNR edits -- add a function to create opposite direction of a link ###
def add_opposite_direction_to_link(
    link_gdf, 
    nodes_df,
    links_df):

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
        nodes_df[["N", "X", "Y"]].rename(columns = {"N" : "A", "X": "A_X", "Y" : "A_Y"}),
        how = "left",
        on = "A"
    )

    link_gdf = pd.merge(
        link_gdf,
        nodes_df[["N", "X", "Y"]].rename(columns = {"N" : "B", "X": "B_X", "Y" : "B_Y"}),
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
    
    return link_gdf


class SetupEmme(object):
    """Class to run Emme import and data management operations."""

    def __init__(self, model_tables, directory, name, include_transit, parameters):
        """
        Initialize Python class to run setup of Emme project.

        Arguments:
            model_tables -- interface to tables of model data
            time_periods -- list of time period objects with .name and .duration
            directory -- the output directory for the Emme project and database
            include_transit -- if True, process transit data from database
        """
        self._model_tables = model_tables
        self._directory = directory
        self._NAME = name
        self._include_transit = bool(include_transit)
        self._parameters = parameters

        # TODO: do not hard code this section
        attributes = [
            NetworkAttribute("NODE", "#node_id", "N", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("NODE", "x", "X"),
            NetworkAttribute("NODE", "y", "Y"),
            NetworkAttribute("NODE", "@bike_node", "bike_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "@drive_node", "drive_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "@walk_node", "walk_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "#node_county", "county", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("NODE", "#osm_node_id", "osm_node_id", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("NODE", "@rail_node", "rail_only", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "@farezone", "farezone", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "@stop_tap_id", "tap_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "@taz_id", "taz_zone_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "@maz_id", "maz_zone_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "@tap_id", "tap_zone_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "#node_type", "node_type", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("CENTROID", "#zone_id", "N", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("CENTROID", "#node_id", "N", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("CENTROID", "x", "X"),
            NetworkAttribute("CENTROID", "y", "Y"),
            NetworkAttribute("CENTROID", "@bike_node", "bike_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "@drive_node", "drive_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "@walk_node", "walk_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "#node_county", "county", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("CENTROID", "#osm_node_id", "osm_node_id", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("CENTROID", "@rail_node", "rail_only", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "@farezone", "farezone", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "@stop_tap_id", "tap_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "@taz_id", "taz_zone_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "@maz_id", "maz_zone_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "@tap_id", "tap_zone_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "#node_type", "node_type", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("LINK", "#link_id", "model_link_id", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("LINK", src_name="A"),
            NetworkAttribute("LINK", src_name="B"),
            NetworkAttribute("LINK", "#a_node", "A", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("LINK", "#b_node", "B", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("LINK", "@assignable", "assignable", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@bike_link", "bike_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@drive_link", "drive_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@walk_link", "walk_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@bus_only", "bus_only", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "#link_county", "county", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("LINK", "length", "distance"),
            NetworkAttribute("LINK", "@ft", "ft", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@managed", "managed", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@rail_link", "rail_only", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@segment_id", "segment_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "#shstgeometryid", "shstGeometryId", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("LINK", "@tollbooth", "tollbooth", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@tollseg", "tollseg", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@transit", "transit", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "#cntype", "cntype", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("LINK", "@lanes_ea", "lanes_EA", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@lanes_am", "lanes_AM", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@lanes_md", "lanes_MD", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@lanes_pm", "lanes_PM", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@lanes_ev", "lanes_EV", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@useclass_ea", "useclass_EA", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@useclass_am", "useclass_AM", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@useclass_md", "useclass_MD", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@useclass_pm", "useclass_PM", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@useclass_ev", "useclass_EV", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "_vertices", "geometry_wkt", storage_type="STANDARD", dtype="WKT_GEOMETRY"),
            NetworkAttribute("CONNECTOR", "#link_id", "model_link_id", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("CONNECTOR", src_name="A"),
            NetworkAttribute("CONNECTOR", src_name="B"),
            NetworkAttribute("CONNECTOR", "#a_node", "A", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "#b_node", "B", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@assignable", "assignable", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@bike_link", "bike_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@drive_link", "drive_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@walk_link", "walk_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@bus_only", "bus_only", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "#link_county", "county", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("CONNECTOR", "length", "distance"),
            NetworkAttribute("CONNECTOR", "@ft", "ft", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@managed", "managed", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@rail_link", "rail_only", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@segment_id", "segment_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "#shstgeometryid", "shstGeometryId", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("CONNECTOR", "@tollbooth", "tollbooth", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@tollseg", "tollseg", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@transit", "transit", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "#cntype", "cntype", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("CONNECTOR", "@lanes_ea", "lanes_EA", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@lanes_am", "lanes_AM", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@lanes_md", "lanes_MD", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@lanes_pm", "lanes_PM", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@lanes_ev", "lanes_EV", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@useclass_ea", "useclass_EA", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@useclass_am", "useclass_AM", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@useclass_md", "useclass_MD", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@useclass_pm", "useclass_PM", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@useclass_ev", "useclass_EV", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "_vertices", "geometry_wkt", storage_type="STANDARD", dtype="WKT_GEOMETRY"),
            NetworkAttribute("TRANSIT_LINE", "#description", "route_long_name", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("TRANSIT_LINE", "#short_name", "route_short_name", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("TRANSIT_LINE", "headway", "headway_minutes"),
            NetworkAttribute("TRANSIT_LINE", "#time_period", "tod_name", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("TRANSIT_LINE", src_name="line_id"),
            NetworkAttribute("TRANSIT_LINE", "#mode", "TM2_mode","NETWORK_FIELD","INTEGER32"),
            NetworkAttribute("TRANSIT_LINE", "#line_haul_name", "TM2_line_haul_name","NETWORK_FIELD","STRING"),
            NetworkAttribute("TRANSIT_LINE", "#vehtype", "vehtype_num","NETWORK_FIELD","INTEGER32"),
            NetworkAttribute("TRANSIT_LINE", "#faresystem", "faresystem","NETWORK_FIELD","INTEGER32"),
            NetworkAttribute("TRANSIT_SEGMENT", "allow_alightings", "allow_alightings", dtype="BOOLEAN"),
            NetworkAttribute("TRANSIT_SEGMENT", "allow_boardings", "allow_boardings", dtype="BOOLEAN"),
            NetworkAttribute("TRANSIT_SEGMENT", "@nntime", "time_minutes","EXTRA", "REAL"),
            NetworkAttribute("TRANSIT_SEGMENT", "#stop_name", "stop_name", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("TRANSIT_SEGMENT", src_name="line_id"),
            NetworkAttribute("TRANSIT_SEGMENT", src_name="node_id"),
            NetworkAttribute("TRANSIT_SEGMENT", src_name="stop_order"),
        ]

        self._attrs = attributes
        self._networks = []
        self._emmebank = None
        self._app = None

    def run(self):
        """Import data and prepare Emme project."""
        self.setup_project()
        centroid_table = self._model_tables["centroid_table"]
        node_table = self._model_tables["node_table"]
        connector_table = self._model_tables["connector_table"]
        link_table = self._model_tables["link_table"]

        proc = ProcessNetwork(self._attrs)
        proc.process_base_network(
                centroid_table, node_table, connector_table, link_table, drive_speed=40.0)  # add drive speed
        if self._include_transit:
            line_table = self._model_tables["line_table"]
            itinerary_table = self._model_tables["itinerary_table"]
            vehicle_table = self._model_tables["vehicle_table"]
            proc.process_transit_network(
                    line_table, itinerary_table, vehicle_table, walk_speed=3.0)
        self._networks.append(proc.network)
        self.create_emmebank()
        self.save_networks()

        # write out emme node ID - wrangler node ID correspondence
        proc.node_id_correspondence_df.to_csv(
            _os.path.join(
                self._directory, 
                self._NAME, 
                'Database', 
                self._NAME + '_node_id_crosswalk.csv'
            ),
            index = False
        )

        # Add database to Emme desktop project (if not already added)
        expected_path = _norm(self._emmebank.path)
        valid_db = [db for db in self._app.data_explorer().databases() if _norm(db.path) == expected_path]
        if valid_db:
            db = valid_db[0]
        else:
            db = self._app.data_explorer().add_database(expected_path)
        db.open()
        self._app.refresh_data()

        # set project coordinate system
        dir_path = self._directory
        name = self._NAME
        # spatial_reference_file = _norm(_join(dir_path, name, name + ".emp.prj"))
        #spatial_reference_file_temp = _norm(_join(dir_path, name, name + ".emp.prj.temp"))

        #spatial_ref = _osr.SpatialReference()
        #prjfile = self._parameters.prj_file
        #prj_file = open(prjfile, 'r')
        #prj_txt = prj_file.read()
        #spatial_ref.ImportFromESRI([prj_txt])

        #spatial_ref.MorphToESRI()
        # with open( spatial_reference_file , "w") as f:
        #     f.write(self._parameters.wkt_projection)
        # self._app.project.spatial_reference_file = spatial_reference_file
        self._app.project.spatial_reference_file = self._parameters.prj_file
        self._app.project.save()

        return self._emmebank

    def setup_project(self):
        """
        Start and configure Emme project.

        If there is an already existing project at this location it will be used instead.
        """
        dir_path = self._directory
        name = self._NAME
        project_path = _norm(_join(dir_path, name, name + ".emp"))
        if not _os.path.exists(project_path):
            _app.create_project(dir_path, name)
        app = _app.start_dedicated(visible=True, user_initials="inro", project=project_path)
        app.project.name = name
        self._app = app

    def create_emmebank(self):
        """Create Emmebank file to hold network and demand data. Existing data is overwritten."""
        # Calculate required emmebank dimensions

        def calc_extra_attribute_values(network, dims=None):
            dims = dims if dims else network.element_totals
            extras = 0
            elem_name_mapping = [
                ("links", "LINK"),
                ("nodes", "NODE"),
                ("turn_entries", "TURN"),
                ("transit_lines", "TRANSIT_LINE"),
                ("transit_segments", "TRANSIT_SEGMENT")
            ]
            dims = _copy(dims)
            dims["nodes"] = dims["regular_nodes"] + dims["centroids"]
            for elem_name, name in elem_name_mapping:
                xatts = [a for a in network.attributes(name) if a.startswith("@")]
                extras += (dims[elem_name] + 2) * len(xatts) * 2
            return extras

        totals = {}
        for network in self._networks:
            for k, v in network.element_totals.items():
                totals[k] = max(totals.get(k, 1), v)
            if totals.get("extra_attribute_values"):
                totals["extra_attribute_values"] = max(
                    totals.get("extra_attribute_values"), calc_extra_attribute_values(network)
                )
            else:
                totals["extra_attribute_values"] = calc_extra_attribute_values(network)

        num_matrices = 5

        dimensions = {
            "scalar_matrices": 999,
            "destination_matrices": 999,
            "origin_matrices": 999,
            "full_matrices": num_matrices,
            "scenarios": 1,
            "centroids": totals["centroids"],
            "regular_nodes": totals["regular_nodes"],
            "links": totals["links"],
            "turn_entries": totals["turn_entries"],
            "transit_vehicles": 600,  # change it to 600 for PNR buses
            "transit_lines": totals["transit_lines"],
            "transit_segments": totals["transit_segments"],
            "extra_attribute_values": totals["extra_attribute_values"],
            "sola_analyses": 240,
            "functions": 99,
            "operators": 5000,
        }

        # Access Emmebank database, or create if it does not exist
        project_path = self._app.project.path
        project_root = _dir(project_path)
        db_root = _join(project_root, "Database")
        if not _os.path.exists(db_root):
            _os.mkdir(db_root)
        emmebank_path = _norm(_join(project_root, "Database", "emmebank"))
        
        if _os.path.exists(emmebank_path):
            _os.remove(emmebank_path)
        emmebank = _eb.create(emmebank_path, dimensions)
        emmebank.title = self._NAME
        emmebank.coord_unit_length = 0.0001  # Meters to kilometers
        emmebank.unit_of_length = "km"
        emmebank.unit_of_cost = "$"
        emmebank.unit_of_energy = "MJ"
        emmebank.node_number_digits = 6
        emmebank.use_engineering_notation = True
        self._emmebank = emmebank

    def save_networks(self):
        """Save processed networks in Emmebank."""

        scen_id = 1 # needs to be int
        scenario = self._emmebank.scenario(scen_id)
        if scenario:
            self._emmebank.delete_scenario(scen_id)
        scenario = self._emmebank.create_scenario(scen_id)

        for attr in self._attrs:
            if attr.storage_type == "EXTRA":
                if scenario.extra_attribute(attr.name) is None:
                    scenario.create_extra_attribute(attr.network_domain, attr.name)
            elif attr.storage_type == "NETWORK_FIELD":
                if scenario.network_field(attr.network_domain, attr.name) is None:
                    scenario.create_network_field(attr.network_domain, attr.name, attr.dtype)
        scenario.publish_network(self._networks[0])

    def close(self):
        self._app.close()
        self._emmebank.dispose()


class ProcessNetwork(object):
    """Class to process and import network data from specified tables."""

    def __init__(self, attributes):
        """
        Initialize Python class to run setup of Emme project.

        Arguments:
            model_tables -- interface to network data (model_nodes, model_links etc.)
            time -- {"name": <>, "duration": <>} for time period, duration in hours
            attributes -- list of NetworkAttributes mapping input data fields to Emme data
                (extra attributes, network fields) with cast details as required
        """

        self._attrs = attributes
        self._network = _network.Network()
        self._ignore_index_errors = True
        self.node_id_correspondence_df = DataFrame()

    @property
    def network(self):
        """Return the network object."""
        return self._network

    def process_base_network(self, centroid_table, node_table, connector_table, link_table, drive_speed=40): # add drive speed
        """Import network data from model_nodes and model_links tables."""
        zone_id_name = "N"

        network = self._network
        # Initialize network object with attributes
        for attr in self._attrs:
            if attr.name not in network.attributes(attr.network_domain) and (
                attr.storage_type != "STANDARD" or attr.name == "_vertices"
            ):
                network.create_attribute(attr.network_domain, attr.name)

        # Process nodes from model_nodes and centroids from model_centroids tables
        nodes = {}
        centroid_attrs = [attr for attr in self._attrs if attr.domain == "CENTROID"]
        # make centroid node ID start from 1
        id_generator = IDGenerator(1, network)
        for row in centroid_table:
            node = network.create_centroid(next(id_generator))
            for attr in centroid_attrs:
                attr.set(node, row)
            nodes[node["#node_id"]] = node
        # start should be the centroid node id end + 1
        #centroid_id_max = max(list(nodes.keys())) if len(nodes) > 0 else 0
        centroid_id_max = len(nodes)
        WranglerLogger.info('max centroid node id: {}'.format(centroid_id_max))
        id_generator = IDGenerator(centroid_id_max + 1, network)
        node_attrs = [attr for attr in self._attrs if attr.domain == "NODE"]
        for row in node_table:
            node = network.create_regular_node(next(id_generator))
            for attr in node_attrs:
                attr.set(node, row)
            nodes[node["#node_id"]] = node

        # create node ID correspondence
        # emme ID - network wrangler ID
        emme_id = []
        wrangler_id = []
        for node_key, node_value in nodes.items():
            emme_id.append(node_value.number)
            wrangler_id.append(node_value['#node_id'])

        self.node_id_correspondence_df = pd.DataFrame(
            data = {
                'emme_node_id' : emme_id,
                'model_node_id' : wrangler_id
            }
        )

        # Process links from model_links and connectors from model_connectors tables
        # No network restrictions, all same mode
        auto_mode = network.create_mode("AUTO", "c")
        auto_mode.description = "car"

        # set pnr/knr modes
        drive_access_mode = network.create_mode("AUX_TRANSIT", "D")
        drive_access_mode.speed = "ul1*1" 
        drive_access_mode.description = 'drive_acc'

        # set pnr dummy link as "p"
        pnr_dummy = network.create_mode("TRANSIT", "p")
        pnr_dummy.description = 'pnrdummy'

        # set knr dummy link as "k"
        knr_dummy = network.create_mode("AUX_TRANSIT", "k")
        knr_dummy.description = 'knrdummy'

        # NOTE: could determin list of mode IDs from attribute dictionary
        def mode_map(row):
            if (row['drive_access'] == 2):
                mode = {pnr_dummy}
            elif (row['drive_access'] == 3):
                mode = {knr_dummy}
            else:
                mode = {auto_mode, drive_access_mode}
            return mode

        # Index errors if a link row references a node / centroid which does not exist
        links = {}
        index_errors = []
        connector_attrs = [attr for attr in self._attrs if attr.domain == "CONNECTOR"]
        count1 = 0
        for row in connector_table:
            count1 += 1
            try:
                i_node = nodes[int(row["A"])]
                j_node = nodes[int(row["B"])]
            except KeyError:
                index_errors.append("-".join([str(row["A"]), str(row["B"])]))
                continue
            link = network.create_link(i_node, j_node, mode_map(row))
            for attr in connector_attrs:
                attr.set(link, row)
            # Default values for connectors, as these do not exist in the input data
            #link["@capacity"] = 9999.0
            #link["lanes"] = 1.0
            #link["volume_delay_func"] = 1
            #link["@speed"] = 60.0
            #link["length"] = 0.1
            links[link["#link_id"]] = link
        print (count1)

        link_attrs = [attr for attr in self._attrs if attr.domain == "LINK"]
        count = 0
        for row in link_table:
            count += 1
            try:
                i_node = nodes[int(row["A"])]
                j_node = nodes[int(row["B"])]
            except KeyError:
                index_errors.append("-".join([str(row["A"]), str(row["B"])]))
                continue
            link = network.create_link(i_node, j_node, mode_map(row))
            for attr in link_attrs:
                attr.set(link, row)
            links[link["#link_id"]] = link
        print (count)
        # Copy link verticies to correct attribute name, if they are present
        if "_vertices" in network.attributes("LINK"):
            for link in network.links():
                link.vertices = link._vertices
            network.delete_attribute("LINK", "_vertices")

        if index_errors:
            self._link_index_errors = index_errors
            if not self._ignore_index_errors:
                raise Exception(
                    "{0} links start / end at nodes which do not exist; e.g. {1}".format(
                        len(index_errors), index_errors[0]
                    )
                )
        return

    def process_transit_network(self, line_table, itinerary_table, vehicle_table, walk_speed):
        """Import network data from vehicle_table, line_table and itinerary_table tables."""
        network = self._network

        line_attrs = [attr for attr in self._attrs if attr.domain == "TRANSIT_LINE"]
        seg_attrs = [attr for attr in self._attrs if attr.domain == "TRANSIT_SEGMENT"]

        # Add only auxilary transit mode, hard coded as "w"
        walk = network.create_mode("AUX_TRANSIT", "w")
        walk.speed = walk_speed
        for link in network.links():
            if (link['@drive_link'] not in [2,3]) and (link['@rail_link']!=1):  # pnr dummy and rail link can only be used by p and rail modes
                link.modes |= set([walk])
        # set drive link and bus link as "b"
        bus = network.create_mode("TRANSIT", "b")
        for link in network.links():
            if (link['@drive_link'] == 1) | (link['@bus_only'] == 1):
                link.modes |= set([bus])
        # walk transfer links
        for link in network.links():
            if (link['@walk_link'] == 2):
                link.modes -= set([network.mode('c'), network.mode('D'), network.mode('b')])
        for vehicle_data in vehicle_table:
            mode = network.mode(vehicle_data["mode"])
            if mode is None:
                mode = network.create_mode("TRANSIT", vehicle_data["mode"])
            vehicle = network.create_transit_vehicle(vehicle_data["id"], mode.id)
            if vehicle_data.get("total_capacity"):
                vehicle.total_capacity = int(vehicle_data['total_capacity'])
            if vehicle_data.get("seated_capacity"):
                vehicle.seated_capacity = int(vehicle_data["seated_capacity"])
            if vehicle_data.get("auto_equivalent"):
                vehicle.auto_equivalent = float(vehicle_data["auto_equivalent"])

        # Get map of external node ID to node objects
        node_map = dict((n["#node_id"], n) for n in network.nodes())
        network.create_attribute("LINK", "is_connector")
        for link in network.links():
            link.is_connector = link.i_node.is_centroid or link.j_node.is_centroid
        # Group stops together by line along with sort order
        all_stops = _defaultdict(lambda: {})

        for stop in itinerary_table:
            all_stops[stop["line_id"]][stop["stop_order"]] = stop

        for line_data in line_table:

            mode = network.transit_vehicle(line_data["vehicle_type"]).mode
            # Get the sequence of stops for this line and sort by "stop_order"
            stop_data = all_stops[line_data["line_id"]]
            stop_seq_iter = iter(sorted([(k, node_map[v["node_id"]]) for k, v in stop_data.items()]))
            seq_num, i_node = next(stop_seq_iter)
            node_seq = []
            node_data = {len(node_seq): stop_data[seq_num]}
            # check that links exist and network and add the required mode
            for seq_num, j_node in stop_seq_iter:
                link = network.link(i_node, j_node)
                if link is None:
                    raise Exception("No link from {} to {} on transit line {}".format(i_node, j_node, line_data["line_id"]))
                link.modes |= set([mode])
                if link.modes.intersection(set([network.mode('h'), network.mode('l'), network.mode('r'), network.mode('f')])):
                    link.modes -= set([network.mode('c'), network.mode('D')]) 
                node_seq.append(link.i_node)
                node_data[len(node_seq)] = stop_data[seq_num]
                i_node = j_node
            node_seq.append(link.j_node)
            # Create the transit line and copy the line data
            line = network.create_transit_line(line_data["line_id"], line_data["vehicle_type"], node_seq)
            for attr in line_attrs:
                attr.set(line, line_data)
            # Copy the transit segment and stop data to the network
            for i, seg in enumerate(line.segments(include_hidden=True)):
                for attr in seg_attrs:
                    attr.set(seg, node_data[i])
        network.delete_attribute("LINK", "is_connector")


class NetworkAttribute(object):
    """Container for network attribute definitions and mapping from source data to Emme network."""

    def __init__(self, domain, name=None, src_name=None, storage_type="STANDARD", dtype=None, cast=None):
        """Return new Emme network attribute with details as defined.

        Arguments:
            domain -- the network element type of the attribute.
            name -- name to use in the Emme network
            src_name -- name of the attribute in the source network data
            storage_type -- type of the network attribute, either "EXTRA", "NETWORK_FIELD", "STANDARD"
            dtype -- the data type, one of 'BOOLEAN', 'INTEGER32', 'REAL', 'STRING', "WKT_GEOMETRY"
            cast -- custom function to cast from source data to Emme data,
                    if not specified it is derived from storage_type and dtype
        """

        self.name = name
        self.src_name = src_name
        self.domain = domain
        if domain == "CENTROID":
            self.network_domain = "NODE"
        elif domain == "CONNECTOR":
            self.network_domain = "LINK"
        else:
            self.network_domain = domain
        self.storage_type = storage_type
        self.dtype = dtype
        if cast is not None:
            self.cast = cast
        elif dtype == "WKT_GEOMETRY":
            self.cast = wkt_to_vertices
        elif storage_type in ["EXTRA", "STANDARD"] and dtype is None:
            self.cast = float
        elif dtype == "REAL":
            self.cast = float
        elif dtype == "INTEGER32":
            self.cast = int
        elif dtype == "BOOLEAN":
            self.cast = bool
        else:
            self.cast = str

    def set(self, element, row):
        """Set the value for this attribute from the row of data to the Emme network element."""
        if self.name is None or self.src_name is None:
            return
        element[self.name] = self.cast(row[self.src_name])


def wkt_to_vertices(wkt_geometry):
    """Convert Well Known Text format to list of coordinate points."""
    geo_obj = _ogr.CreateGeometryFromWkt(wkt_geometry)
    return geo_obj.GetPoints()[1:-1]


class IDGenerator(object):
    """Generate available Node IDs."""

    def __init__(self, start, network):
        """Return new Emme network attribute with details as defined."""
        self._number = start
        self._network = network

    def next(self):
        """Return the next valid node ID number."""
        while True:
            if self._network.node(self._number) is None:
                break
            self._number += 1
        return self._number

    # For Python 3+ iterators must define __next__(), for python < 3 they must define next()
    def __next__(self):
        """Return the next valid node ID number."""
        return self.next()
