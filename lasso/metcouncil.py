import os
import numpy as np
import pandas as pd
import geopandas as gpd

from pyproj import CRS
from scipy.spatial import cKDTree
from shapely.geometry import LineString, Point

from network_wrangler import RoadwayNetwork
from network_wrangler.utils import create_unique_shape_id
from .parameters import Parameters
from .roadway import ModelRoadwayNetwork
from .logger import WranglerLogger

def calculate_number_of_lanes(
    roadway_net=None,
    parameters=None,
    lanes_lookup_file=None,
    network_variable="lanes",
    overwrite=False,
    centroid_connect_lanes=1,
):

    """
    Computes the number of lanes using a heuristic defined in this method.

    Args:
        roadway_net (RoadwayNetwork): Network Wrangler RoadwayNetwork object
        parameters (Parameters): Lasso parameters object
        lanes_lookup_file (str): File path to lanes lookup file.
        network_variable (str): Name of the lanes variable
        overwrite (boolean): Overwrite existing values
        centroid_connect_lanes (int): Number of lanes on centroid connectors

    Returns:
        RoadwayNetwork
    """

    WranglerLogger.info(
        "Calculating Number of Lanes as network variable: '{}'".format(
            network_variable,
        )
    )

    if type(parameters) is dict:
        parameters = Parameters(**parameters)
    elif isinstance(parameters, Parameters):
        parameters = Parameters(**parameters.__dict__)
    else:
        msg = "Parameters should be a dict or instance of Parameters: found {} which is of type:{}".format(
            parameters, type(parameters)
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    """
    Verify inputs
    """

    if not roadway_net:
        msg = "'roadway_net' is missing from the method call.".format(roadway_net)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    lanes_lookup_file = (
        lanes_lookup_file
        if lanes_lookup_file
        else parameters.lanes_lookup_file
    )
    if not lanes_lookup_file:
        msg = "'lanes_lookup_file' not found in method or lasso parameters.".format(
            lanes_lookup_file
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    centroid_connect_lanes = (
        centroid_connect_lanes if centroid_connect_lanes else parameters.centroid_connect_lanes
    )

    update_lanes = False

    if network_variable in roadway_net.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing number of lanes variable '{}' already in network".format(
                    network_variable
                )
            )
            roadway_net.links_df = roadway_net.links_df.drop([network_variable], axis = 1)
        else:
            WranglerLogger.info(
                "Number of lanes variable '{}' updated for some links. Returning without overwriting for those links. Calculating for other links".format(
                    network_variable
                )
            )
            update_lanes = True

    """
    Start actual process
    """
    WranglerLogger.debug("Calculating Centroid Connectors")
    calculate_centroidconnect(
        roadway_net=roadway_net,
        parameters=parameters,
        number_of_lanes=centroid_connect_lanes,
        )

    WranglerLogger.debug(
        "Computing number lanes using: {}".format(
            lanes_lookup_file,
        )
    )

    lanes_df = pd.read_csv(lanes_lookup_file)

    join_df = pd.merge(
        roadway_net.links_df,
        lanes_df,
        how="left",
        on="model_link_id",
    )

    def _set_lanes(x):
        try:
            if x.centroidconnect == 1:
                return int(centroid_connect_lanes)
            elif x.drive_access == 0:
                return int(0)
            elif max([x.anoka, x.hennepin, x.carver, x.dakota, x.washington])>0:
                return int(max([x.anoka, x.hennepin, x.carver, x.dakota, x.washington]))
            elif max([x.widot, x.mndot])>0:
                return int(max([x.widot, x.mndot]))
            elif x.osm_min>0:
                return int(x.osm_min)
            elif x.naive>0:
                return int(x.naive)
            else:
                return int(0)
        except:
            return int(0)

    if update_lanes:
        var_name = network_variable + "_cal"
        join_df[var_name] = join_df.apply(lambda x: _set_lanes(x), axis=1)
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_df[['model_link_id', var_name]],
            how="left",
            on="model_link_id",
        )
        roadway_net.links_df[network_variable] = np.where(
            roadway_net.links_df[network_variable] > 0,
            roadway_net.links_df[network_variable],
            roadway_net.links_df[var_name],
        )
        roadway_net.links_df = roadway_net.links_df.drop([var_name], axis=1)
    else:
        join_df[network_variable] = join_df.apply(lambda x: _set_lanes(x), axis=1)
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_df[['model_link_id', network_variable]],
            how="left",
            on="model_link_id",
        )

    WranglerLogger.info(
        "Finished calculating number of lanes to: {}".format(network_variable)
    )

    return roadway_net

def calculate_number_of_lanes_from_reviewed_network(
    roadway_net=None,
    parameters=None,
    osm_lanes_file=None,
    metc_lanes_file=None,
    network_variable="lanes",
    overwrite=False,
    centroid_connect_lanes=1,
):

    """
    Computes the number of lanes using a heuristic defined in this method.

    Args:
        roadway_net (RoadwayNetwork): Network Wrangler RoadwayNetwork object
        parameters (Parameters): Lasso parameters object
        lanes_lookup_file (str): File path to lanes lookup file.
        network_variable (str): Name of the lanes variable
        overwrite (boolean): Overwrite existing values
        centroid_connect_lanes (int): Number of lanes on centroid connectors

    Returns:
        RoadwayNetwork
    """

    WranglerLogger.info(
        "Calculating Number of Lanes as network variable: '{}'".format(
            network_variable,
        )
    )

    if type(parameters) is dict:
        parameters = Parameters(**parameters)
    elif isinstance(parameters, Parameters):
        parameters = Parameters(**parameters.__dict__)
    else:
        msg = "Parameters should be a dict or instance of Parameters: found {} which is of type:{}".format(
            parameters, type(parameters)
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    """
    Verify inputs
    """

    if not roadway_net:
        msg = "'roadway_net' is missing from the method call.".format(roadway_net)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    osm_lanes_file = (
        osm_lanes_file
        if osm_lanes_file
        else parameters.osm_lanes_file
    )
    if not osm_lanes_file:
        msg = "'osm_lanes_file' not found in method or lasso parameters.".format(
            osm_lanes_file
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    metc_lanes_file = (
        metc_lanes_file
        if metc_lanes_file
        else parameters.metc_lanes_file
    )

    centroid_connect_lanes = (
        centroid_connect_lanes if centroid_connect_lanes else parameters.centroid_connect_lanes
    )

    update_lanes = False

    if network_variable in roadway_net.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing number of lanes variable '{}' already in network".format(
                    network_variable
                )
            )
            roadway_net.links_df = roadway_net.links_df.drop([network_variable], axis = 1)
        else:
            WranglerLogger.info(
                "Number of lanes variable '{}' updated for some links. Returning without overwriting for those links. Calculating for other links".format(
                    network_variable
                )
            )
            update_lanes = True

    """
    Start actual process
    """
    WranglerLogger.debug("Calculating Centroid Connectors")
    calculate_centroidconnect(
        roadway_net=roadway_net,
        parameters=parameters,
        number_of_lanes=centroid_connect_lanes,
        )

    WranglerLogger.debug(
        "Computing number lanes using: {} and {}".format(
            osm_lanes_file, metc_lanes_file
        )
    )

    osm_lanes_df = pd.read_csv(osm_lanes_file)
    metc_lanes_df = pd.read_csv(metc_lanes_file)

    join_df = pd.merge(
        roadway_net.links_df,
        osm_lanes_df[['shstReferenceId', 'osm_lanes_min', 'osm_lanes_max']],
        how="left",
        on="shstReferenceId",
    )

    join_df = pd.merge(
        join_df,
        metc_lanes_df[['shstReferenceId', 'lanes_metc_min', 'lanes_metc_max']],
        how="left",
        on="shstReferenceId",
    )

    def _set_lanes(x):
        if x.lanes_metc_min > 0:
            if x.lanes_metc_min == x.lanes_metc_max:
                return x.lanes_metc_min
            elif x.roadway in ['motorway', 'trunk', 'primary', 'secondary']:
                return x.lanes_metc_max
            else:
                return x.lanes_metc_min
        elif x.osm_lanes_min > 0:
            if x.osm_lanes_min == x.osm_lanes_max:
                return x.osm_lanes_min
            elif x.roadway in ['motorway', 'trunk', 'primary', 'secondary']:
                return x.osm_lanes_max
            else:
                return x.osm_lanes_min
        else:
            return 1

    if update_lanes:
        var_name = network_variable + "_cal"
        join_df[var_name] = join_df.apply(lambda x: _set_lanes(x), axis=1)
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_df[['shstReferenceId', var_name]],
            how="left",
            on="shstReferenceId",
        )
        roadway_net.links_df[network_variable] = np.where(
            roadway_net.links_df[network_variable] > 0,
            roadway_net.links_df[network_variable],
            roadway_net.links_df[var_name],
        )
        roadway_net.links_df = roadway_net.links_df.drop([var_name], axis=1)
    else:
        join_df[network_variable] = join_df.apply(lambda x: _set_lanes(x), axis=1)
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_df[['shstReferenceId', network_variable]],
            how="left",
            on="shstReferenceId",
        )

    WranglerLogger.info(
        "Finished calculating number of lanes to: {}".format(network_variable)
    )

    return roadway_net

def calculate_assign_group_and_roadway_class(
    roadway_net=None,
    parameters=None,
    assign_group_variable_name="assign_group",
    road_class_variable_name="roadway_class",
    mrcc_roadway_class_shape=None,
    mrcc_shst_data=None,
    mrcc_roadway_class_variable_shp=None,
    mrcc_assgngrp_dict=None,
    widot_roadway_class_shape=None,
    widot_shst_data=None,
    widot_roadway_class_variable_shp=None,
    widot_assgngrp_dict=None,
    osm_assgngrp_dict=None,
    overwrite=False,
):
    """
    Calculates assignment group and roadway class variables.

    Assignment Group is used in MetCouncil's traffic assignment to segment the volume/delay curves.
    Original source is from the MRCC data for the Minnesota: "route system" which is a roadway class
    For Wisconsin, it is from the Wisconsin DOT database, which has a variable called "roadway category"

    There is a crosswalk between the MRCC Route System and Wisconsin DOT --> Met Council Assignment group

    This method joins the network with mrcc and widot roadway data by shst js matcher returns

    Args:
        roadway_net (RoadwayNetwork): A Network Wrangler Roadway Network.
        parameters (Parameters): A Lasso Parameters, which stores input files.
        assign_group_variable_name (str): Name of the variable assignment group should be written to.  Default to "assign_group".
        road_class_variable_name (str): Name of the variable roadway class should be written to. Default to "roadway_class".
        mrcc_roadway_class_shape (str): File path to the MRCC route system geodatabase.
        mrcc_shst_data (str): File path to the MRCC SHST match return.
        mrcc_roadway_class_variable_shp (str): Name of the variable where MRCC route system are stored.
        mrcc_assgngrp_dict (dict): Dictionary to map MRCC route system variable to assignment group.
        widot_roadway_class_shape (str): File path to the WIDOT roadway category geodatabase.
        widot_shst_data (str): File path to the WIDOT SHST match return.
        widot_roadway_class_variable_shp (str): Name of the variable where WIDOT roadway category are stored.
        widot_assgngrp_dict (dict): Dictionary to map WIDOT roadway category variable to assignment group.
        osm_assgngrp_dict (dict): Dictionary to map OSM roadway class to assignment group.

    Returns:
        RoadwayNetwork
    """

    update_assign_group = False
    update_roadway_class = False

    WranglerLogger.info(
        "Calculating Assignment Group and Roadway Class as network variables: '{}' and '{}'".format(
            assign_group_variable_name, road_class_variable_name,
        )
    )

    if assign_group_variable_name in roadway_net.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing MPO Variable '{}' already in network".format(
                    assign_group_variable_name
                )
            )
        else:
            WranglerLogger.info(
                "MPO Variable '{}' updated for some links. Returning without overwriting for those links. Calculating for other links".format(
                    assign_group_variable_name
                )
            )
            update_assign_group = True

    if road_class_variable_name in roadway_net.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing MPO Variable '{}' already in network".format(
                    road_class_variable_name
                )
            )
        else:
            WranglerLogger.info(
                "MPO Variable '{}' updated for some links. Returning without overwriting for those links. Calculating for other links".format(
                    road_class_variable_name
                )
            )
            update_roadway_class = True

    """
    Verify inputs
    """
    mrcc_roadway_class_shape = (
        mrcc_roadway_class_shape
        if mrcc_roadway_class_shape
        else parameters.mrcc_roadway_class_shape
    )
    if not mrcc_roadway_class_shape:
        msg = "'mrcc_roadway_class_shape' not found in method or lasso parameters.".format(
            mrcc_roadway_class_shape
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)
    if not os.path.exists(mrcc_roadway_class_shape):
        msg = "'mrcc_roadway_class_shape' not found at following location: {}.".format(
            mrcc_roadway_class_shape
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    widot_roadway_class_shape = (
        widot_roadway_class_shape
        if widot_roadway_class_shape
        else parameters.widot_roadway_class_shape
    )
    if not widot_roadway_class_shape:
        msg = "'widot_roadway_class_shape' not found in method or lasso parameters.".format(
            widot_roadway_class_shape
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)
    if not os.path.exists(widot_roadway_class_shape):
        msg = "'widot_roadway_class_shape' not found at following location: {}.".format(
            widot_roadway_class_shape
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    mrcc_shst_data = (
        mrcc_shst_data if mrcc_shst_data else parameters.mrcc_shst_data
    )
    if not mrcc_shst_data:
        msg = "'mrcc_shst_data' not found in method or lasso parameters.".format(
            mrcc_shst_data
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)
    if not os.path.exists(mrcc_shst_data):
        msg = "'mrcc_shst_data' not found at following location: {}.".format(
            mrcc_shst_data
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    widot_shst_data = (
        widot_shst_data if widot_shst_data else parameters.widot_shst_data
    )
    if not widot_shst_data:
        msg = "'widot_shst_data' not found in method or lasso parameters.".format(
            widot_shst_data
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)
    if not os.path.exists(widot_shst_data):
        msg = "'widot_shst_data' not found at following location: {}.".format(
            widot_shst_data
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    mrcc_roadway_class_variable_shp = (
        mrcc_roadway_class_variable_shp
        if mrcc_roadway_class_variable_shp
        else parameters.mrcc_roadway_class_variable_shp
    )
    if not mrcc_roadway_class_variable_shp:
        msg = "'mrcc_roadway_class_variable_shp' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    widot_roadway_class_variable_shp = (
        widot_roadway_class_variable_shp
        if widot_roadway_class_variable_shp
        else parameters.widot_roadway_class_variable_shp
    )
    if not widot_roadway_class_variable_shp:
        msg = "'widot_roadway_class_variable_shp' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    mrcc_assgngrp_dict = (
        mrcc_assgngrp_dict
        if mrcc_assgngrp_dict
        else parameters.mrcc_assgngrp_dict
    )
    if not mrcc_assgngrp_dict:
        msg = "'mrcc_assgngrp_dict' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    widot_assgngrp_dict = (
        widot_assgngrp_dict
        if widot_assgngrp_dict
        else parameters.widot_assgngrp_dict
    )
    if not widot_assgngrp_dict:
        msg = "'widot_assgngrp_dict' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    osm_assgngrp_dict = (
        osm_assgngrp_dict
        if osm_assgngrp_dict
        else parameters.osm_assgngrp_dict
    )
    if not osm_assgngrp_dict:
        msg = "'osm_assgngrp_dict' not found in method or lasso parameters.".format(
            osm_assgngrp_dict
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    """
    Start actual process
    """

    WranglerLogger.debug("Calculating Centroid Connectors")
    calculate_centroidconnect(
        roadway_net=roadway_net,
        parameters=parameters)

    WranglerLogger.debug(
        "Reading MRCC Shapefile: {}".format(mrcc_roadway_class_shape)
    )
    mrcc_gdf = gpd.read_file(mrcc_roadway_class_shape)
    WranglerLogger.debug("MRCC GDF Columns\n{}".format(mrcc_gdf.columns))
    #'LINK_ID', 'ROUTE_SYS', 'ST_CONCAT', 'geometry'
    mrcc_gdf["LINK_ID"] = range(1, 1 + len(mrcc_gdf))
    # returns shstreets dataframe with geometry ID, pp_link_id (which is the LINK_ID)

    # shstReferenceId,shstGeometryId,pp_link_id
    mrcc_shst_ref_df = pd.read_csv(mrcc_shst_data)
    WranglerLogger.debug(
        "mrcc shst ref df columns\n{}".format(mrcc_shst_ref_df.columns)
    )

    widot_gdf = gpd.read_file(widot_roadway_class_shape)
    widot_gdf["LINK_ID"] = range(1, 1 + len(widot_gdf))
    WranglerLogger.debug("WiDOT GDF Columns\n{}".format(widot_gdf.columns))
    widot_shst_ref_df = ModelRoadwayNetwork.read_match_result(widot_shst_data)
    WranglerLogger.debug(
        "widot shst ref df columns".format(widot_shst_ref_df.columns)
    )
    # join MRCC geodataframe with MRCC shared street return to get MRCC route_sys and shared street geometry id
    #
    # get route_sys from MRCC
    # end up with OSM data with MRCC attributes
    join_gdf = ModelRoadwayNetwork.get_attribute(
        roadway_net.links_df,
        "shstGeometryId",
        mrcc_shst_ref_df,
        mrcc_gdf,
        mrcc_roadway_class_variable_shp,
    )

    # for exporting mrcc_id
    if "mrcc_id" in roadway_net.links_df.columns:
        join_gdf.drop(["source_link_id"], axis=1, inplace=True)
    else:
        join_gdf.rename(columns={"source_link_id" : "mrcc_id"}, inplace=True)

    join_gdf = ModelRoadwayNetwork.get_attribute(
        join_gdf,
        "shstGeometryId",
        widot_shst_ref_df,
        widot_gdf,
        widot_roadway_class_variable_shp,
    )

    osm_asgngrp_crosswalk_df = pd.read_csv(osm_assgngrp_dict)
    mrcc_asgngrp_crosswalk_df = pd.read_csv(
        mrcc_assgngrp_dict, dtype={mrcc_roadway_class_variable_shp: str}
    )
    widot_asgngrp_crosswak_df = pd.read_csv(widot_assgngrp_dict)

    join_gdf = pd.merge(
        join_gdf,
        osm_asgngrp_crosswalk_df.rename(
            columns={
            "assign_group": "assignment_group_osm",
            "roadway_class": "roadway_class_osm"
            }
        ),
        how="left",
        on="roadway",
    )

    join_gdf = pd.merge(
        join_gdf,
        mrcc_asgngrp_crosswalk_df.rename(
            columns={
            "assign_group": "assignment_group_mrcc",
            "roadway_class": "roadway_class_mrcc"
            }
        ),
        how="left",
        on=mrcc_roadway_class_variable_shp,
    )

    join_gdf = pd.merge(
        join_gdf,
        widot_asgngrp_crosswak_df.rename(
            columns={
            "assign_group": "assignment_group_widot",
            "roadway_class": "roadway_class_widot"
            }
        ),
        how="left",
        on=widot_roadway_class_variable_shp,
    )

    def _set_asgngrp(x):
        try:
            if x.centroidconnect == 1:
                return 9
            elif x.bus_only == 1:
                return 98
            elif x.rail_only == 1:
                return 100
            elif x.drive_access == 0:
                return 101
            elif x.assignment_group_mrcc > 0:
                return int(x.assignment_group_mrcc)
            elif x.assignment_group_widot > 0:
                return int(x.assignment_group_widot)
            else:
                return int(x.assignment_group_osm)
        except:
            return 0

    join_gdf[assign_group_variable_name] = join_gdf.apply(lambda x: _set_asgngrp(x), axis=1)

    def _set_roadway_class(x):
        try:
            if x.centroidconnect == 1:
                return 99
            elif x.bus_only == 1:
                return 50
            elif x.rail_only == 1:
                return 100
            elif x.drive_access == 0:
                return 101
            elif x.roadway_class_mrcc > 0:
                return int(x.roadway_class_mrcc)
            elif x.roadway_class_widot > 0:
                return int(x.roadway_class_widot)
            else:
                return int(x.roadway_class_osm)
        except:
            return 0

    join_gdf[road_class_variable_name] = join_gdf.apply(lambda x: _set_roadway_class(x), axis=1)

    if "mrcc_id" in roadway_net.links_df.columns:
        columns_from_source = ["model_link_id"]
    else:
        columns_from_source=[
        "model_link_id",
        "mrcc_id",
        mrcc_roadway_class_variable_shp,
        widot_roadway_class_variable_shp,
        ]

    if update_assign_group:
        join_gdf.rename(
            columns={
            assign_group_variable_name: assign_group_variable_name + "_cal"
            },
            inplace=True
        )
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_gdf[columns_from_source + [assign_group_variable_name + "_cal"]],
            how="left",
            on="model_link_id",
        )
        roadway_net.links_df[assign_group_variable_name] = np.where(
            roadway_net.links_df[assign_group_variable_name] > 0,
            roadway_net.links_df[assign_group_variable_name],
            roadway_net.links_df[assign_group_variable_name + "_cal"],
        )
        roadway_net.links_df.drop(assign_group_variable_name + "_cal", axis=1, inplace=True)
    else:
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_gdf[columns_from_source + [assign_group_variable_name]],
            how = "left",
            on = "model_link_id",
        )

    if "mrcc_id" in roadway_net.links_df.columns:
        columns_from_source = ["model_link_id"]
    else:
        columns_from_source = [
        "model_link_id",
        "mrcc_id",
        mrcc_roadway_class_variable_shp,
        widot_roadway_class_variable_shp,
        ]


    if update_roadway_class:
        join_gdf.rename(
            columns={
            road_class_variable_name: road_class_variable_name + "_cal"
            },
            inplace=True
        )
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_gdf[columns_from_source + [road_class_variable_name + "_cal"]],
            how="left",
            on="model_link_id",
        )
        roadway_net.links_df[road_class_variable_name] = np.where(
            roadway_net.links_df[road_class_variable_name] > 0,
            roadway_net.links_df[road_class_variable_name],
            roadway_net.links_df[road_class_variable_name + "_cal"],
        )
        roadway_net.links_df.drop(road_class_variable_name + "_cal", axis=1, inplace=True)
    else:
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_gdf[columns_from_source + [road_class_variable_name]],
            how="left",
            on="model_link_id",
        )

    WranglerLogger.info(
        "Finished calculating assignment group variable {} and roadway class variable {}".format(
            assign_group_variable_name, road_class_variable_name,
        )
    )

    return roadway_net

def calculate_assign_group_and_roadway_class_from_reviewed_network(
    roadway_net=None,
    parameters=None,
    assign_group_variable_name="assign_group",
    road_class_variable_name="roadway_class",
    metc_assgngrp_file=None,
    metc_rdclass_file=None,
    osm_assgngrp_dict=None,
    overwrite=False,
):
    """
    Calculates assignment group and roadway class variables.

    Assignment Group is used in MetCouncil's traffic assignment to segment the volume/delay curves.
    Original source is from the MRCC data for the Minnesota: "route system" which is a roadway class
    For Wisconsin, it is from the Wisconsin DOT database, which has a variable called "roadway category"

    There is a crosswalk between the MRCC Route System and Wisconsin DOT --> Met Council Assignment group

    This method joins the network with mrcc and widot roadway data by shst js matcher returns

    Args:
        roadway_net (RoadwayNetwork): A Network Wrangler Roadway Network.
        parameters (Parameters): A Lasso Parameters, which stores input files.
        assign_group_variable_name (str): Name of the variable assignment group should be written to.  Default to "assign_group".
        road_class_variable_name (str): Name of the variable roadway class should be written to. Default to "roadway_class".
        osm_assgngrp_dict (dict): Dictionary to map OSM roadway class to assignment group.

    Returns:
        RoadwayNetwork
    """

    update_assign_group = False
    update_roadway_class = False

    WranglerLogger.info(
        "Calculating Assignment Group and Roadway Class as network variables: '{}' and '{}'".format(
            assign_group_variable_name, road_class_variable_name,
        )
    )

    if assign_group_variable_name in roadway_net.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing MPO Variable '{}' already in network".format(
                    assign_group_variable_name
                )
            )
        else:
            WranglerLogger.info(
                "MPO Variable '{}' updated for some links. Returning without overwriting for those links. Calculating for other links".format(
                    assign_group_variable_name
                )
            )
            update_assign_group = True

    if road_class_variable_name in roadway_net.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing MPO Variable '{}' already in network".format(
                    road_class_variable_name
                )
            )
        else:
            WranglerLogger.info(
                "MPO Variable '{}' updated for some links. Returning without overwriting for those links. Calculating for other links".format(
                    road_class_variable_name
                )
            )
            update_roadway_class = True

    """
    Verify inputs
    """
    metc_assgngrp_file = (
        metc_assgngrp_file
        if metc_assgngrp_file
        else parameters.metc_assgngrp_file
    )
    if not metc_assgngrp_file:
        msg = "'metc_assgngrp_file' not found in method or lasso parameters.".format(
            metc_assgngrp_file
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    metc_rdclass_file = (
        metc_rdclass_file
        if metc_rdclass_file
        else parameters.metc_rdclass_file
    )
    if not metc_rdclass_file:
        msg = "'metc_rdclass_file' not found in method or lasso parameters.".format(
            metc_rdclass_file
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    osm_assgngrp_dict = (
        osm_assgngrp_dict
        if osm_assgngrp_dict
        else parameters.osm_assgngrp_dict
    )
    if not osm_assgngrp_dict:
        msg = "'osm_assgngrp_dict' not found in method or lasso parameters.".format(
            osm_assgngrp_dict
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    """
    Start actual process
    """

    WranglerLogger.debug("Calculating Centroid Connectors")
    calculate_centroidconnect(
        roadway_net=roadway_net,
        parameters=parameters)

    metc_assgngrp_df = pd.read_csv(metc_assgngrp_file)
    metc_rdclass_df = pd.read_csv(metc_rdclass_file)

    osm_asgngrp_crosswalk_df = pd.read_csv(osm_assgngrp_dict)

    join_gdf = pd.merge(
        roadway_net.links_df,
        osm_asgngrp_crosswalk_df.rename(
            columns={
            "assign_group": "assignment_group_osm",
            "roadway_class": "roadway_class_osm"
            }
        ),
        how="left",
        on="roadway",
    )

    join_gdf = pd.merge(
        join_gdf,
        metc_assgngrp_df,
        how="left",
        on='shstReferenceId',
    )

    join_gdf = pd.merge(
        join_gdf,
        metc_rdclass_df,
        how="left",
        on='shstReferenceId',
    )

    def _set_asgngrp(x):
        if x.centroidconnect == 1:
            return 9
        elif x.bus_only == 1:
            return 98
        elif x.rail_only == 1:
            return 100
        elif x.drive_access == 0:
            if x.roadway == 'cycleway':
                return 101
            elif x.roadway == 'footway':
                return 102
            else:
                return 103 # cul-de-secs, edge roads, and others
        elif x.assgngrp_min > 0:
            if x.assgngrp_min == x.assgngrp_max:
                return x.assgngrp_min
            elif x.assignment_group_osm == x.assgngrp_min:
                return x.assgngrp_min
            elif x.assignment_group_osm == x.assgngrp_max:
                return x.assgngrp_max
            elif x.roadway in ['motorway', 'trunk', 'primary', 'secondary']:
                return x.assgngrp_min
            else:
                return x.assgngrp_max
        elif x.assignment_group_osm > 0:
            return x.assignment_group_osm
        else:
            return 101

    join_gdf[assign_group_variable_name] = join_gdf.apply(lambda x: _set_asgngrp(x), axis=1)

    def _set_roadway_class(x):
        if x.centroidconnect == 1:
            return 99
        elif x.bus_only == 1:
            return 50
        elif x.rail_only == 1:
            return 100
        elif x.drive_access == 0:
            return 101
        elif x.rdclass_min > 0:
            if x.rdclass_min == x.rdclass_max:
                return x.rdclass_min
            elif x.roadway_class_osm == x.rdclass_min:
                return x.rdclass_min
            elif x.roadway_class_osm == x.rdclass_max:
                return x.rdclass_max
            elif x.roadway in ['motorway', 'trunk', 'primary', 'secondary']:
                return x.rdclass_min
            else:
                return x.rdclass_max
        elif x.roadway_class_osm > 0:
            return x.roadway_class_osm
        else:
            return 101

    join_gdf[road_class_variable_name] = join_gdf.apply(lambda x: _set_roadway_class(x), axis=1)

    if update_assign_group:
        join_gdf.rename(
            columns={
            assign_group_variable_name: assign_group_variable_name + "_cal"
            },
            inplace=True
        )
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_gdf[['shstReferenceId'] + [assign_group_variable_name + "_cal"]],
            how="left",
            on="shstReferenceId",
        )
        roadway_net.links_df[assign_group_variable_name] = np.where(
            roadway_net.links_df[assign_group_variable_name] > 0,
            roadway_net.links_df[assign_group_variable_name],
            roadway_net.links_df[assign_group_variable_name + "_cal"],
        )
        roadway_net.links_df.drop(assign_group_variable_name + "_cal", axis=1, inplace=True)
    else:
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_gdf[['shstReferenceId'] + [assign_group_variable_name]],
            how = "left",
            on = "shstReferenceId",
        )

    if update_roadway_class:
        join_gdf.rename(
            columns={
            road_class_variable_name: road_class_variable_name + "_cal"
            },
            inplace=True
        )
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_gdf[['shstReferenceId'] + [road_class_variable_name + "_cal"]],
            how="left",
            on="shstReferenceId",
        )
        roadway_net.links_df[road_class_variable_name] = np.where(
            roadway_net.links_df[road_class_variable_name] > 0,
            roadway_net.links_df[road_class_variable_name],
            roadway_net.links_df[road_class_variable_name + "_cal"],
        )
        roadway_net.links_df.drop(road_class_variable_name + "_cal", axis=1, inplace=True)
    else:
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_gdf[['shstReferenceId'] + [road_class_variable_name]],
            how="left",
            on="shstReferenceId",
        )

    WranglerLogger.info(
        "Finished calculating assignment group variable {} and roadway class variable {}".format(
            assign_group_variable_name, road_class_variable_name,
        )
    )

    return roadway_net

def calculate_centroidconnect(
    roadway_net,
    parameters,
    network_variable="centroidconnect",
    highest_taz_number=None,
    as_integer=True,
    overwrite=False,
    lanes_variable="lanes",
    number_of_lanes=1,
):
    """
    Calculates centroid connector variable.

    Args:
        roadway_net (RoadwayNetwork): A Network Wrangler Roadway Network.
        parameters (Parameters): A Lasso Parameters, which stores input files.
        network_variable (str): Variable that should be written to in the network. Default to "centroidconnect"
        highest_taz_number (int): the max TAZ number in the network.
        as_integer (bool): If True, will convert true/false to 1/0s.  Default to True.
        overwrite (Bool): True if overwriting existing county variable in network.  Default to False.
        lanes_variable (str): Variable that stores the number of lanes. Default to "lanes".
        number_of_lanes (int): Number of lanes for centroid connectors. Default to 1.

    Returns:
        RoadwayNetwork
    """

    if network_variable in roadway_net.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing Centroid Connector Variable '{}' already in network".format(
                    network_variable
                )
            )
        else:
            WranglerLogger.info(
                "Centroid Connector Variable '{}' already in network. Returning without overwriting.".format(
                    network_variable
                )
            )
            return

    WranglerLogger.info(
        "Calculating Centroid Connector and adding as roadway network variable: {}".format(
            network_variable
        )
    )
    """
    Verify inputs
    """
    highest_taz_number = (
        highest_taz_number
        if highest_taz_number
        else parameters.highest_taz_number
    )

    if not highest_taz_number:
        msg = "No highest_TAZ number specified in method variable or in parameters"
        WranglerLogger.error(msg)
        raise ValueError(msg)

    WranglerLogger.debug(
        "Calculating Centroid Connectors using highest TAZ number: {}".format(
            highest_taz_number
        )
    )

    if not network_variable:
        msg = "No network variable specified for centroid connector"
        WranglerLogger.error(msg)
        raise ValueError(msg)

    number_of_lanes = (
        number_of_lanes if number_of_lanes else parameters.centroid_connect_lanes
    )

    """
    Start actual process
    """
    roadway_net.links_df[network_variable] = False

    roadway_net.links_df.loc[
        (roadway_net.links_df["A"] <= highest_taz_number)
        | (roadway_net.links_df["B"] <= highest_taz_number),
        network_variable,
    ] = True

    if as_integer:
        roadway_net.links_df[network_variable] = roadway_net.links_df[network_variable].astype(
            int
        )
    WranglerLogger.info(
        "Finished calculating centroid connector variable: {}".format(
            network_variable
        )
    )

    return roadway_net

def add_centroid_and_centroid_connector(
    roadway_network = None,
    parameters = None,
    centroid_file: str = None,
    centroid_connector_link_file: str = None,
    centroid_connector_shape_file: str = None,
):
    """
    Add centorid and centroid connectors from pickles.

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        parameters (Parameters): Lasso parameters object
        centroid_file (str): centroid node gdf pickle filename
        centroid_connector_link_file (str): centroid connector link pickle filename
        centroid_connector_shape_file (str): centroid connector shape pickle filename

    Returns:
        roadway network object

    """

    WranglerLogger.info("Adding centroid and centroid connector to standard network")

    """
    Verify inputs
    """
    if type(parameters) is dict:
        parameters = Parameters(**parameters)
    elif isinstance(parameters, Parameters):
        parameters = Parameters(**parameters.__dict__)
    else:
        msg = "Parameters should be a dict or instance of Parameters: found {} which is of type:{}".format(
            parameters, type(parameters)
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    if not roadway_network:
        msg = "'roadway_network' is missing from the method call.".format(roadway_network)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    centroid_file = (
        centroid_file
        if centroid_file
        else parameters.centroid_file
    )

    centroid_connector_link_file = (
        centroid_connector_link_file
        if centroid_connector_link_file
        else parameters.centroid_connector_link_file
    )

    if not centroid_connector_link_file:
        msg = "'centroid_connector_link_file' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    centroid_connector_shape_file = (
        centroid_connector_shape_file
        if centroid_connector_shape_file
        else parameters.centroid_connector_shape_file
    )

    if not centroid_connector_shape_file:
        msg = "'centroid_connector_shape_file' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    """
    Start actual process
    """

    centroid_gdf = pd.read_pickle(centroid_file)
    centroid_connector_link_gdf = pd.read_pickle(centroid_connector_link_file)
    centroid_connector_shape_gdf = pd.read_pickle(centroid_connector_shape_file)

    centroid_connector_link_gdf["lanes"] = 1
    centroid_connector_link_gdf["assign_group"] = 9
    centroid_connector_link_gdf["roadway_class"] = 99
    centroid_connector_link_gdf["centroidconnect"] = 1
    centroid_connector_link_gdf["managed"] = 0

    if 'county' in centroid_connector_link_gdf.columns:
        centroid_connector_link_gdf['county'] = (
            centroid_connector_link_gdf['county']
            .map(parameters.county_code_dict)
            .fillna(10)
            .astype(int)
        )

    centroid_gdf['drive_access'] = 1
    centroid_gdf['walk_access'] = 1
    centroid_gdf['bike_access'] = 1

    centroid_gdf["X"] = centroid_gdf.geometry.apply(
        lambda g: g.x
    )
    centroid_gdf["Y"] = centroid_gdf.geometry.apply(
        lambda g: g.y
    )

    roadway_network.nodes_df = pd.concat(
        [roadway_network.nodes_df,
        centroid_gdf[
            list(set(roadway_network.nodes_df.columns) &
            set(centroid_gdf.columns))
        ]],
        sort = False,
        ignore_index = True
    )

    centroid_connector_link_gdf = assign_link_id(roadway_network, centroid_connector_link_gdf)

    roadway_network.links_df = pd.concat(
        [roadway_network.links_df,
        centroid_connector_link_gdf[
            list(set(roadway_network.links_df.columns) &
            set(centroid_connector_link_gdf.columns))
        ]],
        sort = False,
        ignore_index = True
    )

    roadway_network.shapes_df = pd.concat(
        [roadway_network.shapes_df,
        centroid_connector_shape_gdf[
            list(set(roadway_network.shapes_df.columns) &
            set(centroid_connector_shape_gdf.columns))
        ]],
        sort = False,
        ignore_index = True
    )

    WranglerLogger.info(
        "Finished adding centroid and centroid connectors"
    )

    return roadway_network

def assign_link_id(
    roadway_network = None,
    add_links_df = None
):
    """
    when adding new links, assign id

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        add_links_df: new links

    Returns:
        add_links_df with unique link ids

    """

    existing_max_id = roadway_network.links_df["model_link_id"].max()

    if "model_link_id" in add_links_df.columns:
        add_links_df.drop(["model_link_id"], axis = 1, inplace = True)

    add_links_df["model_link_id"] = range(1, 1+len(add_links_df))

    add_links_df["model_link_id"] = add_links_df["model_link_id"] + existing_max_id

    return add_links_df

def assign_node_id(
    roadway_network = None,
    add_nodes_df = None
):
    """
    when adding new links, assign id

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        add_nodes_df: new nodes

    Returns:
        add_nodes_df with unique link ids

    """

    existing_max_id = roadway_network.nodes_df["model_node_id"].max()

    if "model_node_id" in add_nodes_df.columns:
        add_nodes_df.drop(["model_node_id"], axis = 1, inplace = True)

    add_nodes_df["model_node_id"] = range(1, 1+len(add_nodes_df))

    add_nodes_df["model_node_id"] = add_nodes_df["model_node_id"] + existing_max_id

    return add_nodes_df

def add_rail_links_and_nodes(
    roadway_network = None,
    parameters = None,
    rail_links_file: str = None,
    rail_nodes_file: str = None,
    add_rail_ae_connections: bool = False,
):
    """
    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        parameters (Parameters): Lasso parameters object
        rail_links_file (str): rail link gdf pickle filename
        rail_nodes_file (str): rail node pickle filename

    Returns:
        roadway network object
    """

    WranglerLogger.info("Adding centroid and centroid connector to standard network")

    """
    Verify inputs
    """
    if type(parameters) is dict:
        parameters = Parameters(**parameters)
    elif isinstance(parameters, Parameters):
        parameters = Parameters(**parameters.__dict__)
    else:
        msg = "Parameters should be a dict or instance of Parameters: found {} which is of type:{}".format(
            parameters, type(parameters)
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    if not roadway_network:
        msg = "'roadway_network' is missing from the method call.".format(roadway_network)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    rail_links_file = (
        rail_links_file
        if rail_links_file
        else parameters.rail_links_file
    )

    rail_nodes_file = (
        rail_nodes_file
        if rail_nodes_file
        else parameters.rail_nodes_file
    )

    if not rail_links_file:
        msg = "'rail_links_file' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    if not rail_nodes_file:
        msg = "'rail_nodes_file' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    """
    Start actual process
    """
    rail_links_gdf = gpd.read_file(rail_links_file)
    rail_nodes_gdf = gpd.read_file(rail_nodes_file)

    rail_links_gdf["lanes"] = 1
    rail_links_gdf["assign_group"] = 100
    rail_links_gdf["roadway_class"] = 100
    rail_links_gdf["centroidconnect"] = 0
    rail_links_gdf["managed"] = 0

    rail_nodes_gdf["X"] = rail_nodes_gdf.geometry.apply(
        lambda g: g.x
    )
    rail_nodes_gdf["Y"] = rail_nodes_gdf.geometry.apply(
        lambda g: g.y
    )

    if 'model_link_id' not in rail_links_gdf.columns:
        rail_links_gdf = assign_link_id(roadway_network, rail_links_gdf)
    if 'model_node_id' not in rail_nodes_gdf.columns:
        rail_nodes_gdf = assign_node_id(roadway_network, rail_nodes_gdf)
    if 'A' not in rail_links_gdf.columns:
        node_id_crosswalk = dict(zip(rail_nodes_gdf['shst_node_id'], rail_nodes_gdf['model_node_id']))
        rail_links_gdf['A'] = rail_links_gdf['fromIntersectionId'].map(node_id_crosswalk)
        rail_links_gdf['B'] = rail_links_gdf['toIntersectionId'].map(node_id_crosswalk)

    # create shape id for rail links
    if 'shstReferenceId' not in rail_links_gdf.columns:
        rail_links_gdf['shstReferenceId'] = rail_links_gdf['fromIntersectionId'] + '_' + rail_links_gdf['toIntersectionId']
        rail_links_gdf['shstGeometryId'] = rail_links_gdf['shstReferenceId']
        rail_links_gdf['id'] = rail_links_gdf['shstReferenceId']
    
    roadway_network.nodes_df = pd.concat(
        [roadway_network.nodes_df,
        rail_nodes_gdf[
            list(set(roadway_network.nodes_df.columns) &
            set(rail_nodes_gdf.columns))
        ]],
        sort = False,
        ignore_index = True
    )

    roadway_network.links_df = pd.concat(
        [roadway_network.links_df,
        rail_links_gdf[
            list(set(roadway_network.links_df.columns) &
            set(rail_links_gdf.columns))
        ]],
        sort = False,
        ignore_index = True
    )

    roadway_network.shapes_df = pd.concat(
        [roadway_network.shapes_df,
        rail_links_gdf[
            list(set(roadway_network.shapes_df.columns) &
            set(rail_links_gdf.columns))
        ]],
        sort = False,
        ignore_index = True
    )

    WranglerLogger.info(
        "Finished adding rail links and nodes connectors"
    )

    if add_rail_ae_connections:
        roadway_network = add_rail_ae_connections(
            roadway_network
        )

    return roadway_network

def add_rail_ae_connections(
    roadway_network, 
    parameters
):
    """
    add walk access and egress connectors to rail stations
    """
    WranglerLogger.info('Creating rail access and egress connection links')
    # add links between rail stops and the closest drive node
    if roadway_network.nodes_df.crs == CRS('epsg:4326'):
        roadway_network.nodes_df.crs = CRS('epsg:4269')

    rail_nodes_df = roadway_network.nodes_df[roadway_network.nodes_df.rail_only == 1].copy()

    drive_nodes_df = roadway_network.nodes_df[
        (roadway_network.nodes_df.drive_access == 1) & 
        (roadway_network.nodes_df.model_node_id > parameters.zones)
    ].copy()

    drive_nodes_df = drive_nodes_df.to_crs(CRS('epsg:26915'))
    drive_nodes_df['X'] = drive_nodes_df.geometry.map(lambda g:g.x)
    drive_nodes_df['Y'] = drive_nodes_df.geometry.map(lambda g:g.y)
    inventory_node_ref = drive_nodes_df[['X', 'Y']].values
    tree = cKDTree(inventory_node_ref)

    rail_nodes_df = rail_nodes_df.to_crs(CRS('epsg:26915'))
    rail_nodes_df['X'] = rail_nodes_df['geometry'].apply(lambda p: p.x)
    rail_nodes_df['Y'] = rail_nodes_df['geometry'].apply(lambda p: p.y)

    for i in range(len(rail_nodes_df)):
        point = rail_nodes_df.iloc[i][['X', 'Y']].values
        dd, ii = tree.query(point, k = 1)
        add_snap_gdf = gpd.GeoDataFrame(drive_nodes_df.iloc[ii]).transpose().reset_index(drop = True)
        add_snap_gdf['A'] = rail_nodes_df.iloc[i]['model_node_id']
        if i == 0:
            new_link_gdf = add_snap_gdf.copy()
        else:
            new_link_gdf = pd.concat([new_link_gdf, add_snap_gdf], ignore_index=True, sort=False)

    if len(rail_nodes_df) > 0:
        new_link_gdf = new_link_gdf[['A', 'model_node_id']].copy()
        new_link_gdf.rename(columns = {'model_node_id' : 'B'}, inplace = True)

        # add the opposite direction
        new_link_gdf = pd.concat(
            [
                new_link_gdf,
                new_link_gdf.rename(columns = {'A' : 'B', 'B' : 'A'})
            ],
            sort = False, 
            ignore_index = True
        )

        # create shapes
        new_link_gdf = pd.merge(
            new_link_gdf,
            roadway_network.nodes_df[["model_node_id", "X", "Y"]].rename(
                columns = {"model_node_id" : "A", "X": "A_X", "Y" : "A_Y"}
            ),
            how = "left",
            on = "A"
        )

        new_link_gdf = pd.merge(
            new_link_gdf,
            roadway_network.nodes_df[["model_node_id", "X", "Y"]].rename(
                columns = {"model_node_id" : "B", "X": "B_X", "Y" : "B_Y"}
            ),
            how = "left",
            on = "B"
        )

        new_link_gdf["geometry"] = new_link_gdf.apply(
            lambda g: LineString([Point(g.A_X, g.A_Y), Point(g.B_X, g.B_Y)]),
            axis = 1
        )

        new_link_gdf = gpd.GeoDataFrame(
            new_link_gdf,
            geometry = new_link_gdf['geometry'],
            crs = roadway_network.links_df.crs
        )

        new_link_gdf[RoadwayNetwork.UNIQUE_SHAPE_KEY] = new_link_gdf.apply(
            lambda x: create_unique_shape_id(x["geometry"]),
            axis = 1
        )

        new_link_gdf['drive_access'] = 1
        new_link_gdf['walk_access'] = 1
        new_link_gdf['bike_access'] = 1
        new_link_gdf['assign_group'] = 50
        new_link_gdf['roadway_class'] = 50

        new_link_gdf.drop_duplicates(subset = ['A', 'B'], inplace = True)
        
        new_link_gdf = assign_link_id(roadway_network, new_link_gdf)
        
        roadway_network.links_df = pd.concat(
            [roadway_network.links_df, 
            new_link_gdf[
                list(set(roadway_network.links_df.columns) &
                set(new_link_gdf.columns))
            ]], 
            sort = False, 
            ignore_index = True
        )

        roadway_network.shapes_df = pd.concat(
            [roadway_network.shapes_df, 
            new_link_gdf[
                list(set(roadway_network.shapes_df.columns) &
                set(new_link_gdf.columns))
            ]], 
            sort = False, 
            ignore_index = True
        )

        return roadway_network