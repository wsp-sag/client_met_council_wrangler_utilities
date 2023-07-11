import os
import numpy as np
import pandas as pd
import geopandas as gpd

from network_wrangler import RoadwayNetwork
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
        lanes_lookup_file if lanes_lookup_file else parameters.lanes_lookup_file
    )
    if not lanes_lookup_file:
        msg = "'lanes_lookup_file' not found in method or lasso parameters.".format(
            lanes_lookup_file
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    centroid_connect_lanes = (
        centroid_connect_lanes
        if centroid_connect_lanes
        else parameters.centroid_connect_lanes
    )

    update_lanes = False

    if network_variable in roadway_net.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing number of lanes variable '{}' already in network".format(
                    network_variable
                )
            )
            readway_net.links_df = roadway_net.links_df.drop([network_variable], axis=1)
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
            elif max([x.anoka, x.hennepin, x.carver, x.dakota, x.washington]) > 0:
                return int(max([x.anoka, x.hennepin, x.carver, x.dakota, x.washington]))
            elif max([x.widot, x.mndot]) > 0:
                return int(max([x.widot, x.mndot]))
            elif x.osm_min > 0:
                return int(x.osm_min)
            elif x.naive > 0:
                return int(x.naive)
        except:
            return int(0)

    if update_lanes:
        var_name = network_variable + "_cal"
        join_df[var_name] = join_df.apply(lambda x: _set_lanes(x), axis=1)
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_df[["model_link_id", var_name]],
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
            join_df[["model_link_id", network_variable]],
            how="left",
            on="model_link_id",
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
            assign_group_variable_name,
            road_class_variable_name,
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

    mrcc_shst_data = mrcc_shst_data if mrcc_shst_data else parameters.mrcc_shst_data
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

    widot_shst_data = widot_shst_data if widot_shst_data else parameters.widot_shst_data
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
        msg = (
            "'mrcc_roadway_class_variable_shp' not found in method or lasso parameters."
        )
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
        mrcc_assgngrp_dict if mrcc_assgngrp_dict else parameters.mrcc_assgngrp_dict
    )
    if not mrcc_assgngrp_dict:
        msg = "'mrcc_assgngrp_dict' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    widot_assgngrp_dict = (
        widot_assgngrp_dict if widot_assgngrp_dict else parameters.widot_assgngrp_dict
    )
    if not widot_assgngrp_dict:
        msg = "'widot_assgngrp_dict' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    osm_assgngrp_dict = (
        osm_assgngrp_dict if osm_assgngrp_dict else parameters.osm_assgngrp_dict
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
    calculate_centroidconnect(roadway_net=roadway_net, parameters=parameters)

    WranglerLogger.debug("Reading MRCC Shapefile: {}".format(mrcc_roadway_class_shape))
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
    WranglerLogger.debug("widot shst ref df columns".format(widot_shst_ref_df.columns))
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
        join_gdf.rename(columns={"source_link_id": "mrcc_id"}, inplace=True)

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
                "roadway_class": "roadway_class_osm",
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
                "roadway_class": "roadway_class_mrcc",
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
                "roadway_class": "roadway_class_widot",
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

    join_gdf[assign_group_variable_name] = join_gdf.apply(
        lambda x: _set_asgngrp(x), axis=1
    )

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

    join_gdf[road_class_variable_name] = join_gdf.apply(
        lambda x: _set_roadway_class(x), axis=1
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

    if update_assign_group:
        join_gdf.rename(
            columns={assign_group_variable_name: assign_group_variable_name + "_cal"},
            inplace=True,
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
        roadway_net.links_df.drop(
            assign_group_variable_name + "_cal", axis=1, inplace=True
        )
    else:
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_gdf[columns_from_source + [assign_group_variable_name]],
            how="left",
            on="model_link_id",
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
            columns={road_class_variable_name: road_class_variable_name + "_cal"},
            inplace=True,
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
        roadway_net.links_df.drop(
            road_class_variable_name + "_cal", axis=1, inplace=True
        )
    else:
        roadway_net.links_df = pd.merge(
            roadway_net.links_df,
            join_gdf[columns_from_source + [road_class_variable_name]],
            how="left",
            on="model_link_id",
        )

    WranglerLogger.info(
        "Finished calculating assignment group variable {} and roadway class variable {}".format(
            assign_group_variable_name,
            road_class_variable_name,
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
        highest_taz_number if highest_taz_number else parameters.highest_taz_number
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
        roadway_net.links_df[network_variable] = roadway_net.links_df[
            network_variable
        ].astype(int)
    WranglerLogger.info(
        "Finished calculating centroid connector variable: {}".format(network_variable)
    )

    return roadway_net
