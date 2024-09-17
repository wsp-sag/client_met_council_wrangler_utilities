import json
import os
import re
from typing import Any, Dict, Optional, Union, List
from csv import reader

from pandas.core import base

import numpy as np
import pandas as pd
from pandas import DataFrame
import geopandas as gpd

from network_wrangler import ProjectCard
from network_wrangler import RoadwayNetwork

from .transit import CubeTransit, StandardTransit
from .logger import WranglerLogger
from .parameters import Parameters
from .roadway import ModelRoadwayNetwork
from .util import column_name_to_parts


class Project(object):
    """A single or set of changes to the roadway or transit system.

    Compares a base and a build transit network or a base and build
    highway network and produces project cards.

    .. highlight:: python

    Typical usage example:
    ::
        test_project = Project.create_project(
            base_cube_transit_source=os.path.join(CUBE_DIR, "transit.LIN"),
            build_cube_transit_source=os.path.join(CUBE_DIR, "transit_route_shape_change"),
        )
        test_project.evaluate_changes()
        test_project.write_project_card(
            os.path.join(SCRATCH_DIR, "t_transit_shape_test.yml")
        )

    Attributes:
        DEFAULT_PROJECT_NAME: a class-level constant that defines what
            the project name will be if none is set.
        STATIC_VALUES: a class-level constant which defines values that
            are not evaluated when assessing changes.
        card_data (dict):  {"project": <project_name>, "changes": <list of change dicts>}
        roadway_link_changes (DataFrame):  pandas dataframe of CUBE roadway link changes.
        roadway_node_changes (DataFrame):  pandas dataframe of CUBE roadway node changes.
        transit_changes (CubeTransit):
        base_roadway_network (RoadwayNetwork):
        base_cube_transit_network (CubeTransit):
        build_cube_transit_network (CubeTransit):
        project_name (str): name of the project, set to DEFAULT_PROJECT_NAME if not provided
        parameters: an  instance of the Parameters class which sets a bunch of parameters
    """

    DEFAULT_PROJECT_NAME = "USER TO define"

    STATIC_VALUES = [
        "model_link_id",
        "area_type",
        "county",
        # "assign_group",
        "centroidconnect",
    ]
    CALCULATED_VALUES = [
        "area_type",
        "county",
        "assign_group",
        "centroidconnect",
    ]

    def __init__(
        self,
        roadway_link_changes: Optional[DataFrame] = None,
        roadway_node_changes: Optional[DataFrame] = None,
        transit_changes: Optional[DataFrame] = None,
        base_roadway_network: Optional[RoadwayNetwork] = None,
        base_transit_network: Optional[StandardTransit] = None,
        base_cube_transit_network: Optional[CubeTransit] = None,
        build_cube_transit_network: Optional[CubeTransit] = None,
        project_name: Optional[str] = "",
        evaluate: Optional[bool] = False,
        parameters: Union[dict, Parameters] = {},
    ):
        """
        ProjectCard constructor.

        args:
            roadway_link_changes: dataframe of roadway changes read from a log file
            roadway_node_changes: dataframe of roadway changes read from a log file
            transit_changes: dataframe of transit changes read from a log file
            base_roadway_network: RoadwayNetwork instance for base case
            base_transit_network: StandardTransit instance for base case
            base_cube_transit_network: CubeTransit instance for base transit network
            build_cube_transit_network: CubeTransit instance for build transit network
            project_name: name of the project
            evaluate: defaults to false, but if true, will create card data
            parameters: dictionary of parameter settings (see Parameters class) or an instance of Parameters. If not specified, will use default parameters.

        returns: instance of ProjectCard
        """
        self.card_data = Dict[str, Dict[str, Any]]

        self.roadway_link_changes = roadway_link_changes
        self.roadway_node_changes = roadway_node_changes
        self.base_roadway_network = base_roadway_network
        self.base_transit_network = base_transit_network
        self.base_cube_transit_network = base_cube_transit_network
        self.build_cube_transit_network = build_cube_transit_network
        self.transit_changes = transit_changes
        self.project_name = (
            project_name if project_name else Project.DEFAULT_PROJECT_NAME
        )

        if type(parameters) is dict:
            self.parameters = Parameters(**parameters)
        elif isinstance(parameters, Parameters):
            self.parameters = Parameters(**parameters.__dict__)
        else:
            msg = "Parameters should be a dict or instance of Parameters: found {} which is of type:{}".format(
                parameters, type(parameters)
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        if base_roadway_network != None:
            self.determine_roadway_network_changes_compatibility(
                self.base_roadway_network, 
                self.roadway_link_changes,
                self.roadway_node_changes, 
                self.parameters
            )

        if evaluate:
            self.evaluate_changes()

    def write_project_card(self, filename: str = None):
        """
        Writes project cards.

        Args:
            filename (str): File path to output .yml

        Returns:
            None
        """
        ProjectCard(self.card_data).write(out_filename=filename)

    @staticmethod
    def create_project(
        roadway_log_file: Union[str, List[str], None] = None,
        roadway_shp_file: Optional[str] = None,
        roadway_csv_file: Optional[str] = None,
        network_build_file: Optional[str] = None,
        emme_node_id_crosswalk_file: Optional[str] = None,
        emme_name_crosswalk_file: Optional[str] = None,
        base_roadway_dir: Optional[str] = None,
        base_transit_dir: Optional[str] = None,
        base_cube_transit_source: Optional[str] = None,
        build_cube_transit_source: Optional[str] = None,
        roadway_link_changes: Optional[DataFrame] = None,
        roadway_node_changes: Optional[DataFrame] = None,
        transit_changes: Optional[CubeTransit] = None,
        base_roadway_network: Optional[RoadwayNetwork] = None,
        base_transit_network: Optional[StandardTransit] = None,
        base_cube_transit_network: Optional[CubeTransit] = None,
        build_cube_transit_network: Optional[CubeTransit] = None,
        project_name: Optional[str] = None,
        recalculate_calculated_variables: Optional[bool] = False,
        recalculate_distance: Optional[bool] = False,
        parameters: Optional[dict] = {},
        **kwargs,
    ):
        """
        Constructor for a Project instance.

        Args:
            roadway_log_file: File path to consuming logfile or a list of logfile paths.
            roadway_shp_file: File path to consuming shape file for roadway changes.
            roadway_csv_file: File path to consuming csv file for roadway changes.
            network_build_file: File path to consuming EMME network build for network changes.
            base_roadway_dir: Folder path to base roadway network.
            base_transit_dir: Folder path to base transit network.
            base_cube_transit_source: Folder path to base transit network or cube line file string.
            base_cube_transit_file: File path to base transit network.
            build_cube_transit_source: Folder path to build transit network or cube line file string.
            build_cube_transit_file: File path to build transit network.
            roadway_link_changes: pandas dataframe of CUBE roadway link changes.
            roadway_node_changes: pandas dataframe of CUBE roadway node changes.
            transit_changes: build transit changes.
            base_roadway_network: Base roadway network object.
            base_cube_transit_network: Base cube transit network object.
            build_cube_transit_network: Build cube transit network object.
            project_name:  If not provided, will default to the roadway_log_file filename if
                provided (or the first filename if a list is provided)
            recalculate_calculated_variables: if reading in a base network, if this is true it
                will recalculate variables such as area type, etc. This only needs to be true
                if you are creating project cards that are changing the calculated variables.
            recalculate_distance:  recalculate the distance variable. This only needs to be
                true if you are creating project cards that change the distance.
            parameters: dictionary of parameters
            crs (int): coordinate reference system, ESPG number
            node_foreign_key (str):  variable linking the node table to the link table
            link_foreign_key (list): list of variable linking the link table to the node foreign key
            shape_foreign_key (str): variable linking the links table and shape table
            unique_link_ids (list): list of variables unique to each link
            unique_node_ids (list): list of variables unique to each node
            modes_to_network_link_variables (dict): Mapping of modes to link variables in
                the network
            modes_to_network_nodes_variables (dict): Mapping of modes to node variables
                in the network
            managed_lanes_node_id_scalar (int): Scalar values added to primary keys for nodes for
                corresponding managed lanes.
            managed_lanes_link_id_scalar (int): Scalar values added to primary keys for links for
                corresponding managed lanes.
            managed_lanes_required_attributes (list): attributes that must be specified in managed
                lane projects.
            keep_same_attributes_ml_and_gp (list): attributes to copy to managed lanes from parallel
                general purpose lanes.

        Returns:
            A Project instance.
        """

        if base_cube_transit_source and base_cube_transit_network:
            msg = "Method takes only one of 'base_cube_transit_source' and 'base_cube_transit_network' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if base_cube_transit_source:
            base_cube_transit_network = CubeTransit.create_from_cube(base_cube_transit_source, parameters)
            WranglerLogger.debug(
                "Base network has {} lines".format(len(base_cube_transit_network.lines))
            )
            if len(base_cube_transit_network.lines) <= 10:
                WranglerLogger.debug(
                    "Base network lines: {}".format(
                        "\n - ".join(base_cube_transit_network.lines)
                    )
                )
        elif base_cube_transit_network:
            pass
        else:
            msg = "No base cube transit network."
            WranglerLogger.info(msg)
            base_cube_transit_network = None

        if build_cube_transit_source and transit_changes:
            msg = "Method takes only one of 'build_cube_transit_source' and 'transit_changes' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if build_cube_transit_source:
            WranglerLogger.debug("build")
            build_cube_transit_network = CubeTransit.create_from_cube(build_cube_transit_source, parameters)
            WranglerLogger.debug(
                "Build network has {} lines".format(len(build_cube_transit_network.lines))
            )
            if len(build_cube_transit_network.lines) <= 10:
                WranglerLogger.debug(
                    "Build network lines: {}".format(
                        "\n - ".join(build_cube_transit_network.lines)
                    )
                )
        elif transit_changes:
            pass
        else:
            msg = "No cube transit changes given or processed."
            WranglerLogger.info(msg)
            transit_changes = None

        if roadway_log_file and (roadway_link_changes or roadway_node_changes):
            msg = "Method takes only one of 'roadway_log_file' and 'roadway_changes' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if roadway_shp_file and (roadway_link_changes or roadway_node_changes):
            msg = "Method takes only one of 'roadway_shp_file' and 'roadway_changes' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if roadway_csv_file and (roadway_link_changes or roadway_node_changes):
            msg = "Method takes only one of 'roadway_csv_file' and 'roadway_changes' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if roadway_log_file and roadway_csv_file:
            msg = "Method takes only one of 'roadway_log_file' and 'roadway_csv_file' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if roadway_shp_file and roadway_csv_file:
            msg = "Method takes only one of 'roadway_shp_file' and 'roadway_csv_file' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if roadway_log_file and roadway_shp_file:
            msg = "Method takes only one of 'roadway_log_file' and 'roadway_shp_file' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if roadway_log_file and not project_name:
            if type(roadway_log_file) == list:
                project_name = os.path.splitext(os.path.basename(roadway_log_file[0]))[
                    0
                ]
                WranglerLogger.info(
                    "No Project Name - Using name of first log file in list"
                )
            else:
                project_name = os.path.splitext(os.path.basename(roadway_log_file))[0]
                WranglerLogger.info("No Project Name - Using name of log file")
        if network_build_file and not project_name:
            if type(network_build_file) == list:
                with open(network_build_file[0]) as f:
                    _content = json.load(f)
                project_name = (
                    _content.get('metadata').get('project_title') + ' ' +
                    _content.get('metadata').get('date') + ' ' +
                    _content.get('metadata').get('comments')
                )
                WranglerLogger.info(
                    "No Project Name - Using metadata of first network build file in list"
                )
            else:
                with open(network_build_file) as f:
                    _content = json.load(f)
                project_name = (
                    _content.get('metadata').get('project_title') + ' ' +
                    _content.get('metadata').get('date') + ' ' +
                    _content.get('metadata').get('comments')
                )
                WranglerLogger.info("No Project Name - Using metadata of network build file")
        if roadway_log_file:
            roadway_link_changes, roadway_node_changes = Project.read_logfile(roadway_log_file)
        elif roadway_shp_file:
            roadway_changes = gpd.read_file(roadway_shp_file)
            roadway_link_changes = roadway_changes[roadway_changes.OBJECT == 'L'].copy()
            roadway_node_changes = roadway_changes[roadway_changes.OBJECT == 'N'].copy()
            roadway_link_changes = DataFrame(roadway_link_changes.drop("geometry", axis=1))
            roadway_node_changes = DataFrame(roadway_node_changes.drop("geometry", axis=1))
            roadway_node_changes["model_node_id"] = 0
        elif roadway_csv_file:
            roadway_changes = pd.read_csv(roadway_csv_file)
            roadway_link_changes = roadway_changes[roadway_changes.OBJECT == 'L'].copy()
            roadway_node_changes = roadway_changes[roadway_changes.OBJECT == 'N'].copy()
            roadway_node_changes["model_node_id"] = 0
        elif network_build_file:
            roadway_link_changes, roadway_node_changes, transit_changes = Project.read_network_build_file(network_build_file)
            if emme_node_id_crosswalk_file:
                # get wrangler IDs from emme element_id
                roadway_link_changes, roadway_node_changes, transit_changes = Project.emme_id_to_wrangler_id(
                    roadway_link_changes, 
                    roadway_node_changes, 
                    transit_changes,
                    emme_node_id_crosswalk_file
                )
            else:
                msg = "User needs to specify emme node id crosswalk file using emme_node_id_crosswalk_file = "
                WranglerLogger.error(msg)
                raise ValueError(msg)
            # rename emme attributes to wrangler attributes
            if emme_name_crosswalk_file is None:
                emme_name_crosswalk_file = parameters.emme_name_crosswalk_file
            roadway_link_changes, roadway_node_changes = Project.emme_name_to_wrangler_name(
                roadway_link_changes, 
                roadway_node_changes, 
                emme_name_crosswalk_file
            )
        elif roadway_link_changes:
            pass
        elif roadway_node_changes:
            pass
        else:
            msg = "No roadway changes given or processed."
            WranglerLogger.info(msg)
            roadway_link_changes = pd.DataFrame({})
            roadway_node_changes = pd.DataFrame({})

        if base_roadway_network and base_roadway_dir:
            msg = "Method takes only one of 'base_roadway_network' and 'base_roadway_dir' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if base_roadway_dir:
            base_roadway_network = ModelRoadwayNetwork.read(
                os.path.join(base_roadway_dir, "link.json"),
                os.path.join(base_roadway_dir, "node.geojson"),
                os.path.join(base_roadway_dir, "shape.geojson"),
                fast=True,
                recalculate_calculated_variables=recalculate_calculated_variables,
                recalculate_distance=recalculate_distance,
                parameters=parameters,
                **kwargs,
            )
            base_roadway_network.split_properties_by_time_period_and_category()
        elif base_roadway_network:
            if not isinstance(base_roadway_network, ModelRoadwayNetwork):
                base_roadway_network = ModelRoadwayNetwork.from_RoadwayNetwork(
                    roadway_network_object=base_roadway_network, 
                    parameters=parameters
                )
            base_roadway_network.split_properties_by_time_period_and_category()
        else:
            msg = "No base roadway network."
            WranglerLogger.info(msg)
            base_roadway_network = None

        if base_cube_transit_source and base_transit_dir:
            msg = "Method takes only one of 'base_cube_transit_source' and 'base_transit_dir' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if base_transit_dir:
            base_transit_network = StandardTransit.read_gtfs(
                gtfs_feed_dir=base_transit_dir,
                parameters=parameters
            )
        elif base_transit_network:
            base_transit_network = base_transit_network
        else:
            msg = "No base transit network."
            WranglerLogger.info(msg)
            base_transit_network = None

        project = Project(
            roadway_link_changes=roadway_link_changes,
            roadway_node_changes=roadway_node_changes,
            transit_changes=transit_changes,
            base_roadway_network=base_roadway_network,
            base_transit_network=base_transit_network,
            base_cube_transit_network=base_cube_transit_network,
            build_cube_transit_network=build_cube_transit_network,
            evaluate=True,
            project_name=project_name,
            parameters=parameters,
        )

        return project

    @staticmethod
    def read_logfile(logfilename: Union[str, List[str]]):
        """
        Reads a Cube log file and returns separate dataframes of roadway_link_changes and roadway_node_changes

        Args:
            logfilename (str or list[str]): File path to CUBE logfile or list of logfile paths.

        Returns:
            A DataFrame reprsentation of the log file.
        """
        if type(logfilename) == str:
            logfilename = [logfilename]

        link_df = pd.DataFrame()
        node_df = pd.DataFrame()

        for file in logfilename:
            WranglerLogger.info("Reading logfile: {}".format(file))
            with open(file) as f:
                _content = f.readlines()

                _node_lines = [
                    x.strip().replace(";", ",") for x in _content if x.startswith("N")
                ]
                WranglerLogger.debug("node lines: {}".format(_node_lines))
                _link_lines = [
                    x.strip().replace(";", ",") for x in _content if x.startswith("L")
                ]
                WranglerLogger.debug("link lines: {}".format(_link_lines))

                _nodecol = ["OBJECT", "OPERATION", "GROUP"] + _node_lines[0].split(",")[
                    1:
                ]
                WranglerLogger.debug("Node Cols: {}".format(_nodecol))
                _linkcol = ["OBJECT", "OPERATION", "GROUP"] + _link_lines[0].split(",")[
                    1:
                ]
                WranglerLogger.debug("Link Cols: {}".format(_linkcol))

                def split_log(x):
                    return list(reader([x], delimiter=',', quotechar='"'))[0]

                _node_df = pd.DataFrame([split_log(x) for x in _node_lines[1:]],columns = _nodecol)
                WranglerLogger.debug("Node DF: {}".format(_node_df))
                _link_df = pd.DataFrame([split_log(x) for x in _link_lines[1:]],columns = _linkcol)
                WranglerLogger.debug("Link DF: {}".format(_link_df))

                node_df = pd.concat([node_df, _node_df])
                link_df = pd.concat([link_df, _link_df])

        # CUBE logfile headers for string fields: NAME[111] instead of NAME, need to shorten that
        link_df.columns = [c.split("[")[0] for c in link_df.columns]
        # CUBE logfile headers for string fields: NAME[111] instead of NAME, need to shorten that
        node_df.columns = [c.split("[")[0] for c in node_df.columns]

        if len(link_df) > 0:
            # create operation history
            action_history_df = (
                link_df.groupby(['A', 'B'])["OPERATION"]
                .agg(lambda x: x.tolist())
                .rename("operation_history")
                .reset_index()
            )

            link_df = pd.merge(link_df, action_history_df, on=['A', 'B'], how="left")
            link_df.drop_duplicates(subset=['A', 'B'], keep="last", inplace=True)
            link_df["operation_final"] = link_df.apply(lambda x: Project._final_op(x), axis=1)

        if len(node_df) > 0:
            action_history_df = (
                node_df.groupby('N')["OPERATION"]
                .agg(lambda x: x.tolist())
                .rename("operation_history")
                .reset_index()
            )

            node_df = pd.merge(node_df, action_history_df, on=['N'], how="left")
            node_df.drop_duplicates(subset=['N'], keep="last", inplace=True)
            node_df["operation_final"] = node_df.apply(lambda x: Project._final_op(x), axis=1)

        WranglerLogger.info(
            "Processed {} Node lines and {} Link lines".format(
                node_df.shape[0], link_df.shape[0]
            )
        )

        return link_df, node_df

    @staticmethod
    def read_network_build_file(networkbuildfilename: Union[str, List[str]]):
        """
        Reads a emme network build file and returns separate dataframes of roadway_link_changes and roadway_node_changes

        Args:
            networkbuildfilename (str or list[str]): File path to emme nework build file or list of network build file paths.

        Returns:
            A DataFrame representation of the network build file
        """
        if type(networkbuildfilename) == str:
            networkbuildfilename = [networkbuildfilename]

        _link_command_history_df = DataFrame()
        _node_command_history_df = DataFrame()
        _transit_command_history_df = DataFrame()

        for file in networkbuildfilename:
            WranglerLogger.info("Reading network build file: {}".format(file))
            with open(file) as f:
                _content = json.load(f)

            _command_history = _content.get('command_history')

            # loop through all the commands
            for command in _command_history:
                if command.get('command') == 'set_attribute':
                    element_id = command.get('parameters').get('element_ids')
                    object = Project.get_object_from_network_build_command(command)
                    operation = Project.get_operation_from_network_build_command(command)

                    _command_df = DataFrame(
                        data = {
                            'element_id' : element_id,
                            'object' : object,
                            'operation' : operation
                        }
                    )

                    _command_df[command.get('parameters').get('attribute_name')] = command.get('parameters').get('value')
                
                if command.get('command') in ['create_link', 'create_node']:
                    if command.get('command') == 'create_link':
                        element_id = command.get('results').get('changes').get('added').get('LINK')
                    if command.get('command') == 'create_node':
                        element_id = command.get('results').get('changes').get('added').get('NODE')
                    object = Project.get_object_from_network_build_command(command)
                    operation = Project.get_operation_from_network_build_command(command)

                    _command_df = DataFrame(
                        data = {
                            'element_id' : element_id,
                            'object' : object,
                            'operation' : operation
                        }
                    )

                    for attribute_name, attribute_value in command.get('parameters').get('attributes').items():
                        _command_df[attribute_name] = attribute_value
                
                if command.get('command') == 'delete_link':
                    element_id = command.get('results').get('changes').get('removed').get('LINK')
                    object = Project.get_object_from_network_build_command(command)
                    operation = Project.get_operation_from_network_build_command(command)

                    _command_df = DataFrame(
                        data = {
                            'element_id' : element_id,
                            'object' : object,
                            'operation' : operation
                        }
                    )

                if command.get('command') == 'modify_transit_line':
                    element_id = command.get('parameters').get('line_id')
                    object = Project.get_object_from_network_build_command(command)
                    operation = Project.get_operation_from_network_build_command(command)
                    
                    _command_df = DataFrame(
                        data = {
                            'element_id' : pd.Series(element_id),
                            'object' : pd.Series(object),
                            'operation' : pd.Series(operation)
                        }
                    )

                    _command_df['new_itinerary'] = [command.get('parameters').get('new_itinerary')]

                if ('L' in _command_df['object'].unique()):
                    _link_command_history_df = _link_command_history_df.append(
                        _command_df[_command_df['object'] == 'L'],
                        sort = False,
                        ignore_index = True
                    )

                if ('N' in _command_df['object'].unique()):
                    _node_command_history_df = _node_command_history_df.append(
                        _command_df[_command_df['object'] == 'N'],
                        sort = False,
                        ignore_index = True
                    )
                
                if (
                    ('TRANSIT_LINE' in _command_df['object'].unique()) | 
                    ('TRANSIT_STOP' in _command_df['object'].unique()) |
                    ('TRANSIT_SHAPE' in _command_df['object'].unique()) 
                ):
                    _transit_command_history_df = _transit_command_history_df.append(
                        _command_df[_command_df['object'].isin(['TRANSIT_LINE', 'TRANSIT_STOP', 'TRANSIT_SHAPE'])],
                        sort = False,
                        ignore_index = True
                    )

            if len(_link_command_history_df) > 0:
                # create operation history
                link_action_history_df = (
                    _link_command_history_df.groupby('element_id')["operation"]
                    .agg(lambda x: x.tolist())
                    .rename("operation_history")
                    .reset_index()
                )
                link_action_history_df["operation_final"] = link_action_history_df.apply(
                    lambda x: Project._final_op(x), 
                    axis=1
                )
                
                # get the last none null value for each element
                # consolidate elements to single record
                def get_last_valid(series):
                    if len(series.dropna()) > 0:
                        return series.dropna().iloc[-1]
                    else:
                        return np.nan

                #_command_history_df = _command_history_df.groupby(['element_id']).apply(get_last_valid).reset_index()
                _link_command_history_df = _link_command_history_df.groupby(
                    ['element_id']
                ).last().reset_index()

                _link_command_history_df = pd.merge(_link_command_history_df, link_action_history_df, on='element_id', how="left")

            if len(_node_command_history_df) > 0:
            # create node operation history
                node_action_history_df = (
                    _node_command_history_df.groupby('element_id')["operation"]
                    .agg(lambda x: x.tolist())
                    .rename("operation_history")
                    .reset_index()
                )
                node_action_history_df["operation_final"] = node_action_history_df.apply(
                    lambda x: Project._final_op(x), 
                    axis=1
                )
            
                _node_command_history_df = _node_command_history_df.groupby(
                    ['element_id']
                ).last().reset_index()

                _node_command_history_df = pd.merge(_node_command_history_df, node_action_history_df, on='element_id', how="left")

        WranglerLogger.info(
            "Processed {} link element commands, {} node element commands".format(
                _link_command_history_df.shape[0],
                _node_command_history_df.shape[0]
            )
        )

        return _link_command_history_df, _node_command_history_df, _transit_command_history_df

    @staticmethod
    def emme_id_to_wrangler_id(emme_link_change_df, emme_node_change_df, emme_transit_changes_df, emme_node_id_crosswalk_file):
        """
        rewrite the emme id with wrangler id, using the emme wrangler id crosswalk located in database folder
        """
        WranglerLogger.info('Reading emme node id crosswalk file from {}'.format(emme_node_id_crosswalk_file))
        emme_node_id_crosswalk_df = pd.read_csv(emme_node_id_crosswalk_file)
        emme_node_id_dict = dict(zip(emme_node_id_crosswalk_df['emme_node_id'], emme_node_id_crosswalk_df['model_node_id']))

        # get node changes
        if len(emme_node_change_df) > 0:
            emme_node_change_df['emme_id'] = emme_node_change_df['element_id'].apply(lambda x: int(x.split('-')[0]))

            # get new emme nodes
            new_emme_node_id_list = [
                n for n in emme_node_change_df['emme_id'].to_list() if n not in emme_node_id_crosswalk_df['emme_node_id'].to_list()
            ]
            WranglerLogger.info('New emme node id list {}'.format(new_emme_node_id_list))
            new_wrangler_node = emme_node_id_crosswalk_df['model_node_id'].max()

            # add crosswalk for new emme nodes
            for new_emme_node in new_emme_node_id_list:
                if new_emme_node in emme_node_id_dict.keys():
                    msg = "new node id {} has already been added to the crosswalk".format(new_emme_node)
                    WranglerLogger.error(msg)
                    raise ValueError(msg)
                else:
                    new_wrangler_node = new_wrangler_node + 1
                    emme_node_id_dict.update({new_emme_node : new_wrangler_node})
                    new_emme_node_id_crosswalk_df = pd.DataFrame(emme_node_id_dict.items(), columns=['emme_node_id', 'model_node_id'])
                    new_emme_node_id_crosswalk_df.to_csv(emme_node_id_crosswalk_file, index=False)
                
            # for nodes update model_node_id
            emme_node_change_df['model_node_id'] = emme_node_change_df['emme_id'].map(emme_node_id_dict).fillna(0)
        
        if len(emme_link_change_df) > 0:
            emme_link_change_df['A'] = emme_link_change_df['element_id'].apply(lambda x: int(x.split('-')[0]))
            emme_link_change_df['B'] = emme_link_change_df['element_id'].apply(lambda x: int(x.split('-')[-1]))
            # for links update A,B nodes
            emme_link_change_df['A'] = emme_link_change_df['A'].map(emme_node_id_dict)
            emme_link_change_df['B'] = emme_link_change_df['B'].map(emme_node_id_dict)
        
        if len(emme_transit_changes_df) > 0:
            emme_transit_changes_df['i_node'] = emme_transit_changes_df.apply(
                lambda x: x['element_id'].split('-')[-3] if x['object'] == 'TRANSIT_STOP' else 0,
                axis = 1
            )

            emme_transit_changes_df['j_node'] = emme_transit_changes_df.apply(
                lambda x: x['element_id'].split('-')[-2] if x['object'] == 'TRANSIT_STOP' else 0,
                axis = 1
            )
            
            # update i,j nodes
            emme_transit_changes_df['i_node'] = emme_transit_changes_df[
                'i_node'
                ].astype(
                    int
                ).map(
                    emme_node_id_dict
                ).fillna(0).astype(int)
            emme_transit_changes_df['j_node'] = emme_transit_changes_df[
                'j_node'
                ].astype(
                    int
                ).map(
                    emme_node_id_dict
                ).fillna(0).astype(int)

            # update routing nodes
            emme_transit_changes_df['new_itinerary'] = emme_transit_changes_df.apply(
                lambda x: [emme_node_id_dict.get(n) for n in x['new_itinerary']] if x['object'] == 'TRANSIT_SHAPE' else 0,
                axis = 1
            )
            
        return emme_link_change_df, emme_node_change_df, emme_transit_changes_df

    def get_object_from_network_build_command(row):
        """
        determine the network build object is node or link

        Args:
            row: network build command history dataframe

        Returns:
            'N' for node, 'L' for link
        """

        if row.get('command') == 'create_link':
            return 'L'

        if row.get('command') == 'create_node':
            return 'N'

        if row.get('command') == 'delete_link':
            return 'L'

        if row.get('command') == 'set_attribute':
            if row.get('parameters').get('element_type') == 'LINK':
                return 'L'
            if row.get('parameters').get('element_type') == 'NODE':
                return 'N'
            if row.get('parameters').get('element_type') == 'TRANSIT_LINE':
                return 'TRANSIT_LINE'
            if row.get('parameters').get('element_type') == 'TRANSIT_SEGMENT':
                return 'TRANSIT_STOP'
        
        if row.get('command') == 'modify_transit_line':
            return 'TRANSIT_SHAPE'

    def get_operation_from_network_build_command(row):
        """
        determine the network build object action type

        Args:
            row: network build command history dataframe

        Returns:
            'A', 'C', 'D'
        """

        if row.get('command') == 'create_link':
            return 'A'

        if row.get('command') == 'create_node':
            return 'A'

        if row.get('command') == 'delete_link':
            return 'D'

        if row.get('command') == 'set_attribute':
            if row.get('parameters').get('element_type') == 'LINK':
                return 'C'
            if row.get('parameters').get('element_type') == 'NODE':
                return 'C'
            if row.get('parameters').get('element_type') == 'TRANSIT_LINE':
                return 'C'
            if row.get('parameters').get('element_type') == 'TRANSIT_SEGMENT':
                return 'C'

        if row.get('command') == 'modify_transit_line':
            return 'C'

    @staticmethod
    def emme_name_to_wrangler_name(emme_link_change_df, emme_node_change_df, emme_name_crosswalk_file):
        """
        rename emme names to wrangler names using crosswalk file
        """

        WranglerLogger.info('Reading emme attribute name crosswalk file {}'.format(emme_name_crosswalk_file))
        emme_name_crosswalk_df = pd.read_csv(emme_name_crosswalk_file)
        emme_name_crosswalk_dict = dict(zip(emme_name_crosswalk_df['emme_name'], emme_name_crosswalk_df['wrangler_name']))

        # drop columns we don't need from emme to avoid confusion
        ignore_columns = [
            c for c in emme_link_change_df.columns if c not in list(emme_name_crosswalk_dict.keys()) + ['operation_final', 'A', 'B']
        ]
        WranglerLogger.info('Ignoring link changes in {}'.format(ignore_columns))
        emme_link_change_df = emme_link_change_df.drop(ignore_columns, axis = 1)
        
        ignore_columns = [
            c for c in emme_node_change_df.columns if c not in list(emme_name_crosswalk_dict.keys()) + ['operation_final', 'model_node_id']
        ]
        WranglerLogger.info('Ignoring node changes in {}'.format(ignore_columns))
        emme_node_change_df = emme_node_change_df.drop(ignore_columns, axis = 1)

        # rename emme name to wrangler name
        emme_link_change_df.rename(columns = emme_name_crosswalk_dict, inplace = True)
        emme_node_change_df.rename(columns = emme_name_crosswalk_dict, inplace = True)

        return emme_link_change_df, emme_node_change_df

    @staticmethod
    def determine_roadway_network_changes_compatibility(
        base_roadway_network: ModelRoadwayNetwork,
        roadway_link_changes: DataFrame,
        roadway_node_changes: DataFrame,
        parameters: Parameters,
    ):
        """
        Checks to see that any links or nodes that change exist in base roadway network.
        """
        WranglerLogger.info(
            "Evaluating compatibility between roadway network changes and base network. Not evaluating deletions."
        )

        # CUBE log file saves all variable names in upper cases, need to convert them to be same as network
        log_to_net_df = pd.read_csv(parameters.log_to_net_crosswalk)
        log_to_net_dict = dict(zip(log_to_net_df["log"], log_to_net_df["net"]))

        dbf_to_net_df = pd.read_csv(parameters.net_to_dbf_crosswalk)
        dbf_to_net_dict = dict(zip(dbf_to_net_df["dbf"], dbf_to_net_df["net"]))

        for c in roadway_link_changes.columns:
            if (c not in log_to_net_df["log"].tolist() + log_to_net_df["net"].tolist()) & (c not in ["A", "B"]):
                roadway_link_changes.rename(columns={c : c.lower()}, inplace=True)
        roadway_link_changes.rename(columns=log_to_net_dict, inplace=True)
        roadway_link_changes.rename(columns=dbf_to_net_dict, inplace=True)

        for c in roadway_node_changes.columns:
            if (c not in log_to_net_df["log"].tolist() + log_to_net_df["net"].tolist()) & (c not in ["A", "B", "X", "Y"]):
                roadway_node_changes.rename(columns={c : c.lower()}, inplace=True)
        roadway_node_changes.rename(columns=log_to_net_dict, inplace=True)
        roadway_node_changes.rename(columns=dbf_to_net_dict, inplace=True)

        # for links "L"  that change "C",
        # find locations where there isn't a base roadway link
        if len(roadway_link_changes) > 0:
            link_changes_df = roadway_link_changes[
                roadway_link_changes["operation_final"] == "C"
            ].copy()

            link_merge_df = pd.merge(
                link_changes_df[["A", "B"]].astype(str),
                base_roadway_network.links_df[["A", "B", "model_link_id"]].astype(str),
                how="left",
                on=["A", "B"],
            )

            missing_links = link_merge_df.loc[link_merge_df["model_link_id"].isna()]

            if missing_links.shape[0]:
                msg = "Network missing the following AB links:\n{}".format(missing_links)
                WranglerLogger.error(msg)
                raise ValueError(msg)

        # for links "N"  that change "C",
        # find locations where there isn't a base roadway node
        if len(roadway_node_changes) > 0:
            node_changes_df = roadway_node_changes[
                roadway_node_changes["operation_final"] == "C"
            ].copy()

            node_merge_df = pd.merge(
                node_changes_df[["model_node_id"]],
                base_roadway_network.nodes_df[["model_node_id", "geometry"]],
                how="left",
                on=["model_node_id"],
            )
            missing_nodes = node_merge_df.loc[node_merge_df["geometry"].isna()]
            if missing_nodes.shape[0]:
                msg = "Network missing the following nodes:\n{}".format(missing_nodes)
                WranglerLogger.error(msg)
                raise ValueError(msg)

    def evaluate_changes(self):
        """
        Determines which changes should be evaluated, initiates
        self.card_data to be an aggregation of transit and highway changes.
        """
        highway_change_list = []
        transit_change_list = []

        WranglerLogger.info("Evaluating project changes.")

        if (not self.roadway_link_changes.empty) | (not self.roadway_node_changes.empty):
            highway_change_list = self.add_highway_changes()

        if self.transit_changes is not None:
            if (not self.transit_changes.empty)  or (
                self.base_cube_transit_network is not None
                and self.build_cube_transit_network is not None
            ):
                transit_change_list = self.add_transit_changes()

        if (
                self.base_cube_transit_network is not None
                and self.build_cube_transit_network is not None
            ):
                transit_change_list = self.add_transit_changes()

        self.card_data = {
            "project": self.project_name,
            "changes": transit_change_list + highway_change_list,
        }

    def add_transit_changes(self):
        """
        Evaluates changes between base and build transit objects and
        adds entries into the self.card_data dictionary.
        """
        if self.build_cube_transit_network:
            transit_change_list = self.build_cube_transit_network.evaluate_differences(
                self.base_cube_transit_network
            )
        elif self.base_transit_network:
            transit_change_list = self.base_transit_network.evaluate_differences(
                self.transit_changes
            )
        return transit_change_list
    
    @staticmethod
    def _final_op(x):
        if x["operation_history"][-1] == "D":
            if "A" in x["operation_history"][:-1]:
                return "N"
            else:
                return "D"
        elif x["operation_history"][-1] == "A":
            if "D" in x["operation_history"][:-1]:
                return "C"
            else:
                return "A"
        else:
            if "A" in x["operation_history"][:-1]:
                return "A"
            else:
                return "C"

    def add_highway_changes(self, limit_variables_to_existing_network=False):
        """
        Evaluates changes from the log file based on the base highway object and
        adds entries into the self.card_data dictionary.

        Args:
            limit_variables_to_existing_network (bool): True if no ad-hoc variables.  Default to False.
        """

        for c in self.parameters.string_col:
            if c in self.roadway_link_changes.columns:
                self.roadway_link_changes[c] = self.roadway_link_changes[c].str.lstrip(" ")
            if c in self.roadway_node_changes.columns:
                self.roadway_node_changes[c] = self.roadway_node_changes[c].str.lstrip(" ")

        ## if worth it, could also add some functionality  to network wrangler itself.
        node_changes_df = self.roadway_node_changes.copy()

        link_changes_df = self.roadway_link_changes.copy()

        def _process_deletions(link_changes_df):
            """
            create deletion section in project card
            """
            WranglerLogger.debug("Processing link deletions")

            cube_delete_df = link_changes_df[link_changes_df["operation_final"] == "D"].copy()

            # make sure columns has the same type as base network
            cube_delete_df['A'] = cube_delete_df['A'].astype(
                type(self.base_roadway_network.links_df['A'].iloc[0])
            )
            cube_delete_df['B'] = cube_delete_df['B'].astype(
                type(self.base_roadway_network.links_df['B'].iloc[0])
            )

            if 'model_link_id' in cube_delete_df.columns:
                cube_delete_df.drop(['model_link_id'], axis = 1, inplace = True)

            cube_delete_df = pd.merge(
                cube_delete_df,
                self.base_roadway_network.links_df[['A', 'B', 'model_link_id']],
                how = 'left',
                on = ['A', 'B']
            )

            if len(cube_delete_df) > 0:
                links_to_delete = cube_delete_df["model_link_id"].tolist()
                delete_link_dict = {
                    "category": "Roadway Deletion",
                    "links": {"model_link_id": links_to_delete},
                }
                WranglerLogger.debug("{} Links Deleted.".format(len(links_to_delete)))
            else:
                delete_link_dict = None
                WranglerLogger.debug("No link deletions processed")

            return delete_link_dict

        def _process_link_additions(
            link_changes_df, limit_variables_to_existing_network
        ):
            """"""
            WranglerLogger.debug("Processing link additions")
            cube_add_df = link_changes_df[link_changes_df["operation_final"] == "A"]
            if len(cube_add_df) == 0:
                WranglerLogger.debug("No link additions processed")
                return {}

            if limit_variables_to_existing_network:
                add_col = [
                    c
                    for c in cube_add_df.columns
                    if c in self.base_roadway_network.links_df.columns
                ]
            else:
                add_col = [
                    c for c in cube_add_df.columns if c not in ["operation_final"]
                ]
                # can leave out "operation_final" from writing out, is there a reason to write it out?

            for x in add_col:
                cube_add_df[x] = cube_add_df[x].astype(self.base_roadway_network.links_df[x].dtype)
            
            add_col_final = add_col.copy()
            # properties that are split by time period and category
            for c in add_col:
                if "_" not in c:
                    continue
                # WranglerLogger.debug("Processing Column: {}".format(c))
                (
                    p_base_name,
                    p_time_period,
                    p_category,
                    managed_lane,
                ) = column_name_to_parts(c, self.parameters) 

                if p_base_name not in self.parameters.properties_to_split.keys():
                    continue

                if (p_base_name in cube_add_df.columns):
                    if cube_add_df[p_base_name].equals(cube_add_df[c]):
                        add_col_final.remove(c)
                        continue
                    else:
                        msg = "Detected different changes for split-property variables on regular roadway links: "
                        msg += "conflicting \"{}\" values \"{}\", \"{}\"".format(p_base_name, cube_add_df[p_base_name], cube_add_df[c])
                        WranglerLogger.error(msg)
                        raise ValueError(msg)

                else:
                    cube_add_df[p_base_name] = cube_add_df[c]
                    add_col_final.remove(c)
                    add_col_final.append(p_base_name)

            add_link_properties = cube_add_df[add_col_final].to_dict("records")            

            # WranglerLogger.debug("Add Link Properties: {}".format(add_link_properties))
            WranglerLogger.debug("{} Links Added".format(len(add_link_properties)))

            return {"category": "Add New Roadway", "links": add_link_properties}

        def _process_node_additions(node_add_df):
            """"""
            WranglerLogger.debug("Processing node additions")

            if len(node_add_df) == 0:
                WranglerLogger.debug("No node additions processed")
                return []

            node_add_df = node_add_df.drop(["operation_final"], axis=1)

            node_add_df = node_add_df.apply(_reproject_coordinates, axis=1)

            for x in node_add_df.columns:
                node_add_df[x] = node_add_df[x].astype(self.base_roadway_network.nodes_df[x].dtype)

            add_nodes_dict_list = node_add_df.to_dict(
                "records"
            )
            WranglerLogger.debug("{} Nodes Added".format(len(add_nodes_dict_list)))

            return add_nodes_dict_list

        def _reproject_coordinates(row):
            reprojected_x, reprojected_y = self.parameters.transformer.transform(row['X'], row['Y'])
            row['X'] = reprojected_x
            row['Y'] = reprojected_y
            return row

        def _process_single_link_change(change_row, changeable_col):
            """"""

            #  1. Find associated base year network values
            base_df = self.base_roadway_network.links_df[
                (self.base_roadway_network.links_df["A"] == int(change_row.A))
                & (self.base_roadway_network.links_df["B"] == int(change_row.B))
            ]

            if not base_df.shape[0]:
                msg = "No match found in network for AB combination: ({},{}). Incompatible base network.".format(
                    change_row.A, change_row.B
                )
                WranglerLogger.error(msg)
                raise ValueError(msg)

            elif base_df.shape[0] > 1:
                WranglerLogger.warning(
                    "Found more than one match in base network for AB combination: ({},{}). Selecting first one to operate on but AB should be unique to network.".format(
                        change_row.A, change_row.B
                    )
                )

            base_row = base_df.iloc[0]
            # WranglerLogger.debug("Properties with changes: {}".format(changeable_col))

            # 2. find columns that changed (enough)
            changed_col = []
            for col in changeable_col:
                WranglerLogger.debug("Assessing Column: {}".format(col))
                # if it is the same as before, or a static value, don't process as a change
                if str(change_row[col]).strip('"\'') == str(base_row[col]).strip('"\''):
                    continue
                # if it is NaN or None, don't process as a change
                if (change_row[col] != change_row[col]) | (change_row[col] is None):
                    continue
                if (col == "roadway_class") & (change_row[col] == 0):
                    continue
                # only look at distance if it has significantly changed
                if col == "distance":
                    if (
                        abs(
                            (float(change_row[col]) - float(base_row[col]))
                            / base_row[col].astype(float)
                        )
                        > 0.01
                    ):
                        change_row[col] = type(base_row[col])(change_row[col])
                        changed_col.append(col)
                    else:
                        continue
                else:
                    change_row[col] = type(base_row[col])(change_row[col])
                    changed_col.append(col)

            WranglerLogger.debug(
                "Properties with changes that will be processed: {}".format(changed_col)
            )

            if not changed_col:
                return pd.DataFrame()

            # 3. Iterate through columns with changed values and structure the changes as expected in  project card
            property_dict_list = []
            processed_properties = []

            # check if it's a manged lane change
            for c in changed_col:
                if c.startswith("ML_"):
                    # TODO ML project card skeleton
                    msg = "Detected managed lane changes, please create managed lane project card!"
                    WranglerLogger.error(msg)
                    raise ValueError(msg)
                    return

            # regular roadway property change
            for c in changed_col:
                # WranglerLogger.debug("Processing Column: {}".format(c))
                (
                    p_base_name,
                    p_time_period,
                    p_category,
                    managed_lane,
                ) = column_name_to_parts(c, self.parameters)

                _d = {
                    "existing": base_row[c],
                    "set": change_row[c],
                }
                if c in Project.CALCULATED_VALUES:
                    _d = {
                        "set": change_row[c],
                    }
                if p_time_period:
                    if managed_lane == 1:
                        _d["time"] = list(
                            self.parameters.time_period_to_time[p_time_period]
                        )
                        if p_category:
                            _d["category"] = p_category

                # iterate through existing properties that have been changed and see if you should just add
                if (p_base_name in processed_properties) & (managed_lane == 1):
                    for processed_p in property_dict_list:
                        if processed_p["property"] == p_base_name:
                            processed_p["timeofday"] += [_d]
                elif (p_base_name in processed_properties) & (managed_lane == 0):
                    for processed_p in property_dict_list:
                        if processed_p["property"] == p_base_name:
                            if processed_p["set"] != change_row[c]:
                                msg = "Detected different changes for split-property variables on regular roadway links: "
                                msg += "conflicting \"{}\" values \"{}\", \"{}\"".format(p_base_name, processed_p["set"], change_row[c])
                                WranglerLogger.error(msg)
                                raise ValueError(msg)
                elif p_time_period:
                    if managed_lane == 1:
                        property_dict = {"property": p_base_name, "timeofday": [_d]}
                        processed_properties.append(p_base_name)
                        property_dict_list.append(property_dict)
                    else:
                        _d["property"] = p_base_name
                        processed_properties.append(_d["property"])
                        property_dict_list.append(_d)
                else:
                    _d["property"] = p_base_name
                    processed_properties.append(_d["property"])
                    property_dict_list.append(_d)

            card_df = pd.DataFrame(
                {
                    "properties": pd.Series([property_dict_list]),
                    "model_link_id": pd.Series(base_row["model_link_id"]),
                }
            )

            # WranglerLogger.debug('single change card_df:\n {}'.format(card_df))

            return card_df

        def _process_link_changes(link_changes_df, changeable_col):
            """"""
            cube_change_df = link_changes_df[link_changes_df["operation_final"] == "C"].copy()
            
            # make sure columns has the same type as base network
            cube_change_df['A'] = cube_change_df['A'].astype(
                type(self.base_roadway_network.links_df['A'].iloc[0])
            )
            cube_change_df['B'] = cube_change_df['B'].astype(
                type(self.base_roadway_network.links_df['B'].iloc[0])
            )
            
            if 'model_link_id' in cube_change_df.columns:
                cube_change_df.drop('model_link_id', axis = 1, inplace = True)

            cube_change_df = pd.merge(
                cube_change_df,
                self.base_roadway_network.links_df[['A', 'B', 'model_link_id']],
                how = 'left',
                on = ['A', 'B']
            )

            if not cube_change_df.shape[0]:
                WranglerLogger.info("No link changes processed")
                return []

            change_link_dict_df = pd.DataFrame(columns=["properties", "model_link_id"])

            for index, row in cube_change_df.iterrows():
                card_df = _process_single_link_change(row, changeable_col)

                change_link_dict_df = pd.concat(
                    [change_link_dict_df, card_df], ignore_index=True, sort=False
                )

            if not change_link_dict_df.shape[0]:
                WranglerLogger.info("No link changes processed")
                return []

            # WranglerLogger.debug('change_link_dict_df Unaggregated:\n {}'.format(change_link_dict_df))

            # Have to change to string so that it is a hashable type for the aggregation
            change_link_dict_df["properties"] = change_link_dict_df[
                "properties"
            ].astype(str)
            # Group the changes that are the same
            change_link_dict_df = (
                change_link_dict_df.groupby("properties")[["model_link_id"]]
                .agg(lambda x: list(x))
                .reset_index()
            )
            # WranglerLogger.debug('change_link_dict_df Aggregated:\n {}'.format(change_link_dict_df))

            # Reformat model link id to correct "facility" format
            change_link_dict_df["facility"] = change_link_dict_df.apply(
                lambda x: {"link": [{"model_link_id": x.model_link_id}]}, axis=1
            )

            # WranglerLogger.debug('change_link_dict_df 3: {}'.format(change_link_dict_df))
            change_link_dict_df["properties"] = change_link_dict_df["properties"].apply(
                lambda x: json.loads(
                    x.replace("'\"", "'").replace("\"'", "'").replace("'", '"')
                )
            )

            change_link_dict_df["category"] = "Roadway Property Change"

            change_link_dict_list = change_link_dict_df[
                ["category", "facility", "properties"]
            ].to_dict("record")

            WranglerLogger.debug(
                "{} Changes Processed".format(len(change_link_dict_list))
            )
            return change_link_dict_list

        def _consolidate_actions(log, base, key_list):
            log_df = log.copy()
            # will be changed if to allow new variables being added/changed that are not in base network
            changeable_col = [x for x in log_df.columns if x in base.columns]
            #print(log_df)
            #for x in changeable_col:
            #    print(x)
                #log_df[x] = log_df[x].astype(base[x].dtype)

            if 'operation_final' not in log_df.columns:
                action_history_df = (
                    log_df.groupby(key_list)["operation"]
                    .agg(lambda x: x.tolist())
                    .rename("operation_history")
                    .reset_index()
                )

                log_df = pd.merge(log_df, action_history_df, on=key_list, how="left")
                log_df.drop_duplicates(subset=key_list, keep="last", inplace=True)
                log_df["operation_final"] = log_df.apply(lambda x: Project._final_op(x), axis=1)

            return log_df[changeable_col + ["operation_final"]]

        delete_link_dict = None
        add_link_dict = None
        change_link_dict_list = []

        if len(link_changes_df) != 0:
            link_changes_df = _consolidate_actions(
                link_changes_df, self.base_roadway_network.links_df, ["A", "B"]
            )

            # process deletions
            delete_link_dict = _process_deletions(link_changes_df)

            # process additions
            add_link_dict = _process_link_additions(
                link_changes_df, limit_variables_to_existing_network
            )

            # process changes
            WranglerLogger.debug("Processing changes")
            WranglerLogger.debug(link_changes_df)
            changeable_col = list(
                (
                    set(link_changes_df.columns)
                    & set(self.base_roadway_network.links_df.columns)
                )
                - set(Project.STATIC_VALUES)
                - set(["cntype","distance"]) # will be calculated in model network
            )

            cols_in_changes_not_in_net = list(
                set(link_changes_df.columns)
                - set(self.base_roadway_network.links_df.columns)
            )

            if cols_in_changes_not_in_net:
                WranglerLogger.warning(
                    "The following attributes are specified in the changes but do not exist in the base network: {}".format(
                        cols_in_changes_not_in_net
                    )
                )

            for c in changeable_col:
                if c in self.parameters.string_col:
                    self.base_roadway_network.links_df[c].fillna("", inplace=True)
                else:
                    self.base_roadway_network.links_df[c].fillna(0, inplace=True)
            
            change_link_dict_list = _process_link_changes(link_changes_df, changeable_col)

        if len(node_changes_df) != 0:
            node_changes_df = _consolidate_actions(
                node_changes_df, self.base_roadway_network.nodes_df, ["model_node_id"]
            )

            # print error message for node change and node deletion
            if (
                len(node_changes_df[node_changes_df["operation_final"].isin(["C", "D"])])
                > 0
            ):
                msg = "NODE changes and deletions are not allowed!"
                WranglerLogger.warning(msg)
                #raise ValueError(msg)
            node_add_df = node_changes_df[node_changes_df["operation_final"] == "A"]

            if add_link_dict:
                add_link_dict["nodes"] = _process_node_additions(node_add_df)
            else:
                add_link_dict = {"category": "Add New Roadway", "nodes": _process_node_additions(node_add_df)}

        else:
            None

        # combine together

        highway_change_list = list(
            filter(None, [delete_link_dict] + [add_link_dict] + change_link_dict_list)
        )

        return highway_change_list
