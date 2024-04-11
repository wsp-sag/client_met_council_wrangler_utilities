"""Transit-related classes to parse, compare, and write standard and cube transit files.

  Typical usage example:

    tn = CubeTransit.create_from_cube(CUBE_DIR)
    transit_change_list = tn.evaluate_differences(base_transit_network)

    cube_transit_net = StandardTransit.read_gtfs(BASE_TRANSIT_DIR)
    cube_transit_net.write_as_cube_lin(os.path.join(WRITE_DIR, "outfile.lin"))
"""
import os
import copy
import csv
import datetime, time
from typing import Any, Dict, Optional, Union

from lark import Lark, Transformer, v_args
from pandas import DataFrame

import pandas as pd
import partridge as ptg
import numpy as np

from network_wrangler import TransitNetwork

from .logger import WranglerLogger
from .parameters import Parameters

class CubeTransit(object):
    """Class for storing information about transit defined in Cube line
    files.

    Has the capability to:

     - Parse cube line file properties and shapes into python dictionaries
     - Compare line files and represent changes as Project Card dictionaries

    .. highlight:: python

    Typical usage example:
    ::
        tn = CubeTransit.create_from_cube(CUBE_DIR)
        transit_change_list = tn.evaluate_differences(base_transit_network)

    Attributes:
        lines (list): list of strings representing unique line names in
            the cube network.
        line_properties (dict): dictionary of line properties keyed by line name. Property
            values are stored in a dictionary by property name. These
            properties are directly read from the cube line files and haven't
            been translated to standard transit values.
        shapes (dict): dictionary of shapes
            keyed by line name. Shapes stored as a pandas DataFrame of nodes with following columns:
              - 'node_id' (int): positive integer of node id
              - 'node' (int): node number, with negative indicating a non-stop
              - 'stop' (boolean): indicates if it is a stop
              - 'order' (int):  order within this shape
        program_type (str): Either PT or TRNBLD
        parameters (Parameters):
            Parameters instance that will be applied to this instance which
            includes information about time periods and variables.
        source_list (list):
            List of cube line file sources that have been read and added.
        diff_dict (dict):
    """

    def __init__(self, parameters: Union[Parameters, dict] = {}):
        """
        Constructor  for CubeTransit

        parameters: dictionary of parameter settings (see Parameters class) or an instance of Parameters
        """
        WranglerLogger.debug("Creating a new Cube Transit instance")

        self.lines = []

        self.line_properties = {}
        self.shapes = {}

        self.program_type = None

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

        self.source_list = []

        self.diff_dict = Dict[str, Any]

    def add_cube(self, transit_source: str):
        """Reads a .lin file and adds it to existing TransitNetwork instance.

        Args:
            transit_source:  a string or the directory of the cube line file to be parsed

        """

        """
        Figure out what kind of transit source it is
        """

        parser = Lark(TRANSIT_LINE_FILE_GRAMMAR, debug="debug", parser="lalr")

        if "NAME=" in transit_source:
            WranglerLogger.debug("reading transit source as string")
            self.source_list.append("input_str")
            parse_tree = parser.parse(transit_source)
        elif os.path.isfile(transit_source):
            print("reading: {}".format(transit_source))
            with open(transit_source) as file:
                WranglerLogger.debug(
                    "reading transit source: {}".format(transit_source)
                )
                self.source_list.append(transit_source)
                parse_tree = parser.parse(file.read())
        elif os.path.isdir(transit_source):
            import glob

            for lin_file in glob.glob(os.path.join(transit_source, "*.LIN")):
                self.add_cube(lin_file)
            return
        else:
            msg = "{} not a valid transit line string, directory, or file"
            WranglerLogger.error(msg)
            raise ValueError(msg)

        WranglerLogger.debug("finished parsing cube line file")
        # WranglerLogger.debug("--Parse Tree--\n {}".format(parse_tree.pretty()))
        transformed_tree_data = CubeTransformer().transform(parse_tree)
        # WranglerLogger.debug("--Transformed Parse Tree--\n {}".format(transformed_tree_data))

        _line_data = transformed_tree_data["lines"]

        line_properties_dict = {k: v["line_properties"] for k, v in _line_data.items()}
        line_shapes_dict = {k: v["line_shape"] for k, v in _line_data.items()}
        new_lines = list(line_properties_dict.keys())
        """
        Before adding lines, check to see if any are overlapping with existing ones in the network
        """

        overlapping_lines = set(new_lines) & set(self.lines)
        if overlapping_lines:
            msg = "Overlapping lines found when adding from {}. \nSource files:\n{}\n{} Overlapping Lines of {} total new lines.\n-->{}".format(
                transit_source,
                "\n - ".join(self.source_list),
                len(new_lines),
                len(overlapping_lines),
                overlapping_lines,
            )
            print(msg)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        self.program_type = transformed_tree_data.get("program_type", None)

        self.lines += new_lines
        self.line_properties.update(line_properties_dict)
        self.shapes.update(line_shapes_dict)

        WranglerLogger.debug("Added lines to CubeTransit: \n".format(new_lines))

    @staticmethod
    def create_from_cube(transit_source: str, parameters: Optional[dict] = {}):
        """
        Reads a cube .lin file and stores as TransitNetwork object.

        Args:
            transit_source:  a string or the directory of the cube line file to be parsed

        Returns:
            A ::CubeTransit object created from the transit_source.
        """

        tn = CubeTransit(parameters)
        tn.add_cube(transit_source)

        return tn

    def evaluate_differences(self, base_transit):
        """
        1. Identifies what routes need to be updated, deleted, or added
        2. For routes being added or updated, identify if the time periods
            have changed or if there are multiples, and make duplicate lines if so
        3. Create project card dictionaries for each change.

        Args:
            base_transit (CubeTransit): an instance of this class for the base condition

        Returns:
            A list of dictionaries containing project card changes
            required to evaluate the differences between the base network
            and this transit network instance.
        """
        transit_change_list = []

        """
        Identify what needs to be evaluated
        """
        lines_to_update = [l for l in self.lines if l in base_transit.lines]
        lines_to_delete = [l for l in base_transit.lines if l not in self.lines]
        lines_to_add = [l for l in self.lines if l not in base_transit.lines]

        project_card_changes = []

        """
        Evaluate Property Updates
        """

        for line in lines_to_update:
            WranglerLogger.debug(
                "Finding differences in time periods for: {}".format(line)
            )

            """
            Find any additional time periods that might need to add or delete.
            """
            base_cube_time_period_numbers = (
                CubeTransit.get_time_period_numbers_from_cube_properties(
                    base_transit.line_properties[line]
                )
            )

            try:
                assert len(base_cube_time_period_numbers) == 1
            except:
                msg = "Base network line {} should only have one time period per route, but {} found".format(
                    line, base_cube_time_period_numbers
                )
                WranglerLogger.error(msg)
                raise ValueError(msg)

            base_cube_time_period_number = base_cube_time_period_numbers[0]

            build_cube_time_period_numbers = (
                CubeTransit.get_time_period_numbers_from_cube_properties(
                    self.line_properties[line]
                )
            )

            time_periods_to_add = [
                tp
                for tp in build_cube_time_period_numbers
                if tp not in base_cube_time_period_numbers
            ]

            for tp in time_periods_to_add:
                lines_to_add.append(self.add_additional_time_periods(tp, line))

            time_periods_to_delete = [
                tp
                for tp in base_cube_time_period_numbers
                if tp not in build_cube_time_period_numbers
            ]

            for tp in time_periods_to_delete:
                lines_to_delete.append(line)

            WranglerLogger.debug("Evaluating differences in: {}".format(line))
            updated_properties = self.evaluate_route_property_differences(
                self.line_properties[line],
                base_transit.line_properties[line],
                base_cube_time_period_number,
            )
            updated_shapes = CubeTransit.evaluate_route_shape_changes(
                self.shapes[line].node, base_transit.shapes[line].node
            )
            if updated_properties:
                update_prop_card_dict = self.create_update_route_card_dict(
                    line, updated_properties
                )
                project_card_changes.append(update_prop_card_dict)

            if updated_shapes:
                update_shape_card_dict = self.create_update_route_card_dict(
                    line, updated_shapes
                )
                project_card_changes.append(update_shape_card_dict)

        """
        Evaluate Deletions
        """
        for line in lines_to_delete:
            delete_card_dict = self.create_delete_route_card_dict(
                line, base_transit.line_properties[line]
            )
            project_card_changes.append(delete_card_dict)

        """
        Evaluate Additions

        First assess if need to add multiple routes if there are multiple time periods
        """
        for line in lines_to_add:
            time_period_numbers = (
                CubeTransit.get_time_period_numbers_from_cube_properties(
                    self.line_properties[line]
                )
            )
            if len(time_period_numbers) > 1:
                for tp in time_period_numbers[1:]:
                    lines_to_add.append(self.add_additional_time_periods(tp, line))

        for line in lines_to_add:
            add_card_dict = self.create_add_route_card_dict(line)
            project_card_changes.append(add_card_dict)

        return project_card_changes

    def add_additional_time_periods(
        self, new_time_period_number: int, orig_line_name: str
    ):
        """
        Copies a route to another cube time period with appropriate
        values for time-period-specific properties.

        New properties are stored under the new name in:
         - ::self.shapes
         - ::self.line_properties

        Args:
            new_time_period_number (int): cube time period number
            orig_line_name(str): name of the originating line, from which
                the new line will copy its properties.

        Returns:
            Line name with new time period.
        """
        WranglerLogger.debug(
            "adding time periods {} to line {}".format(
                new_time_period_number, orig_line_name
            )
        )

        (
            route_id,
            _init_time_period,
            agency_id,
            direction_id,
        ) = CubeTransit.unpack_route_name(orig_line_name)
        new_time_period_name = self.parameters.cube_time_periods[new_time_period_number]
        new_tp_line_name = CubeTransit.build_route_name(
            route_id=route_id,
            time_period=new_time_period_name,
            agency_id=agency_id,
            direction_id=direction_id,
        )

        try:
            assert new_tp_line_name not in self.lines
        except:
            msg = "Trying to add a new time period {} to line {}, but constructed name {} is already in  line list.".format(
                new_time_period_number, orig_line_name, new_tp_line_name
            )
            WrangerLogger.error(msg)
            raise ValueError(msg)

        # copy to a new line and add it to list of lines to add
        self.line_properties[new_tp_line_name] = copy.deepcopy(
            self.line_properties[orig_line_name]
        )
        self.shapes[new_tp_line_name] = copy.deepcopy(self.shapes[orig_line_name])
        self.line_properties[new_tp_line_name]["NAME"] = new_tp_line_name

        """
        Remove entries that aren't for this time period from the new line's properties list.
        """
        this_time_period_properties_list = [
            p + "[" + str(new_time_period_number) + "]"
            ##todo parameterize all time period specific variables
            for p in ["HEADWAY", "FREQ"]
        ]

        not_this_tp_properties_list = list(
            set(self.parameters.time_period_properties_list)
            - set(this_time_period_properties_list)
        )

        for k in not_this_tp_properties_list:
            self.line_properties[new_tp_line_name].pop(k, None)

        """
        Remove entries for time period from the original line's properties list.
        """
        for k in this_time_period_properties_list:
            self.line_properties[orig_line_name].pop(k, None)

        """
        Add new line to list of lines to add.
        """
        WranglerLogger.debug(
            "Adding new time period {} for line {} as {}.".format(
                new_time_period_number, orig_line_name, new_tp_line_name
            )
        )
        return new_tp_line_name

    def create_update_route_card_dict(self, line: str, updated_properties_dict: dict):
        """
        Creates a project card change formatted dictionary for updating
        the line.

        Args:
            line: name of line that is being updated
            updated_properties_dict: dictionary of attributes to update as
                'property': <property name>,
                'set': <new property value>

        Returns:
            A project card change-formatted dictionary for the attribute update.
        """
        base_start_time_str, base_end_time_str = self.calculate_start_end_times(
            self.line_properties[line]
        )

        update_card_dict = {
            "category": "Transit Service Property Change",
            "facility": {
                "route_id": line.split("_")[1],
                "direction_id": int(line.split("_")[-2].strip("d\"")),
                "shape_id": line.split("_")[-1].strip("s\""),
                "start_time": base_start_time_str,
                "end_time": base_end_time_str,
            },
            "properties": updated_properties_dict,
        }
        WranglerLogger.debug(
            "Updating {} route to changes:\n{}".format(line, str(update_card_dict))
        )

        return update_card_dict

    def create_delete_route_card_dict(
        self, line: str, base_transit_line_properties_dict: dict
    ):
        """
        Creates a project card change formatted dictionary for deleting a line.

        Args:
            line: name of line that is being deleted
            base_transit_line_properties_dict: dictionary of cube-style
                attribute values in order to find time periods and
                start and end times.

        Returns:
            A project card change-formatted dictionary for the route deletion.
        """
        base_start_time_str, base_end_time_str = self.calculate_start_end_times(
            base_transit_line_properties_dict
        )

        delete_card_dict = {
            "category": "Delete Transit Service",
            "facility": {
                "route_id": line.split("_")[1],
                "direction_id": int(line.strip('"')[-1]),
                "start_time": base_start_time_str,
                "end_time": base_end_time_str,
            },
        }
        WranglerLogger.debug(
            "Deleting {} route to changes:\n{}".format(line, delete_card_dict)
        )

        return delete_card_dict

    def create_add_route_card_dict(self, line: str):
        """
        Creates a project card change formatted dictionary for adding
        a route based on the information in self.route_properties for
        the line.

        Args:
            line: name of line that is being updated

        Returns:
            A project card change-formatted dictionary for the route addition.
        """
        start_time_str, end_time_str = self.calculate_start_end_times(
            self.line_properties[line]
        )

        standard_properties = self.cube_properties_to_standard_properties(
            self.line_properties[line]
        )

        routing_properties = {
            "property": "routing",
            "set": self.shapes[line]["node"].tolist(),
        }

        add_card_dict = {
            "category": "New Transit Service",
            "facility": {
                "route_id": line.split("_")[1],
                "direction_id": int(line.strip('_')[-2]),
                "start_time": start_time_str,
                "end_time": end_time_str,
                "agency_id": line.strip('_')[0],
            },
            "properties": standard_properties + [routing_properties],
        }

        WranglerLogger.debug(
            "Adding {} route to changes:\n{}".format(line, add_card_dict)
        )
        return add_card_dict

    @staticmethod
    def get_time_period_numbers_from_cube_properties(properties_list: list):
        """
        Finds properties that are associated with time periods and the
        returns the numbers in them.

        Args:
            properties_list (list): list of all properties.

        Returns:
            list of strings of the time period numbers found
        """
        time_periods_list = []
        for p in properties_list:
            if ("[" not in p) or ("]" not in p):
                continue
            tp_num = p.split("[")[1][0]
            if tp_num and tp_num not in time_periods_list:
                time_periods_list.append(tp_num)
        return time_periods_list

    @staticmethod
    def build_route_name(
        route_id: str = "",
        time_period: str = "",
        agency_id: str = 0,
        direction_id: str = 1,
    ):
        """
        Create a route name by contatenating route, time period, agency, and direction

        Args:
            route_id: i.e. 452-111
            time_period: i.e. pk
            direction_id: i.e. 1
            agency_id: i.e. 0

        Returns:
            constructed line_name i.e. "0_452-111_452_pk1"
        """

        return (
            str(agency_id)
            + "_"
            + str(route_id)
            + "_"
            + str(route_id.split("-")[0])
            + "_"
            + str(time_period)
            + str(direction_id)
        )

    @staticmethod
    def unpack_route_name(line_name: str):
        """
        Unpacks route name into direction, route, agency, and time period info

        Args:
            line_name (str): i.e. "0_452-111_452_pk1"

        Returns:
            route_id (str): 452-111
            time_period (str): i.e. pk
            direction_id (str) : i.e. 1
            agency_id (str) : i.e. 0
        """

        line_name = line_name.strip('"')

        agency_id, route_id, _rtid, _tp_direction = line_name.split("_")
        time_period = _tp_direction[0:-1]
        direction_id = _tp_direction[-1]

        return route_id, time_period, agency_id, direction_id

    def calculate_start_end_times(self, line_properties_dict: dict):
        """
        Calculate the start and end times of the property change
        WARNING: Doesn't take care of discongruous time periods!!!!

        Args:
            line_properties_dict: dictionary of cube-flavor properties for a transit line
        """
        start_time_m = 24 * 60
        end_time_m = 0 * 60

        WranglerLogger.debug(
            "parameters.time_period_properties_list: {}".format(
                self.parameters.time_period_properties_list
            )
        )
        current_cube_time_period_numbers = (
            CubeTransit.get_time_period_numbers_from_cube_properties(
                line_properties_dict
            )
        )

        WranglerLogger.debug(
            "current_cube_time_period_numbers:{}".format(
                current_cube_time_period_numbers
            )
        )

        for tp in current_cube_time_period_numbers:
            time_period_name = self.parameters.cube_time_periods[tp]
            WranglerLogger.debug("time_period_name:{}".format(time_period_name))
            _start_time, _end_time = self.parameters.time_period_to_time[
                time_period_name
            ]

            # change from "HH:MM" to integer # of seconds
            _start_time_m = (int(_start_time.split(":")[0]) * 60) + int(
                _start_time.split(":")[1]
            )
            _end_time_m = (int(_end_time.split(":")[0]) * 60) + int(
                _end_time.split(":")[1]
            )

            # find bounding start and end times
            if _start_time_m < start_time_m:
                start_time_m = _start_time_m
            if _end_time_m > end_time_m:
                end_time_m = _end_time_m

        if start_time_m > end_time_m:
            msg = "Start time ({}) is after end time ({})".format(
                start_time_m, end_time_m
            )
            #WranglerLogger.error(msg)
            #raise ValueError(msg)

        start_time_str = "{:02d}:{:02d}".format(*divmod(start_time_m, 60))
        end_time_str = "{:02d}:{:02d}".format(*divmod(end_time_m, 60))
        return start_time_str, end_time_str

    @staticmethod
    def cube_properties_to_standard_properties(cube_properties_dict: dict):
        """
        Converts cube style properties to standard properties.

        This is most pertinent to time-period specific variables like headway,
        and varibles that have stnadard units like headway, which is minutes
        in cube and seconds in standard format.

        Args:
            cube_properties_dict: <cube style property name> : <property value>

        Returns:
            A list of dictionaries with values for `"property": <standard
                style property name>, "set" : <property value with correct units>`

        """
        standard_properties_list = []
        for k, v in cube_properties_dict.items():
            change_item = {}
            if any(i in k for i in ["HEADWAY", "FREQ"]):
                change_item["property"] = "headway_secs"
                change_item["set"] = v * 60
            else:
                change_item["property"] = k
                change_item["set"] = v
            standard_properties_list.append(change_item)

        return standard_properties_list

    def evaluate_route_property_differences(
        self,
        properties_build: dict,
        properties_base: dict,
        time_period_number: str,
        absolute: bool = True,
        validate_base: bool = False,
    ):
        """
        Checks if any values have been updated or added for a specific
        route and creates project card entries for each.

        Args:
            properties_build: ::<property_name>: <property_value>
            properties_base: ::<property_name>: <property_value>
            time_period_number: time period to evaluate
            absolute: if True, will use `set` command rather than a change.  If false, will automatically check the base value.  Note that this only applies to the numeric values of frequency/headway
            validate_base: if True, will add the `existing` line in the project card

        Returns:
            transit_change_list (list): a list of dictionary values suitable for writing to a project card
                `{
                'property': <property_name>,
                'set': <set value>,
                'change': <change from existing value>,
                'existing': <existing value to check>,
                }`

        """

        # Remove time period specific values for things that aren't part of the time period in question
        this_time_period_properties_list = [
            p + "[" + str(time_period_number) + "]"
            ##todo parameterize all time period specific variables
            for p in ["HEADWAY", "FREQ"]
        ]

        not_this_tp_properties_list = list(
            set(self.parameters.time_period_properties_list)
            - set(this_time_period_properties_list)
        )

        for k in not_this_tp_properties_list:
            properties_build.pop(k, None)
            properties_base.pop(k, None)

        difference_dict = dict(
            set(properties_build.items()) ^ set(properties_base.items())
        )

        # Iterate through properties list to build difference project card list

        properties_list = []
        for k, v in difference_dict.items():
            change_item = {}
            if any(i in k for i in ["HEADWAY", "FREQ"]):
                change_item["property"] = "headway_secs"

                if absolute:
                    change_item["set"] = (
                        v * 60
                    )  # project cards are in secs, cube is in minutes
                else:
                    change_item["change"] = (
                        properties_build[k] - properties_base[k]
                    ) * 60
                if validate_base or not absolute:
                    change_item["existing"] = properties_base[k] * 60
            else:
                change_item["property"] = k
                change_item["set"] = v
                if validate_base:
                    change_item["existing"] = properties_base[k]

            properties_list.append(change_item)
        WranglerLogger.debug(
            "Evaluated Route Changes: \n {})".format(
                "\n".join(map(str, properties_list))
            )
        )
        return properties_list

    @staticmethod
    def evaluate_route_shape_changes(
        shape_build: DataFrame, shape_base: DataFrame
    ):
        """
        Compares two route shapes and constructs returns list of changes
        suitable for a project card.

        Args:
            shape_build: DataFrame of the build-version of the route shape.
            shape_base: dDataFrame of the base-version of the route shape.

        Returns:
            List of shape changes formatted as a project card-change dictionary.

        """

        if shape_build.equals(shape_base):
            return None

        shape_change_list = []

        base_node_list = shape_base.tolist()
        build_node_list = shape_build.tolist()

        sort_len = max(len(base_node_list), len(build_node_list))

        start_pos = None
        end_pos = None
        for i in range(sort_len):
            if (i == len(base_node_list)) | (i == len(build_node_list)):
                start_pos = i - 1
                break
            if base_node_list[i] != build_node_list[i]:
                start_pos = i
                break
            else:
                continue

        j = -1
        for i in range(sort_len):
            if (i == len(base_node_list)) | (i == len(build_node_list)):
                end_pos = j + 1
                break
            if base_node_list[j] != build_node_list[j]:
                end_pos = j
                break
            else:
                j -= 1

        if start_pos or end_pos:
            existing = base_node_list[
                (start_pos - 2 if start_pos > 1 else None) : (
                    end_pos + 2 if end_pos < -2 else None
                )
            ]
            set = build_node_list[
                (start_pos - 2 if start_pos > 1 else None) : (
                    end_pos + 2 if end_pos < -2 else None
                )
            ]

            shape_change_list.append(
                {"property": "routing", "existing": existing, "set": set}
            )

        return shape_change_list


class StandardTransit(object):
    """Holds a standard transit feed as a Partridge object and contains
    methods to manipulate and translate the GTFS data to MetCouncil's
    Cube Line files.

    .. highlight:: python
    Typical usage example:
    ::
        cube_transit_net = StandardTransit.read_gtfs(BASE_TRANSIT_DIR)
        cube_transit_net.write_as_cube_lin(os.path.join(WRITE_DIR, "outfile.lin"))

    Attributes:
        feed: Partridge Feed object containing read-only access to GTFS feed
        parameters (Parameters): Parameters instance containing information
            about time periods and variables.
    """

    def __init__(self, ptg_feed, parameters: Union[Parameters, dict] = {}):
        """

        Args:
            ptg_feed: partridge feed object
            parameters: dictionary of parameter settings (see Parameters class) or an instance of Parameters
        """
        self.feed = ptg_feed

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

    @staticmethod
    def fromTransitNetwork(
        transit_network_object: TransitNetwork, parameters: Union[Parameters, dict] = {}
    ):
        """
        RoadwayNetwork to ModelRoadwayNetwork

        Args:
            transit_network_object: Reference to an instance of TransitNetwork.
            parameters: dictionary of parameter settings (see Parameters class) or an instance of Parameters. If not provided will
                use default parameters.

        Returns:
            StandardTransit
        """
        return StandardTransit(transit_network_object.feed, parameters=parameters)

    @staticmethod
    def read_gtfs(gtfs_feed_dir: str, parameters: Union[Parameters, dict] = {}):
        """
        Reads GTFS files from a directory and returns a StandardTransit
        instance.

        Args:
            gtfs_feed_dir: location of the GTFS files
            parameters: dictionary of parameter settings (see Parameters class) or an instance of Parameters. If not provided will
                use default parameters.

        Returns:
            StandardTransit instance
        """
        return StandardTransit(ptg.load_feed(gtfs_feed_dir), parameters=parameters)

    def write_as_cube_lin(self, outpath: str = None):
        """
        Writes the gtfs feed as a cube line file after
        converting gtfs properties to MetCouncil cube properties.
        #MC
        Args:
            outpath: File location for output cube line file.

        """
        if not outpath:
            outpath = os.path.join(self.parameters.scratch_location, "outtransit.lin")
        trip_cube_df = self.route_properties_gtfs_to_cube(self)

        trip_cube_df["LIN"] = trip_cube_df.apply(self.cube_format, axis=1)

        l = trip_cube_df["LIN"].tolist()

        with open(outpath, "w") as f:
            f.write("\n".join(l))

    @staticmethod
    def route_properties_gtfs_to_cube(self):
        """
        Prepare gtfs for cube lin file.
        #MC
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
            "Converting GTFS Standard Properties to MetCouncil's Cube Standard"
        )
        metro_operator_dict = {
            "0": 3,
            "1": 3,
            "2": 3,
            "3": 4,
            "4": 2,
            "5": 5,
            "6": 8,
            "7": 1,
            "8": 1,
            "9": 10,
            "10": 3,
            "11": 9,
            "12": 3,
            "13": 4,
            "14": 4,
            "15": 3,
        }

        shape_df = self.feed.shapes.copy()
        trip_df = self.feed.trips.copy()

        """
        Add information from: routes, frequencies, and routetype to trips_df
        """
        trip_df = pd.merge(trip_df, self.feed.routes, how="left", on="route_id")
        trip_df = pd.merge(trip_df, self.feed.frequencies, how="left", on="trip_id")

        trip_df["tod_name"] = trip_df.start_time.apply(self.time_to_cube_time_period)
        inv_cube_time_periods_map = {
            v: k for k, v in self.parameters.cube_time_periods.items()
        }
        trip_df["tod_num"] = trip_df.tod_name.map(inv_cube_time_periods_map)
        trip_df["tod_name"] = trip_df.tod_name.map(
            self.parameters.cube_time_periods_name
        )

        trip_df["NAME"] = trip_df.apply(
            lambda x: x.agency_id
            + "_"
            + x.route_id
            + "_"
            + x.route_short_name
            + "_"
            + x.tod_name
            + str(x.direction_id),
            axis=1,
        )

        trip_df["LONGNAME"] = trip_df["route_long_name"]
        trip_df["HEADWAY"] = (trip_df["headway_secs"] / 60).astype(int)
        trip_df["MODE"] = trip_df.apply(self.calculate_cube_mode, axis=1)
        trip_df["ONEWAY"] = "T"
        trip_df["OPERATOR"] = trip_df["agency_id"].map(metro_operator_dict)

        return trip_df

    def calculate_cube_mode(self, row):
        """
        Assigns a cube mode number by following logic.
        #MC
        For rail, uses GTFS route_type variable:
        https://developers.google.com/transit/gtfs/reference

        ::
            #             route_type : cube_mode
            route_type_to_cube_mode = {0: 8, # Tram, Streetcar, Light rail
                                       3: 0, # Bus; further disaggregated for cube
                                       2: 9} # Rail

        For buses, uses route id numbers and route name to find
        express and suburban buses  as follows:

        ::
            if not cube_mode:
                if 'express' in row['LONGNAME'].lower():
                    cube_mode = 7  # Express
                elif int(row['route_id'].split("-")[0]) > 99:
                    cube_mode = 6  # Suburban Local
                else:
                    cube_mode = 5  # Urban Local

        Args:
            row: A DataFrame row with route_type, route_long_name, and route_id

        Returns:
            cube mode number
        """
        #                 route_type : cube_mode
        route_type_to_cube_mode = {
            0: 8,  # Tram, Streetcar, Light rail
            3: 0,  # Bus; further disaggregated for cube
            2: 9,
        }  # Rail

        cube_mode = route_type_to_cube_mode[row["route_type"]]

        if not cube_mode:
            if "express" in row["route_long_name"].lower():
                cube_mode = 7  # Express
            elif int(row["route_id"].split("-")[0]) > 99:
                cube_mode = 6  # Suburban Local
            else:
                cube_mode = 5  # Urban Local

        return cube_mode

    def time_to_cube_time_period(
        self, start_time_secs: int, as_str: bool = True, verbose: bool = False
    ):
        """
        Converts seconds from midnight to the cube time period.

        Args:
            start_time_secs: start time for transit trip in seconds
                from midnight
            as_str: if True, returns the time period as a string,
                otherwise returns a numeric time period

        Returns:
            this_tp_num: if as_str is False, returns the numeric
                time period
            this_tp: if as_str is True, returns the Cube time period
                name abbreviation
        """
        from .util import hhmmss_to_datetime, secs_to_datetime

        # set initial time as the time that spans midnight

        start_time_dt = secs_to_datetime(start_time_secs)

        # set initial time as the time that spans midnight
        this_tp = "NA"
        for tp_name, _times in self.parameters.time_period_to_time.items():
            _start_time, _end_time = _times
            _dt_start_time = hhmmss_to_datetime(_start_time)
            _dt_end_time = hhmmss_to_datetime(_end_time)
            if _dt_start_time > _dt_end_time:
                this_tp = tp_name
                break

        for tp_name, _times in self.parameters.time_period_to_time.items():
            _start_time, _end_time = _times
            _dt_start_time = hhmmss_to_datetime(_start_time)
            if start_time_dt >= _dt_start_time:
                this_time = _dt_start_time
                this_tp = tp_name

        if verbose:
            WranglerLogger.debug(
                "Finding Cube Time Period from Start Time: \
                \n  - start_time_sec: {} \
                \n  - start_time_dt: {} \
                \n  - this_tp: {}".format(
                    start_time_secs, start_time_dt, this_tp
                )
            )

        if as_str:
            return this_tp

        name_to_num = {v: k for k, v in self.parameters.cube_time_periods.items()}
        this_tp_num = name_to_num.get(this_tp)

        if not this_tp_num:
            msg = (
                "Cannot find time period number in {} for time period name: {}".format(
                    name_to_num, this_tp
                )
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        return this_tp_num

    def shape_gtfs_to_cube(self, row, add_nntime = False):
        """
        Creates a list of nodes that for the route in appropriate
        cube format.

        Args:
            row: DataFrame row with both shape_id and trip_id

        Returns: a string representation of the node list
            for a route in cube format.

        """
        agency_raw_name = row.agency_raw_name
        shape_id = row.shape_id
        trip_id = row.trip_id

        trip_stop_times_df = self.feed.stop_times.copy()

        if 'agency_raw_name' in trip_stop_times_df.columns:
            trip_stop_times_df.drop('agency_raw_name', axis = 1, inplace = True)

        trip_stop_times_df = pd.merge(
            trip_stop_times_df,
            self.feed.trips[['trip_id', 'agency_raw_name']],
            how = 'left',
            on = ['trip_id']
        )

        trip_stop_times_df = trip_stop_times_df[
            (trip_stop_times_df.trip_id == row.trip_id) &
            (trip_stop_times_df.agency_raw_name == agency_raw_name)
        ]

        trip_node_df = self.feed.shapes.copy()
        if 'agency_raw_name' in trip_node_df.columns:
            trip_node_df.drop('agency_raw_name', axis = 1, inplace = True)

        trip_node_df = pd.merge(
            trip_node_df,
            self.feed.trips[['shape_id', 'agency_raw_name']].drop_duplicates(),
            how = 'left',
            on = ['shape_id']
        )

        trip_node_df = trip_node_df[
            (trip_node_df.shape_id == shape_id) &
            (trip_node_df.agency_raw_name == agency_raw_name)
        ]

        trip_node_df.sort_values(by = ["shape_pt_sequence"], inplace = True)

        stops_df = self.feed.stops.copy()
        stops_df['stop_id'] = stops_df['stop_id'].astype(float).astype(int)
        trip_stop_times_df['stop_id'] = trip_stop_times_df['stop_id'].astype(float).astype(int)

        if 'trip_id' in self.feed.stops.columns:
            if agency_raw_name != 'sjrtd_2015_0127':
                stops_df = stops_df[stops_df.agency_raw_name != 'sjrtd_2015_0127']
                trip_stop_times_df = pd.merge(
                    trip_stop_times_df, stops_df.drop('trip_id', axis = 1), how="left", on=["stop_id"]
                )
            else:
                stops_df = stops_df[stops_df.agency_raw_name == 'sjrtd_2015_0127']
                stops_df['trip_id'] = stops_df['trip_id'].astype(float).astype(int).astype(str)
                trip_stop_times_df['trip_id'] = trip_stop_times_df['trip_id'].astype(float).astype(int).astype(str)
                trip_stop_times_df = pd.merge(
                    trip_stop_times_df, stops_df, how="left", on=['agency_raw_name', 'trip_id',"stop_id"]
                )
        else:
            trip_stop_times_df = pd.merge(
                trip_stop_times_df, stops_df, how="left", on="stop_id"
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

        # ACCESS
        def _access_type(x):
            if (x.pickup_type in [1, "1"]):
                return 2
            elif (x.drop_off_type in [1, "1"]):
                return 1
            else:
                return 0

        trip_stop_times_df["ACCESS"] = trip_stop_times_df.apply(lambda x: _access_type(x), axis = 1)

        trip_runtime = round(trip_stop_times_df[trip_stop_times_df['NNTIME'] > 0]['NNTIME'].sum(),2)

        # node list
        node_list_str = ""
        stop_seq = 0
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

                    if nntime_v > 0:
                        nntime = ", NNTIME=%s" % (nntime_v)
                    else:
                        nntime = ""
                else:
                    nntime = ""

                access_v = trip_stop_times_df.loc[
                    (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]),"ACCESS"].iloc[0]
                if access_v > 0:
                    access = ", ACCESS=%s" % (access_v)
                else:
                    access = ""

                node_list_str += "\n %s%s%s" % (trip_node_list[nodeIdx], nntime, access)
                if nodeIdx < (len(trip_node_list) - 1):
                    node_list_str += ","
                    if ((add_nntime) & (stop_seq > 1) & (len(nntime) > 0)) | (len(access) > 0):
                        node_list_str += " N="
            else:
                node_list_str += "\n -%s" % (trip_node_list[nodeIdx])
                if nodeIdx < (len(trip_node_list) - 1):
                    node_list_str += ","

        # remove NNTIME = 0
        node_list_str = node_list_str.replace(" NNTIME=0.0, N=", "")
        node_list_str = node_list_str.replace(" NNTIME=0.0,", "")

        return node_list_str, trip_runtime


    def cube_format(self, row):
        """
        Creates a string represnting the route in cube line file notation.
        #MC
        Args:
            row: row of a DataFrame representing a cube-formatted trip, with the Attributes
                trip_id, shape_id, NAME, LONGNAME, tod, HEADWAY, MODE, ONEWAY, OPERATOR

        Returns:
            string representation of route in cube line file notation
        """

        s = '\nLINE NAME="{}",'.format(row.NAME)
        s += '\n LONGNAME="{}",'.format(row.LONGNAME)
        s += "\n HEADWAY[{}]={},".format(row.tod_num, row.HEADWAY)
        s += "\n MODE={},".format(row.MODE)
        s += "\n ONEWAY={},".format(row.ONEWAY)
        s += "\n OPERATOR={},".format(row.OPERATOR)
        s += "\n NODES={}".format(self.shape_gtfs_to_cube(row))

        return s

    def shape_gtfs_to_emme(self, trip_row):
        """
        Creates transit segment for the trips in appropriate
        emme format.

        Args:
            row: DataFrame row with both shape_id and trip_id

        Returns: a dataframe representation of the transit segment
            for a trip in emme format.

        """
        trip_stop_times_df = self.feed.stop_times.copy()
        trip_stop_times_df = trip_stop_times_df[
            trip_stop_times_df.trip_id == trip_row.trip_id
        ]

        trip_node_df = self.feed.shapes.copy()
        trip_node_df = trip_node_df[trip_node_df.shape_id == trip_row.shape_id]
        trip_node_df.sort_values(by = ["shape_pt_sequence"], inplace = True)

        trip_stop_times_df = pd.merge(
            trip_stop_times_df, self.feed.stops, how="left", on="stop_id"
        )

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
        trip_node_df['node_id'] = trip_node_df['shape_model_node_id'].astype(int)
        trip_node_df['stop_order'] = trip_node_df['shape_pt_sequence']

        return trip_node_df

    def evaluate_differences(self, transit_changes):
        """
        Compare changes from the transit_changes dataframe with the standard transit network
        returns the project card changes in dictionary format
        """

        # simple properties change
        trip_df = self.feed.trips.copy()

        mode_crosswalk = pd.read_csv(self.parameters.mode_crosswalk_file)
        mode_crosswalk.drop_duplicates(subset = ["agency_raw_name", "route_type", "is_express_bus"], inplace = True)

        trip_df = pd.merge(trip_df, self.feed.routes.drop("agency_raw_name", axis = 1), how="left", on="route_id")

        trip_df = pd.merge(trip_df, self.feed.frequencies, how="left", on="trip_id")

        trip_df["tod"] = trip_df.start_time.apply(self.time_to_cube_time_period, as_str = False)
        trip_df["tod_name"] = trip_df.start_time.apply(self.time_to_cube_time_period)

        trip_df["headway_minutes"] = (trip_df["headway_secs"] / 60).astype(int)

        trip_df = pd.merge(trip_df, self.feed.agency[["agency_name", "agency_raw_name", "agency_id"]], how = "left", on = ["agency_raw_name", "agency_id"])

        # identify express bus
        from .mtc import _is_express_bus
        trip_df["is_express_bus"] = trip_df.apply(lambda x: _is_express_bus(x), axis = 1)
        trip_df.drop("agency_name", axis = 1 , inplace = True)

        trip_df = pd.merge(
            trip_df,
            mode_crosswalk.drop("agency_id", axis = 1),
            how = "left",
            on = ["agency_raw_name", "route_type", "is_express_bus"]
        )

        trip_df["line_id"] = trip_df.apply(
            lambda x: str(x.TM2_operator)
            + "_"
            + str(x.route_id)
            + "_"
            + x.tod_name
            + "_"
            + "d"
            + str(int(x.direction_id))
            + "_s"
            + x.shape_id,
            axis=1,
        )

        trip_df["line_id"] = trip_df["line_id"].str.slice(stop = 28)

        project_card_changes = []

        # lines updated
        transit_changes['line_id'] = transit_changes.apply(
            lambda x: '-'.join(x['element_id'].split('-')[:-3]) if
            x['object'] == 'TRANSIT_STOP' else
            x['element_id'],
            axis = 1
        )

        lines_updated_df = transit_changes[
            (transit_changes['operation'] == 'C') &
            (transit_changes['line_id'].isin(trip_df['line_id'].tolist()))
        ].copy()

        #########################
        # simple property changes
        #########################

        property_changes_df = lines_updated_df[
            lines_updated_df.object == 'TRANSIT_LINE'
        ].copy()

        property_attribute_list = ['headway_secs']

        for index, row in property_changes_df.iterrows():
            line_id = row['line_id']
            properties_list = []
            change_item = {}
            for c in property_attribute_list:
                existing_value = int(trip_df[
                    trip_df['line_id'] == line_id
                ][c].iloc[0])

                change_item["existing"] = existing_value

                if c == 'headway_secs':
                    change_item["set"] = row['headway'] * 60
                else:
                    change_item["set"] = row[c]

                change_item["property"] = c

                properties_list.append(change_item)

            property_changes_df.loc[index, 'properties'] = properties_list

        ###############
        # shape changes
        ###############

        shape_changes_df = lines_updated_df[
            lines_updated_df.object.isin(['TRANSIT_SHAPE'])
        ].copy()

        for index, row in shape_changes_df.iterrows():
            line_id = row.line_id

            # get base shape
            trip_row = trip_df[trip_df.line_id == line_id].copy().squeeze()

            base_shape = self.shape_gtfs_to_emme(
                trip_row=trip_row
            )
            base_shape['shape_model_node_id'] = base_shape['shape_model_node_id'].astype(int)

            # get build shape
            build_shape = row.new_itinerary

            updated_shapes = CubeTransit.evaluate_route_shape_changes(
                shape_base = base_shape.shape_model_node_id,
                shape_build = pd.Series(row.new_itinerary)
            )
            updated_shapes[0]['property'] = 'shapes'
            shape_changes_df.loc[index, 'properties'] = updated_shapes

        ##############
        # stop changes
        ##############
        stop_changes_df = lines_updated_df[
            lines_updated_df.object.isin(['TRANSIT_STOP'])
        ].copy()

        stop_attribute_list = ['allow_alightings', 'allow_boardings']

        stop_changes_df = stop_changes_df.groupby(
            ['line_id','i_node']
        )[stop_attribute_list].last().reset_index()
        
        stop_attribute_changes_df = pd.DataFrame()

        for attribute in stop_attribute_list:

            attribute_df = stop_changes_df.groupby(
                ['line_id', attribute]
            )['i_node'].apply(list).reset_index()
            attribute_df['properties'] = attribute_df.apply(
                lambda x: {
                    'property' : attribute if x[attribute] == True else 'no_'+attribute.split('_')[-1],
                    'set': x['i_node']},
                axis = 1
            )
            
            stop_attribute_changes_df = pd.concat(
                [stop_attribute_changes_df, 
                attribute_df[['line_id', 'properties']]],
                sort = False,
                ignore_index = True
            )
        
        ##############
        # combine all transit changes
        ##############
        transit_changes_df = pd.concat(
            [
                property_changes_df,
                shape_changes_df,
                stop_attribute_changes_df
            ],
            sort = False,
            ignore_index = True
        )

        # groupby line_id
        transit_changes_df = transit_changes_df.groupby(
            ['line_id']
        )['properties'].apply(list).reset_index()

        # create change items by line_id
        for index, row in transit_changes_df.iterrows():
            line_id = row['line_id']
            base_start_time_str = self.parameters.time_period_to_time.get(
                line_id.split("_")[2]
            )[0]

            base_end_time_str = self.parameters.time_period_to_time.get(
                line_id.split("_")[2]
            )[1]

            update_card_dict = {
                "category": "Transit Service Property Change",
                "facility": {
                    "route_id": line_id.split("_")[1],
                    "direction_id": int(line_id.split("_")[-2].strip("d\"")),
                    "shape_id": line_id.split("_")[-1].strip("s\""),
                    "start_time": base_start_time_str,
                    "end_time": base_end_time_str
                },
                "properties": row['properties'],
            }
            
            project_card_changes.append(update_card_dict)

        return project_card_changes

class CubeTransformer(Transformer):
    """A lark-parsing Transformer which transforms the parse-tree to
    a dictionary.

    .. highlight:: python
    Typical usage example:
    ::
        transformed_tree_data = CubeTransformer().transform(parse_tree)

    Attributes:
        line_order (int): a dynamic counter to hold the order of the nodes within
            a route shape
        lines_list (list): a list of the line names
    """

    def __init__(self):
        self.line_order = 0
        self.lines_list = []

    def lines(self, line):
        # WranglerLogger.debug("lines: \n {}".format(line))

        # This MUST be a tuple because it returns to start in the tree
        lines = {k: v for k, v in line}
        return ("lines", lines)

    @v_args(inline=True)
    def program_type_line(self, PROGRAM_TYPE, whitespace=None):
        # WranglerLogger.debug("program_type_line:{}".format(PROGRAM_TYPE))
        self.program_type = PROGRAM_TYPE.value

        # This MUST be a tuple because it returns to start  in the tree
        return ("program_type", PROGRAM_TYPE.value)

    @v_args(inline=True)
    def line(self, lin_attributes, nodes):
        # WranglerLogger.debug("line...attributes:\n  {}".format(lin_attributes))
        # WranglerLogger.debug("line...nodes:\n  {}".format(nodes))
        lin_name = lin_attributes["NAME"]

        self.line_order = 0
        # WranglerLogger.debug("parsing: {}".format(lin_name))

        return (lin_name, {"line_properties": lin_attributes, "line_shape": nodes})

    @v_args(inline=True)
    def lin_attributes(self, *lin_attr):
        lin_attr = {k: v for (k, v) in lin_attr}
        # WranglerLogger.debug("lin_attributes:  {}".format(lin_attr))
        return lin_attr

    @v_args(inline=True)
    def lin_attr(self, lin_attr_name, attr_value, SEMICOLON_COMMENT=None):
        # WranglerLogger.debug("lin_attr {}:  {}".format(lin_attr_name, attr_value))
        return lin_attr_name, attr_value

    def lin_attr_name(self, args):
        attr_name = args[0].value.upper()
        # WranglerLogger.debug(".......args {}".format(args))
        if attr_name in ["FREQ", "HEADWAY"]:
            attr_name = attr_name + "[" + str(args[2]) + "]"
        return attr_name

    def attr_value(self, attr_value):
        try:
            return int(attr_value[0].value)
        except:
            return attr_value[0].value

    def nodes(self, lin_node):
        lin_node = DataFrame(lin_node)
        # WranglerLogger.debug("nodes:\n {}".format(lin_node))

        return lin_node

    @v_args(inline=True)
    def lin_node(self, NODE_NUM, SEMICOLON_COMMENT=None, *lin_nodeattr):
        self.line_order += 1
        n = int(NODE_NUM.value)
        return {"node_id": abs(n), "node": n, "stop": n > 0, "order": self.line_order}

    start = dict


TRANSIT_LINE_FILE_GRAMMAR = r"""

start             : program_type_line? lines
WHITESPACE        : /[ \t\r\n]/+
STRING            : /("(?!"").*?(?<!\\)(\\\\)*?"|'(?!'').*?(?<!\\)(\\\\)*?')/i
SEMICOLON_COMMENT : /;[^\n]*/
BOOLEAN           : "T"i | "F"i
program_type_line : ";;<<" PROGRAM_TYPE ">><<LINE>>;;" WHITESPACE?
PROGRAM_TYPE      : "PT" | "TRNBUILD"

lines             : line*
line              : "LINE" lin_attributes nodes

lin_attributes    : lin_attr+
lin_attr          : lin_attr_name "=" attr_value "," SEMICOLON_COMMENT*
TIME_PERIOD       : "1".."5"
!lin_attr_name     : "allstops"i
                    | "color"i
                    | ("freq"i "[" TIME_PERIOD "]")
                    | ("headway"i "[" TIME_PERIOD "]")
                    | "mode"i
                    | "name"i
                    | "oneway"i
                    | "owner"i
                    | "runtime"i
                    | "timefac"i
                    | "xyspeed"i
                    | "longname"i
                    | "shortname"i
                    | ("usera1"i)
                    | ("usera2"i)
                    | "circular"i
                    | "vehicletype"i
                    | "operator"i
                    | "faresystem"i

attr_value        : BOOLEAN | STRING | SIGNED_INT | FLOAT

nodes             : lin_node+
lin_node          : ("N" | "NODES")? "="? NODE_NUM ","? SEMICOLON_COMMENT? lin_nodeattr*
NODE_NUM          : SIGNED_INT
lin_nodeattr      : lin_nodeattr_name "=" attr_value ","? SEMICOLON_COMMENT*
!lin_nodeattr_name : "access_c"i
                    | "access"i
                    | "delay"i
                    | "xyspeed"i
                    | "timefac"i
                    | "nntime"i
                    | "time"i

operator          : SEMICOLON_COMMENT* "OPERATOR" opmode_attr* SEMICOLON_COMMENT*
mode              : SEMICOLON_COMMENT* "MODE" opmode_attr* SEMICOLON_COMMENT*
opmode_attr       : ( (opmode_attr_name "=" attr_value) ","?  )
opmode_attr_name  : "number" | "name" | "longname"

%import common.SIGNED_INT
%import common.FLOAT
%import common.WS
%ignore WS

"""