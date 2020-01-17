import os
import copy
import csv
import datetime, time
from typing import Any, Dict, Optional

from lark import Lark, Transformer, v_args
from pandas import DataFrame

import pandas as pd
import partridge as ptg

from .logger import WranglerLogger
from .parameters import Parameters


class CubeTransformer(Transformer):
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
        #WranglerLogger.debug("parsing: {}".format(lin_name))

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
        if attr_name in ["USERA", "FREQ", "HEADWAY"]:
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

?start             : program_type_line? lines
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
                    | ("usera"i TIME_PERIOD)
                    | "vehicletype"i
                    | "operator"i
                    | "faresystem"i

attr_value        : BOOLEAN | STRING | SIGNED_INT

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
%import common.WS
%ignore WS

"""


class CubeTransit(object):
    """
    Class for storing information about transit defined in Cube line
    files.  Has the capability to:
    - Parse cube line file properties and shapes into python dictionaries
    - Compare line files and represent changes as Project Card dictionaries
    """

    def __init__(self, parameters=Parameters()):
        """
        line_properties_dict (dict[line names]: line level attributes)
        line_shapes_dict (dict[line names]: line shape df)


        """
        WranglerLogger.debug("Creating a new Cube Transit instance")

        self.lines = []

        self.line_properties = {}
        self.shapes = {}

        self.program_type = None
        self.parameters = parameters

        self.source_list = []

        self.diff_dict = Dict[str, Any]

    def add_cube(self, transit_source: str):
        """
        reads a .lin file and stores as TransitNetwork object

        transformed_tree_data (dict): a dictionary with the following structure
            'program_type': 'PT' or 'TRNBUILD'
            'lines' (dict[line_name]):
                'line_properties' (dict[property_name]): dictionary of property names to values
                'line_shape' (DataFrame): pandas DataFrame of nodes with following columns
                    'node_id' (int)
                    'node' (int)
                    'stop' (boolean)
                    'order' (int)

        Parameters
        -----------
        transit_source:  a string or the directory of the cube line file to be parsed

        Returns
        -------
        TransitNetwork object
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
            msg: "{} not a valid transit line string, directory, or file"
            WranglerLogger.error(msg)
            raise ValueError(msg)

        WranglerLogger.debug("finished parsing cube line file")
        # WranglerLogger.debug("--Parse Tree--\n {}".format(parse_tree.pretty()))
        transformed_tree_data = CubeTransformer().transform(parse_tree)
        # ranglerLogger.debug("--Transformed Tree Data --\n {}".format(transformed_tree_data["lines"]))

        line_properties_dict = {
            k: v["line_properties"] for k, v in transformed_tree_data["lines"].items()
        }
        line_shapes_dict = {
            k: v["line_shape"] for k, v in transformed_tree_data["lines"].items()
        }
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

        self.program_type = transformed_tree_data["program_type"]
        self.lines += new_lines
        self.line_properties.update(line_properties_dict)
        self.shapes.update(line_shapes_dict)

        WranglerLogger.debug("Added lines to CubeTransit: \n".format(new_lines))

    @staticmethod
    def create_from_cube(transit_source: str):
        """
        reads a cube .lin file and stores as TransitNetwork object

        Parameters
        -----------
        transit_source:  a string or the directory of the cube line file to be parsed

        Returns
        -------
        TransitNetwork object
        """

        tn = CubeTransit()
        tn.add_cube(transit_source)

        return tn

    def evaluate_differences(self, base_transit):
        # todo change name to
        """
        1. Identifies what routes need to be updated, deleted, or added
        2. For routes being added or updated, identify if the time periods
            have changed or if there are multiples, and make duplicate lines if so
        3. Create project card dictionaries for each change.

        ##TODO update route shapes

        Parameters
        -----------
        base_transit (CubeTransit): instance of this class for the base condition

        Returns
        -------
        project_card_changes (list): a list of dictionaries containing project card changes required to evaluate the differences between the base network and this transit network instance
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
            base_cube_time_period_numbers = CubeTransit.get_time_period_numbers_from_cube_properties(
                base_transit.line_properties[line]
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

            build_cube_time_period_numbers = CubeTransit.get_time_period_numbers_from_cube_properties(
                self.line_properties[line]
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
            updated_shapes = self.evaluate_route_shape_changes(
                self.shapes[line],
                base_transit.shapes[line],
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
            time_period_numbers = CubeTransit.get_time_period_numbers_from_cube_properties(
                self.line_properties[line]
            )
            if len(time_period_numbers) > 1:
                for tp in time_period_numbers[1:]:
                    lines_to_add.append(self.add_additional_time_periods(tp, line))

        for line in lines_to_add:
            add_card_dict = self.create_add_route_card_dict(line)
            project_card_changes.append(add_card_dict)

        return project_card_changes

    def add_additional_time_periods(self, new_time_period_number, orig_line_name):
        """
        Copies a route to another cube time period with appropriate
        values for time-period-specific properties.

        New properties are stored under the new name in:
            self.shapes
            self.line_properties

        Args:
            new_time_period_number (int): cube time period number
            orig_line_name(str): name of the originating line, from which
                the new line will copy its properties.

        Returns:
            new_tp_line_name
        """
        WranglerLogger.debug(
            "adding time periods {} to line {}".format(
                new_time_period_number, orig_line_name
            )
        )

        route_id, _init_time_period, agency_id, direction_id = CubeTransit.unpack_route_name(
            orig_line_name
        )
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
                ```
                'property': <property name>,
                'set': <new property value>
                ```

        Returns:
            update_card_dict (dict): project card change formatted dictionary
        """
        base_start_time_str, base_end_time_str = self.calculate_start_end_times(
            self.line_properties[line]
        )

        update_card_dict = {
            "category": "Transit Service Property Change",
            "facility": {
                "route_id": line.split("_")[1],
                "direction_id": int(line.strip('"')[-1]),
                "start_time": base_start_time_str,
                "end_time": base_end_time_str,
            },
            "properties": updated_properties_dict,
        }
        WranglerLogger.debug(
            "Updating {} route to changes:\n{}".format(line, str(update_card_dict))
        )

        return update_card_dict

    def create_delete_route_card_dict(self, line: str, base_transit_line_properties_dict: dict):
        """
        Creates a project card change formatted dictionary for deleting a line.

        Args:
            line: name of line that is being deleted
            base_transit_line_properties_dict: dictionary of cube-style
                attribute values in order to find time periods and
                start and end times.

        Returns:
            delete_card_dict (dict):  project card change formatted dictionary
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

    def create_add_route_card_dict(self, line):
        """
        Creates a project card change formatted dictionary for adding
        a route.

        Args:
            line: name of line that is being updated

        Returns:
            add_card_dict (dict): project card change formatted dictionary
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
                "direction_id": int(line.strip('"')[-1]),
                "start_time": start_time_str,
                "end_time": end_time_str,
                "agency_id": int(line.strip('"')[0]),
            },
            "properties": standard_properties + [routing_properties],
        }

        WranglerLogger.debug(
            "Adding {} route to changes:\n{}".format(line, add_card_dict)
        )
        return add_card_dict

    @staticmethod
    def get_time_period_numbers_from_cube_properties(properties_list):
        """
        Finds properties with time periods and the returns the numbers in them

        Args
        ------
        properties_list (list)

        Returns
        --------
        time_periods_list (list): list of strings of the time period numbers found
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
    def build_route_name(route_id="", time_period="", agency_id=0, direction_id=1):
        """
        Create a route name by contatenating route, time period, agency, and direction

        Args
        ------
        route_id (str): 452-111
        time_period (str): i.e. pk
        direction_id (str) : i.e. 1
        agency_id (str) : i.e. 0

        Returns
        --------
        line_name (str): i.e. "0_452-111_452_pk1"
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
    def unpack_route_name(line_name:str):
        """
        Unpacks route name into direction, route, agency, and time period info

        Args
        ------
        line_name (str): For example `0_452-111_452_pk1`

        Returns
        --------
        route_id (str): Route ID, for example 452-111
        time_period (str): Time Period abbrebiation, for example pk or MD
        direction_id (str) : Direction ID, for example 1
        agency_id (str) : Agency ID, for example 0
        """

        line_name = line_name.strip('"')

        agency_id, route_id, _rtid, _tp_direction = line_name.split("_")
        time_period = _tp_direction[0:-1]
        direction_id = _tp_direction[-1]

        return route_id, time_period, agency_id, direction_id

    def calculate_start_end_times(self, line_properties_dict):
        """
        Calculate the start and end times of the property change
        WARNING: Doesn't take care of discongruous time periods!!!!

        Parameters
        -----------
        line_properties_dict (dict): dictionary of cube-flavor properties for a transit line
        """
        start_time_m = 24 * 60
        end_time_m = 0 * 60

        WranglerLogger.debug(
            "parameters.time_period_properties_list: {}".format(
                self.parameters.time_period_properties_list
            )
        )
        current_cube_time_period_numbers = CubeTransit.get_time_period_numbers_from_cube_properties(
            line_properties_dict
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
            WranglerLogger.error(msg)
            raise ValueError(msg)

        start_time_str = "{:02d}:{:02d}".format(*divmod(start_time_m, 60))
        end_time_str = "{:02d}:{:02d}".format(*divmod(end_time_m, 60))
        return start_time_str, end_time_str

    @staticmethod
    def cube_properties_to_standard_properties(cube_properties_dict):
        """
        Converts cube style properties to standard properties.
        This is most pertinent to time-period specific variables like headway,
        and varibles that have stnadard units like headway, which is minutes
        in cube and seconds in standard format.

        parameters
        -----------
        cube_properties_dict(dict):
            <cube style property name> : <property value>

        returns
        --------
        standard_properties_list (list):
            list of dictionaries with:
               - "property": <standard style property name>
               - "set" : <property value with correct units>

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
        properties_build,
        properties_base,
        time_period_number,
        absolute=True,
        validate_base=False,
    ):
        """
        Checks if any values have been updated or added for a specific
        route and creates project card entries for each.

        Parameters
        -----------
        properties_build (dict): property_name: property_value
        properties_base (dict): property_name: property_value
        time_period_number (str): time period to evaluate
        absolute (bool): if True, will use `set` command rather than a change.  If false, will automatically check the base value.  Note that this only applies to the numeric values of frequency/headway
        validate_base (bool): if True, will add the `existing` line in the project card

        Returns
        -------
        transit_change_list (list): a list of dictionary values suitable for writing to a project card
             'property': <property_name>
             'set': <set value>
             'change': <change from existing value>
             'existing': <existing value to check>

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

    def evaluate_route_shape_changes(self, shape_build: DataFrame, shape_base: DataFrame):
        """

        Args:
            shape_build: DataFrame
            shape_base: DataFrame

        """

        if shape_build.node.equals(shape_base.node):
            return None

        shape_change_list = []

        base_node_list = shape_build.node.tolist()
        build_node_list = shape_base.node.tolist()

        sort_len = max(len(base_node_list), len(build_node_list))

        start_pos = None
        end_pos = None
        for i in range(sort_len):
            if ((i == len(base_node_list)) | (i == len(build_node_list))):
                start_pos = i-1
                break
            if base_node_list[i] != build_node_list[i]:
                start_pos = i
                break
            else:
                continue

        j = -1
        for i in range(sort_len):
            if ((i == len(base_node_list)) | (i == len(build_node_list))):
                end_pos = j+1
                break
            if base_node_list[j] != build_node_list[j]:
                end_pos = j
                break
            else:
                j -= 1

        if (start_pos or end_pos):
            existing = base_node_list[(start_pos-2 if start_pos > 1 else None):(end_pos+2 if end_pos < -2 else None)]
            set = build_node_list[(start_pos-2 if start_pos > 1 else None):(end_pos+2 if end_pos < -2 else None)]

            shape_change_list.append(
                {"property" : "routing", "existing" : existing, "set" : set}
            )

        return shape_change_list


class StandardTransit(object):
    """
    Holds a standard transit feed as a Partridge object and contains
    methods to manipulate and translate the GTFS data to MetCouncil's
    Cube Line files.
    """

    def __init__(self, ptg_feed, parameters=Parameters()):
        self.feed = ptg_feed
        self.parameters = parameters

    @staticmethod
    def read_gtfs(gtfs_feed_dir: str, parameters: Parameters = Parameters()):
        """
        Reads GTFS files from a directory and returns a StandardTransit
        instance.

        Args:
            gtfs_feed_dir: location of the GTFS files
            parameters (Optional): Parameters instance

        Returns:
            StandardTransit instance
        """
        return StandardTransit(ptg.load_feed(gtfs_feed_dir), parameters=parameters)

    def write_as_cube_lin(self, outpath: str):
        """
        Writes the gtfs feed as a cube line file after
        converting gtfs properties to MetCouncil cube properties.

        Args:
            outpath: file location for cube line file

        """


        trip_cube_df = self.route_properties_gtfs_to_cube(self)

        trip_cube_df["LIN"] = trip_cube_df.apply(self.cube_format, axis=1)

        l = trip_cube_df["LIN"].tolist()

        with open(outpath,'w') as f:
            f.write("\n".join(l))

    @staticmethod
    def route_properties_gtfs_to_cube(self):
        """
        prepare gtfs for cube lin file

        Returns:
            trip_df (DataFrame): DataFrame of trips with cube-appropriate values for:
                - NAME
                - ONEWAY
                - OPERATOR
                - MODE
                - HEADWAY
        """
        WranglerLogger.info("Converting GTFS Standard Properties to MetCouncil's Cube Standard")
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

        trip_df["tod"] = trip_df.start_time.apply(self.time_to_cube_time_period)

        trip_df["NAME"] = trip_df.apply(
            lambda x: x.agency_id
            + "_"
            + x.route_id
            + "_"
            + x.route_short_name
            + "_"
            + x.tod
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

        For rail, uses GTFS route_type variable:
        https://developers.google.com/transit/gtfs/reference

        ```
        #                 route_type : cube_mode
        route_type_to_cube_mode = {0: 8, # Tram, Streetcar, Light rail
                                   3: 0, # Bus; further disaggregated for cube
                                   2: 9} # Rail
        ```
        For buses, uses route id numbers and route name to find
        express and suburban buses  as follows:

        ```
        if not cube_mode:
            if 'express' in row['LONGNAME'].lower():
                cube_mode = 7  # Express
            elif int(row['route_id'].split("-")[0]) > 99:
                cube_mode = 6  # Suburban Local
            else:
                cube_mode = 5  # Urban Local
        ```

        Args:
            row: DataFrame row

        Returns:
            cube_mode: cube mode number
        """
        #                 route_type : cube_mode
        route_type_to_cube_mode = {0: 8, # Tram, Streetcar, Light rail
                                   3: 0, # Bus; further disaggregated for cube
                                   2: 9} # Rail


        cube_mode = route_type_to_cube_mode[row["route_type"]]

        if not cube_mode:
            if 'express' in row['route_long_name'].lower():
                cube_mode = 7  # Express
            elif int(row['route_id'].split("-")[0]) > 99:
                cube_mode = 6  # Suburban Local
            else:
                cube_mode = 5  # Urban Local

        return cube_mode

    def time_to_cube_time_period(self, start_time_secs: int, as_str: bool = True, verbose: bool = False):
        """
        Converts seconds from midnight to the cube time period.

        if `as_str` is True, will return as the time period name,
        otherwise will return as time period number.

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

        name_to_num = {v: k for k, v in self.parameters.cube_time_periods.items}
        this_tp_num = name_to_num.get(this_tp)

        if not this_tp_num:
            msg = "Cannot find time period number in {} for time period name: {}".format(
                name_to_num, this_tp
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        return this_tp_num

    def shape_gtfs_to_cube(self, row):
        """
        Creates a list of nodes that for the route in appropriate
        cube format.

        Returns:
            node_list_str: a string representation of the node list
                for a route in cube format.

        """
        trip_stop_times_df = self.feed.stop_times.copy()
        trip_stop_times_df = trip_stop_times_df[trip_stop_times_df.trip_id == row.trip_id]

        trip_node_df = self.feed.shapes.copy()
        trip_node_df = trip_node_df[trip_node_df.shape_id == row.shape_id]

        trip_stop_times_df = pd.merge(
            trip_stop_times_df, self.feed.stops, how="left", on="stop_id"
        )

        stop_node_id_list = trip_stop_times_df["model_node_id"].tolist()
        trip_node_list = trip_node_df["shape_model_node_id"].tolist()

        # node list
        node_list_str = ""
        for nodeIdx in range(len(trip_node_list)):
            if trip_node_list[nodeIdx] in stop_node_id_list:
                node_list_str += "\n %s" % (trip_node_list[nodeIdx])
                if nodeIdx < (len(trip_node_list) - 1):
                    node_list_str += ","
            else:
                node_list_str += "\n -%s" % (trip_node_list[nodeIdx])
                if nodeIdx < (len(trip_node_list) - 1):
                    node_list_str += ","

        return node_list_str

    def cube_format(self, row):
        """
        Creates a string represnting the route in cube line file notation

        Args:
            row: row of trips DataFrame

        Returns:
            s: string representation of route in cube line file notation
        """

        s = '\nLINE NAME="{}",'.format(row.NAME)
        s += '\n LONGNAME="{}",'.format(row.LONGNAME)
        s += "\n HEADWAY[{}]={},".format(row.tod, row.HEADWAY)
        s += "\n MODE={},".format(row.MODE)
        s += "\n ONEWAY={},".format(row.ONEWAY)
        s += "\n OPERATOR={},".format(row.OPERATOR)
        s += "\n NODES={}".format(self.shape_gtfs_to_cube(row))

        return s
