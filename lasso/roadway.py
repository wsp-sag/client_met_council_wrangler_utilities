import copy
import glob
import os
from typing import Optional, Union

import geopandas as gpd
import pandas as pd

from geopandas import GeoDataFrame
from pandas import DataFrame
import numpy as np

from network_wrangler import RoadwayNetwork
from .parameters import Parameters
from .logger import WranglerLogger


class ModelRoadwayNetwork(RoadwayNetwork):
    """
    Subclass of network_wrangler class :ref:`RoadwayNetwork <network_wrangler:RoadwayNetwork>`

    A representation of the physical roadway network and its properties.
    """

    CALCULATED_VALUES = [
        "area_type",
        "county",
        "centroidconnect",
    ]

    def __init__(
        self,
        nodes: GeoDataFrame,
        links: DataFrame,
        shapes: GeoDataFrame,
        parameters: Union[Parameters, dict] = {},
        **kwargs,
    ):
        """
        Constructor

        Args:
            nodes: geodataframe of nodes
            links: dataframe of links
            shapes: geodataframe of shapes
            parameters: dictionary of parameter settings (see Parameters class) or an instance of Parameters.
                If not specified, will use default parameters.
            crs (int): coordinate reference system, ESPG number
            node_foreign_key (str):  variable linking the node table to the link table
            link_foreign_key (list): list of variable linking the link table to the node foreign key
            shape_foreign_key (str): variable linking the links table and shape table
            unique_link_ids (list): list of variables unique to each link
            unique_node_ids (list): list of variables unique to each node
            modes_to_network_link_variables (dict): Mapping of modes to link variables in the network
            modes_to_network_nodes_variables (dict): Mapping of modes to node variables in the network
            managed_lanes_node_id_scalar (int): Scalar values added to primary keys for nodes for
                corresponding managed lanes.
            managed_lanes_link_id_scalar (int): Scalar values added to primary keys for links for
                corresponding managed lanes.
            managed_lanes_required_attributes (list): attributes that must be specified in managed
                lane projects.
            keep_same_attributes_ml_and_gp (list): attributes to copy to managed lanes from parallel
                general purpose lanes.
        """
        super().__init__(nodes, links, shapes, **kwargs)

        # will have to change if want to alter them
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

        self.links_metcouncil_df = None
        self.nodes_metcouncil_df = None

        self.fill_na()
        self.convert_int()
        # self.shapes_metcouncil_df = None
        ##todo also write to file
        # WranglerLogger.debug("Used PARAMS\n", '\n'.join(['{}: {}'.format(k,v) for k,v in self.parameters.__dict__.items()]))

    @staticmethod
    def read(
        link_filename: str,
        node_filename: str,
        shape_filename: str,
        fast: bool = False,
        recalculate_calculated_variables=False,
        recalculate_distance=False,
        parameters: Union[dict, Parameters] = {},
        **kwargs,
    ):
        """
        Reads in links and nodes network standard.

        Args:
            link_filename (str): File path to link json.
            node_filename (str): File path to node geojson.
            shape_filename (str): File path to link true shape geojson
            fast (bool): boolean that will skip validation to speed up read time.
            recalculate_calculated_variables (bool): calculates fields from spatial joins, etc.
            recalculate_distance (bool):  re-calculates distance.
            parameters: dictionary of parameter settings (see Parameters class) or an instance of Parameters. If not specified, will use default parameters.
            crs (int): coordinate reference system, ESPG number
            node_foreign_key (str):  variable linking the node table to the link table
            link_foreign_key (list): list of variable linking the link table to the node foreign key
            shape_foreign_key (str): variable linking the links table and shape table
            unique_link_ids (list): list of variables unique to each link
            unique_node_ids (list): list of variables unique to each node
            modes_to_network_link_variables (dict): Mapping of modes to link variables in the network
            modes_to_network_nodes_variables (dict): Mapping of modes to node variables in the network
            managed_lanes_node_id_scalar (int): Scalar values added to primary keys for nodes for
                corresponding managed lanes.
            managed_lanes_link_id_scalar (int): Scalar values added to primary keys for links for
                corresponding managed lanes.
            managed_lanes_required_attributes (list): attributes that must be specified in managed
                lane projects.
            keep_same_attributes_ml_and_gp (list): attributes to copy to managed lanes from parallel
                general purpose lanes.
        Returns:
            ModelRoadwayNetwork
        """

        nodes_df, links_df, shapes_df = RoadwayNetwork.load_transform_network(
            node_filename,
            link_filename,
            shape_filename,
            validate_schema=not fast,
            **kwargs,
        )

        m_road_net = ModelRoadwayNetwork(
            nodes_df,
            links_df,
            shapes_df,
            parameters=parameters,
            **kwargs,
        )

        if recalculate_calculated_variables:
            m_road_net.create_calculated_variables()
        if recalculate_distance:
            m_road_net.calculate_distance(overwrite=True)

        m_road_net.fill_na()
        # this method is making period values as string "NaN", need to revise.
        m_road_net.split_properties_by_time_period_and_category()
        for c in m_road_net.links_df.columns:
            m_road_net.links_df[c] = m_road_net.links_df[c].replace("NaN", np.nan)
        m_road_net.convert_int()

        return m_road_net

    @staticmethod
    def from_RoadwayNetwork(
        roadway_network_object,
        parameters: Union[dict, Parameters] = {},
    ):
        """
        RoadwayNetwork to ModelRoadwayNetwork

        Args:
            roadway_network_object (RoadwayNetwork).
            parameters: dictionary of parameter settings (see Parameters class) or an instance of Parameters. If not specified, will use default parameters.

        Returns:
            ModelRoadwayNetwork
        """

        additional_params_dict = {
            k: v
            for k, v in roadway_network_object.__dict__.items()
            if k not in ["nodes_df", "links_df", "shapes_df", "parameters"]
        }

        return ModelRoadwayNetwork(
            roadway_network_object.nodes_df,
            roadway_network_object.links_df,
            roadway_network_object.shapes_df,
            parameters=parameters,
            **additional_params_dict,
        )

    def split_properties_by_time_period_and_category(self, properties_to_split=None):
        """
        Splits properties by time period, assuming a variable structure of

        Args:
            properties_to_split: dict
                dictionary of output variable prefix mapped to the source variable and what to stratify it by
                e.g.
                {
                    'lanes' : {'v':'lanes', 'times_periods':{"AM": ("6:00", "10:00"),"PM": ("15:00", "19:00")}},
                    'ML_lanes' : {'v':'ML_lanes', 'times_periods':{"AM": ("6:00", "10:00"),"PM": ("15:00", "19:00")}},
                    'use' : {'v':'use', 'times_periods':{"AM": ("6:00", "10:00"),"PM": ("15:00", "19:00")}},
                }

        """
        import itertools

        if properties_to_split == None:
            properties_to_split = self.parameters.properties_to_split

        for out_var, params in properties_to_split.items():
            if params["v"] not in self.links_df.columns:
                WranglerLogger.warning(
                    "Specified variable to split: {} not in network variables: {}. Returning 0.".format(
                        params["v"], str(self.links_df.columns)
                    )
                )
                if params.get("time_periods") and params.get("categories"):

                    for time_suffix, category_suffix in itertools.product(
                        params["time_periods"], params["categories"]
                    ):
                        self.links_df[
                            out_var + "_" + time_suffix + "_" + category_suffix
                        ] = 0
                elif params.get("time_periods"):
                    for time_suffix in params["time_periods"]:
                        self.links_df[out_var + "_" + time_suffix] = 0
            elif params.get("time_periods") and params.get("categories"):
                for time_suffix, category_suffix in itertools.product(
                    params["time_periods"], params["categories"]
                ):
                    self.links_df[
                        out_var + "_" + category_suffix + "_" + time_suffix
                    ] = self.get_property_by_time_period_and_group(
                        params["v"],
                        category=params["categories"][category_suffix],
                        time_period=params["time_periods"][time_suffix],
                    )
            elif params.get("time_periods"):
                for time_suffix in params["time_periods"]:
                    self.links_df[
                        out_var + "_" + time_suffix
                    ] = self.get_property_by_time_period_and_group(
                        params["v"],
                        category=None,
                        time_period=params["time_periods"][time_suffix],
                    )
            else:
                raise ValueError(
                    "Shoudn't have a category without a time period: {}".format(params)
                )

    def create_calculated_variables(self):
        """
        Creates calculated roadway variables.

        Args:
            None
        """
        WranglerLogger.info("Creating calculated roadway variables.")

        #MTC
        self.create_ML_variable()
        #/MTC
        #MC
        self.calculate_area_type()
        self.calculate_county()
        self.calculate_mpo()
        self.add_counts()
        self.create_ML_variable()
        self.create_hov_corridor_variable()
        self.create_managed_variable()
        #/MC

    def calculate_county(
        self,
        county_shape=None,
        county_shape_variable=None,
        network_variable="county",
        county_codes_dict=None,
        overwrite=False,
    ):
        """
        #MC
        Calculates county variable.

        This uses the centroid of the geometry field to determine which county it should be labeled.
        This isn't perfect, but it much quicker than other methods.

        Args:
            county_shape (str): The File path to county geodatabase.
            county_shape_variable (str): The variable name of county in county geodadabase.
            network_variable (str): The variable name of county in network standard.  Default to "county".
            overwrite (Bool): True if overwriting existing county variable in network.  Default to False.

        Returns:
            None
        """
        if network_variable in self.links_df:
            if overwrite:
                WranglerLogger.info(
                    "Overwriting existing County Variable '{}' already in network".format(
                        network_variable
                    )
                )
            else:
                WranglerLogger.info(
                    "County Variable '{}' already in network. Returning without overwriting.".format(
                        network_variable
                    )
                )
                return

        """
        Verify inputs
        """

        county_shape = county_shape if county_shape else self.parameters.county_shape

        county_shape_variable = (
            county_shape_variable
            if county_shape_variable
            else self.parameters.county_variable_shp
        )

        WranglerLogger.info(
            "Adding roadway network variable for county using a spatial join with: {}".format(
                county_shape
            )
        )

        county_codes_dict = (
            county_codes_dict if county_codes_dict else self.parameters.county_code_dict
        )
        if not county_codes_dict:
            msg = "No county codes dictionary specified"
            WranglerLogger.error(msg)
            raise ValueError(msg)

        """
        Start actual process
        """

        centroids_gdf = self.links_df.copy()
        centroids_gdf["geometry"] = centroids_gdf["geometry"].centroid

        county_gdf = gpd.read_file(county_shape)
        county_gdf = county_gdf.to_crs(epsg=self.crs)
        joined_gdf = gpd.sjoin(centroids_gdf, county_gdf, how="left", op="intersects")

        joined_gdf[county_shape_variable] = (
            joined_gdf[county_shape_variable]
            .map(county_codes_dict)
            .fillna(10)
            .astype(int)
        )

        self.links_df[network_variable] = joined_gdf[county_shape_variable]

        WranglerLogger.info(
            "Finished Calculating county variable: {}".format(network_variable)
        )

    def calculate_area_type(
        self,
        area_type_shape=None,
        area_type_shape_variable=None,
        network_variable="area_type",
        area_type_codes_dict=None,
        downtown_area_type_shape=None,
        downtown_area_type=None,
        overwrite=False,
    ):
        """
        #MC
        Calculates area type variable.

        This uses the centroid of the geometry field to determine which area it should be labeled.
        This isn't perfect, but it much quicker than other methods.

        Args:
            area_type_shape (str): The File path to area geodatabase.
            area_type_shape_variable (str): The variable name of area type in area geodadabase.
            network_variable (str): The variable name of area type in network standard.  Default to "area_type".
            area_type_codes_dict: The dictionary to map input area_type_shape_variable to network_variable
            downtown_area_type_shape: The file path to the downtown area type boundary.
            downtown_area_type (int): Integer value of downtown area type
            overwrite (Bool): True if overwriting existing county variable in network.  Default to False.

        Returns:
            None

        """

        if network_variable in self.links_df:
            if overwrite:
                WranglerLogger.info(
                    "Overwriting existing Area Type Variable '{}' already in network".format(
                        network_variable
                    )
                )
            else:
                WranglerLogger.info(
                    "Area Type Variable '{}' already in network. Returning without overwriting.".format(
                        network_variable
                    )
                )
                return

        WranglerLogger.info(
            "Calculating Area Type from Spatial Data and adding as roadway network variable: {}".format(
                network_variable
            )
        )

        """
        Verify inputs
        """

        area_type_shape = (
            area_type_shape if area_type_shape else self.parameters.area_type_shape
        )

        if not area_type_shape:
            msg = "No area type shape specified"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if not os.path.exists(area_type_shape):
            msg = "File not found for area type shape: {}".format(area_type_shape)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        area_type_shape_variable = (
            area_type_shape_variable
            if area_type_shape_variable
            else self.parameters.area_type_variable_shp
        )

        if not area_type_shape_variable:
            msg = "No area type shape varible specified"
            WranglerLogger.error(msg)
            raise ValueError(msg)

        area_type_codes_dict = (
            area_type_codes_dict
            if area_type_codes_dict
            else self.parameters.area_type_code_dict
        )
        if not area_type_codes_dict:
            msg = "No area type codes dictionary specified"
            WranglerLogger.error(msg)
            raise ValueError(msg)

        downtown_area_type_shape = (
            downtown_area_type_shape
            if downtown_area_type_shape
            else self.parameters.downtown_area_type_shape
        )

        if not downtown_area_type_shape:
            msg = "No downtown area type shape specified"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if not os.path.exists(downtown_area_type_shape):
            msg = "File not found for downtown area type shape: {}".format(
                downtown_area_type_shape
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        downtown_area_type = (
            downtown_area_type
            if downtown_area_type
            else self.parameters.downtown_area_type
        )
        if not downtown_area_type:
            msg = "No downtown area type value specified"
            WranglerLogger.error(msg)
            raise ValueError(msg)

        """
        Start actual process
        """
        centroids_gdf = self.links_df.copy()
        centroids_gdf["geometry"] = centroids_gdf["geometry"].centroid

        WranglerLogger.debug("Reading Area Type Shapefile {}".format(area_type_shape))
        area_type_gdf = gpd.read_file(area_type_shape)
        area_type_gdf = area_type_gdf.to_crs(epsg=self.crs)

        downtown_gdf = gpd.read_file(downtown_area_type_shape)
        downtown_gdf = downtown_gdf.to_crs(epsg=self.crs)

        joined_gdf = gpd.sjoin(
            centroids_gdf, area_type_gdf, how="left", op="intersects"
        )

        joined_gdf[area_type_shape_variable] = (
            joined_gdf[area_type_shape_variable]
            .map(area_type_codes_dict)
            .fillna(1)
            .astype(int)
        )

        WranglerLogger.debug("Area Type Codes Used: {}".format(area_type_codes_dict))

        d_joined_gdf = gpd.sjoin(
            centroids_gdf, downtown_gdf, how="left", op="intersects"
        )

        d_joined_gdf["downtown_area_type"] = d_joined_gdf["Id"].fillna(-99).astype(int)

        joined_gdf.loc[
            d_joined_gdf["downtown_area_type"] == 0, area_type_shape_variable
        ] = downtown_area_type

        WranglerLogger.debug(
            "Downtown Area Type used boundary file: {}".format(downtown_area_type_shape)
        )

        self.links_df[network_variable] = joined_gdf[area_type_shape_variable]

        WranglerLogger.info(
            "Finished Calculating Area Type from Spatial Data into variable: {}".format(
                network_variable
            )
        )

    def calculate_mpo(
        self,
        county_network_variable="county",
        network_variable="mpo",
        as_integer=True,
        mpo_counties=None,
        overwrite=False,
    ):
        """
        Calculates mpo variable.
        #MC
        Args:
            county_variable (str): Name of the variable where the county names are stored.  Default to "county".
            network_variable (str): Name of the variable that should be written to.  Default to "mpo".
            as_integer (bool): If true, will convert true/false to 1/0s.
            mpo_counties (list): List of county names that are within mpo region.
            overwrite (Bool): True if overwriting existing county variable in network.  Default to False.

        Returns:
            None
        """

        if network_variable in self.links_df:
            if overwrite:
                WranglerLogger.info(
                    "Overwriting existing MPO Variable '{}' already in network".format(
                        network_variable
                    )
                )
            else:
                WranglerLogger.info(
                    "MPO Variable '{}' already in network. Returning without overwriting.".format(
                        network_variable
                    )
                )
                return

        WranglerLogger.info(
            "Calculating MPO as roadway network variable: {}".format(network_variable)
        )
        """
        Verify inputs
        """
        county_network_variable = (
            county_network_variable
            if county_network_variable
            else self.parameters.county_network_variable
        )

        if not county_network_variable:
            msg = "No variable specified as containing 'county' in the network."
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if county_network_variable not in self.links_df.columns:
            msg = "Specified county network variable: {} does not exist in network. Try running or debuging county calculation."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        mpo_counties = mpo_counties if mpo_counties else self.parameters.mpo_counties

        if not mpo_counties:
            msg = "No MPO Counties specified in method call or in parameters."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        WranglerLogger.debug("MPO Counties: {}".format(",".join(str(mpo_counties))))

        """
        Start actual process
        """

        mpo = self.links_df[county_network_variable].isin(mpo_counties)

        if as_integer:
            mpo = mpo.astype(int)

        self.links_df[network_variable] = mpo

        WranglerLogger.info(
            "Finished calculating MPO variable: {}".format(network_variable)
        )

    def add_variable_using_shst_reference(
        self,
        var_shst_csvdata=None,
        shst_csv_variable=None,
        network_variable=None,
        network_var_type=int,
        overwrite=False,
    ):
        """
        Join network links with source data, via SHST API node match result.

        Args:
            var_shst_csvdata (str): File path to SHST API return.
            shst_csv_variable (str): Variable name in the source data.
            network_variable (str): Name of the variable that should be written to.
            network_var_type : Variable type in the written network.
            overwrite (bool): True is overwriting existing variable. Default to False.

        Returns:
            None

        """
        WranglerLogger.info(
            "Adding Variable {} using Shared Streets Reference from {}".format(
                network_variable, var_shst_csvdata
            )
        )

        var_shst_df = pd.read_csv(var_shst_csvdata)

        if "shstReferenceId" not in var_shst_df.columns:
            msg = "'shstReferenceId' required but not found in {}".format(var_shst_data)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        if shst_csv_variable not in var_shst_df.columns:
            msg = "{} required but not found in {}".format(
                shst_csv_variable, var_shst_data
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        join_gdf = pd.merge(
            self.links_df,
            var_shst_df[["shstReferenceId", shst_csv_variable]],
            how="left",
            on="shstReferenceId",
        )

        join_gdf[shst_csv_variable].fillna(0, inplace=True)

        if network_variable in self.links_df.columns and not overwrite:
            join_gdf.loc[join_gdf[network_variable] > 0, network_variable] = join_gdf[
                shst_csv_variable
            ].astype(network_var_type)
        else:
            join_gdf[network_variable] = join_gdf[shst_csv_variable].astype(
                network_var_type
            )

        self.links_df[network_variable] = join_gdf[network_variable]

        WranglerLogger.info(
            "Added variable: {} using Shared Streets Reference".format(network_variable)
        )

    def add_counts(
        self,
        network_variable="AADT",
        mndot_count_shst_data=None,
        widot_count_shst_data=None,
        mndot_count_variable_shp=None,
        widot_count_variable_shp=None,
    ):

        """
        Adds count variable.
        #MC
        join the network with count node data, via SHST API node match result

        Args:
            network_variable (str): Name of the variable that should be written to.  Default to "AADT".
            mndot_count_shst_data (str): File path to MNDOT count location SHST API node match result.
            widot_count_shst_data (str): File path to WIDOT count location SHST API node match result.
            mndot_count_variable_shp (str): File path to MNDOT count location geodatabase.
            widot_count_variable_shp (str): File path to WIDOT count location geodatabase.

        Returns:
            None
        """

        WranglerLogger.info("Adding Counts")

        """
        Verify inputs
        """

        mndot_count_shst_data = (
            mndot_count_shst_data
            if mndot_count_shst_data
            else self.parameters.mndot_count_shst_data
        )
        widot_count_shst_data = (
            widot_count_shst_data
            if widot_count_shst_data
            else self.parameters.widot_count_shst_data
        )
        mndot_count_variable_shp = (
            mndot_count_variable_shp
            if mndot_count_variable_shp
            else self.parameters.mndot_count_variable_shp
        )
        widot_count_variable_shp = (
            widot_count_variable_shp
            if widot_count_variable_shp
            else self.parameters.widot_count_variable_shp
        )

        for varname, var in {
            "mndot_count_shst_data": mndot_count_shst_data,
            "widot_count_shst_data": widot_count_shst_data,
        }.items():
            if not var:
                msg = "'{}' not found in method or lasso parameters.".format(varname)
                WranglerLogger.error(msg)
                raise ValueError(msg)
            if not os.path.exists(var):
                msg = "{}' not found at following location: {}.".format(varname, var)
                WranglerLogger.error(msg)
                raise ValueError(msg)

        for varname, var in {
            "mndot_count_variable_shp": mndot_count_variable_shp,
            "widot_count_variable_shp": widot_count_variable_shp,
        }.items():
            if not var:
                msg = "'{}' not found in method or lasso parameters.".format(varname)
                WranglerLogger.error(msg)
                raise ValueError(msg)

        """
        Start actual process
        """
        WranglerLogger.debug(
            "Adding MNDOT Counts using \n- shst file: {}\n- shp file: {}\n- as network variable: {}".format(
                mndot_count_shst_data, mndot_count_variable_shp, network_variable
            )
        )
        # Add Minnesota Counts
        self.add_variable_using_shst_reference(
            var_shst_csvdata=mndot_count_shst_data,
            shst_csv_variable=mndot_count_variable_shp,
            network_variable=network_variable,
            network_var_type=int,
            overwrite=True,
        )
        WranglerLogger.debug(
            "Adding WiDot Counts using \n- shst file: {}\n- shp file: {}\n- as network variable: {}".format(
                widot_count_shst_data, widot_count_variable_shp, network_variable
            )
        )
        # Add Wisconsin Counts, but don't overwrite Minnesota
        self.add_variable_using_shst_reference(
            var_shst_csvdata=widot_count_shst_data,
            shst_csv_variable=widot_count_variable_shp,
            network_variable=network_variable,
            network_var_type=int,
            overwrite=False,
        )

        self.links_df["count_AM"] = self.links_df[network_variable] / 4
        self.links_df["count_MD"] = self.links_df[network_variable] / 4
        self.links_df["count_PM"] = self.links_df[network_variable] / 4
        self.links_df["count_NT"] = self.links_df[network_variable] / 4

        self.links_df["count_daily"] = self.links_df[network_variable]
        self.links_df["count_year"] = 2017

        WranglerLogger.info(
            "Finished adding counts variable: {}".format(network_variable)
        )

    @staticmethod
    def read_match_result(path):
        """
        Reads the shst geojson match returns.

        Returns shst dataframe.

        Reading lots of same type of file and concatenating them into a single DataFrame.

        Args:
            path (str): File path to SHST match results.

        Returns:
            geodataframe: geopandas geodataframe

        ##todo
        not sure why we need, but should be in utilities not this class
        """
        refId_gdf = DataFrame()
        refid_file = glob.glob(path)
        for i in refid_file:
            new = gpd.read_file(i)
            refId_gdf = pd.concat([refId_gdf, new], ignore_index=True, sort=False)
        return refId_gdf

    @staticmethod
    def get_attribute(
        links_df,
        join_key,  # either "shstReferenceId", or "shstGeometryId", tests showed the latter gave better coverage
        source_shst_ref_df,  # source shst refId
        source_gdf,  # source dataframe
        field_name,  # , # targetted attribute from source
    ):
        """
        Gets attribute from source data using SHST match result.

        Args:
            links_df (dataframe): The network dataframe that new attribute should be written to.
            join_key (str): SHST ID variable name used to join source data with network dataframe.
            source_shst_ref_df (str): File path to source data SHST match result.
            source_gdf (str): File path to source data.
            field_name (str): Name of the attribute to get from source data.

        Returns:
            None
        """
        # join based on shared streets geometry ID
        # pp_link_id is shared streets match return
        # source_ink_id is mrcc
        WranglerLogger.debug(
            "source ShSt rename_variables_for_dbf columns\n{}".format(
                source_shst_ref_df.columns
            )
        )
        WranglerLogger.debug("source gdf columns\n{}".format(source_gdf.columns))
        # end up with OSM network with the MRCC Link ID
        # could also do with route_sys...would that be quicker?
        join_refId_df = pd.merge(
            links_df,
            source_shst_ref_df[[join_key, "pp_link_id", "score"]].rename(
                columns={"pp_link_id": "source_link_id", "score": "source_score"}
            ),
            how="left",
            on=join_key,
        )

        # joined with MRCC dataframe to get route_sys

        join_refId_df = pd.merge(
            join_refId_df,
            source_gdf[["LINK_ID", field_name]].rename(
                columns={"LINK_ID": "source_link_id"}
            ),
            how="left",
            on="source_link_id",
        )

        # drop duplicated records with same field value

        join_refId_df.drop_duplicates(
            subset=["model_link_id", "shstReferenceId", field_name], inplace=True
        )

        # more than one match, take the best score

        join_refId_df.sort_values(
            by=["model_link_id", "source_score"],
            ascending=True,
            na_position="first",
            inplace=True,
        )

        join_refId_df.drop_duplicates(
            subset=["model_link_id"], keep="last", inplace=True
        )

        # self.links_df[field_name] = join_refId_df[field_name]

        return join_refId_df[links_df.columns.tolist() + [field_name, "source_link_id"]]

    def calculate_use(
        self,
        network_variable="use",
        as_integer=True,
        overwrite=False,
    ):
        """
        Calculates use variable.

        Args:
            network_variable (str): Variable that should be written to in the network. Default to "use"
            as_integer (bool): If True, will convert true/false to 1/0s.  Defauly to True.
            overwrite (Bool): True if overwriting existing county variable in network.  Default to False.

        Returns:
            None
        """

        if network_variable in self.links_df:
            if overwrite:
                WranglerLogger.info(
                    "Overwriting existing hov Variable '{}' already in network".format(
                        network_variable
                    )
                )
            else:
                WranglerLogger.info(
                    "'use' Variable '{}' already in network. Returning without overwriting.".format(
                        network_variable
                    )
                )
                return

        WranglerLogger.info(
            "Calculating hov and adding as roadway network variable: {}".format(
                network_variable
            )
        )
        """
        Verify inputs
        """

        if not network_variable:
            msg = "No network variable specified for centroid connector"
            WranglerLogger.error(msg)
            raise ValueError(msg)

        """
        Start actual process
        """
        #MTC
        self.links_df[network_variable] = int(1)
        #/MTC

        self.links_df[network_variable] = 0

        self.links_df.loc[
            (self.links_df["assign_group"] == 8) | (self.links_df["access"] == "hov"),
            network_variable,
        ] = 100
        #/MC


        if as_integer:
            self.links_df[network_variable] = self.links_df[network_variable].astype(
                int
            )
        WranglerLogger.info(
            "Finished calculating hov variable: {}".format(network_variable)
        )

    def create_ML_variable(
        self,
        network_variable="ML_lanes",
        overwrite=False,
    ):
        """
        Created ML lanes placeholder for project to write out ML changes

        ML lanes default to 0, ML info comes from cube LOG file and store in project cards

        Args:
            overwrite (Bool): True if overwriting existing variable in network.  Default to False.

        Returns:
            None
        """
        if network_variable in self.links_df:
            if overwrite:
                WranglerLogger.info(
                    "Overwriting existing ML Variable '{}' already in network".format(
                        network_variable
                    )
                )
                self.links_df[network_variable] = int(0)
            else:
                WranglerLogger.info(
                    "ML Variable '{}' already in network. Returning without overwriting.".format(
                        network_variable
                    )
                )
                return

        """
        Verify inputs
        """

        WranglerLogger.info(
            "Finished creating ML lanes variable: {}".format(network_variable)
        )

    def create_hov_corridor_variable(
        self,
        network_variable="segment_id",
        overwrite=False,
    ):
        """
        Created hov corridor placeholder for project to write out corridor changes

        hov corridor id default to 0, its info comes from cube LOG file and store in project cards

        Args:
            overwrite (Bool): True if overwriting existing variable in network.  Default to False.

        Returns:
            None
        """
        if network_variable in self.links_df:
            if overwrite:
                WranglerLogger.info(
                    "Overwriting existing hov corridor Variable '{}' already in network".format(
                        network_variable
                    )
                )
            else:
                WranglerLogger.info(
                    "Hov corridor Variable '{}' already in network. Returning without overwriting.".format(
                        network_variable
                    )
                )
                return

        """
        Verify inputs
        """

        self.links_df[network_variable] = int(0)

        WranglerLogger.info(
            "Finished creating hov corridor variable: {}".format(network_variable)
        )

    def create_managed_variable(
        self,
        network_variable="managed",
        overwrite=False,
    ):
        """
        Created placeholder for project to write out managed

        managed default to 0, its info comes from cube LOG file and store in project cards

        Args:
            overwrite (Bool): True if overwriting existing variable in network.  Default to False.

        Returns:
            None
        """
        if network_variable in self.links_df:
            if overwrite:
                WranglerLogger.info(
                    "Overwriting existing managed Variable '{}' already in network".format(
                        network_variable
                    )
                )
            else:
                WranglerLogger.info(
                    "Managed Variable '{}' already in network. Returning without overwriting.".format(
                        network_variable
                    )
                )
                return

        """
        Verify inputs
        """

        self.links_df[network_variable] = int(0)

        WranglerLogger.info(
            "Finished creating managed variable: {}".format(network_variable)
        )

    def calculate_centroidconnect(
        self,
        parameters,
        network_variable="centroidconnect",
        highest_taz_number=None,
        as_integer=True,
        overwrite=False,
    ):
        """
        Calculates centroid connector variable.

        Args:
            parameters (Parameters): A Lasso Parameters, which stores input files.
            network_variable (str): Variable that should be written to in the network. Default to "centroidconnect"
            highest_taz_number (int): the max TAZ number in the network.
            as_integer (bool): If True, will convert true/false to 1/0s.  Default to True.
            overwrite (Bool): True if overwriting existing county variable in network.  Default to False.
        Returns:
            RoadwayNetwork
        """

        if network_variable in self.links_df:
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

        """
        Start actual process
        """
        self.links_df[network_variable] = False

        self.links_df.loc[
            (self.links_df["A"] <= highest_taz_number)
            | (self.links_df["B"] <= highest_taz_number),
            network_variable,
        ] = True

        if as_integer:
            self.links_df[network_variable] = self.links_df[
                network_variable
            ].astype(int)
        WranglerLogger.info(
            "Finished calculating centroid connector variable: {}".format(network_variable)
        )


    def calculate_distance(
        self, network_variable="distance", centroidconnect_only=False, overwrite=False
    ):
        """
        calculate link distance in miles

        Args:
            centroidconnect_only (Bool):  True if calculating distance for centroidconnectors only.  Default to False.
            overwrite (Bool): True if overwriting existing variable in network.  Default to False.

        Returns:
            None

        """

        if network_variable in self.links_df:
            if overwrite:
                WranglerLogger.info(
                    "Overwriting existing distance Variable '{}' already in network".format(
                        network_variable
                    )
                )
            else:
                WranglerLogger.info(
                    "Distance Variable '{}' already in network. Returning without overwriting.".format(
                        network_variable
                    )
                )
                return

        """
        Verify inputs
        """

        #MC
        if ("centroidconnect" not in self.links_df) & ("taz" not in self.links_df.roadway.unique()):
            if centroidconnect_only:
                msg = "No variable specified for centroid connector, calculating centroidconnect first"
                WranglerLogger.error(msg)
                raise ValueError(msg)
        #/MC

        """
        Start actual process
        """

        temp_links_gdf = self.links_df.copy()
        temp_links_gdf.crs = "EPSG:4326"
        temp_links_gdf = temp_links_gdf.to_crs(epsg=26915)

        #MTC
        WranglerLogger.info(
            "Calculating distance for all links".format(network_variable)
        )
        temp_links_gdf[network_variable] = temp_links_gdf.geometry.length / 1609.34
        #/MTC
        #MC
        if centroidconnect_only:
            WranglerLogger.info(
                "Calculating {} for centroid connectors".format(network_variable)
            )
            temp_links_gdf[network_variable] = np.where(
                temp_links_gdf.centroidconnect == 1,
                temp_links_gdf.geometry.length / 1609.34,
                temp_links_gdf[network_variable],
            )
        else:
            WranglerLogger.info(
                "Calculating distance for all links".format(network_variable)
            )
            temp_links_gdf[network_variable] = temp_links_gdf.geometry.length / 1609.34
        #/MC

        self.links_df[network_variable] = temp_links_gdf[network_variable]

    def convert_int(self, int_col_names=[]):
        """
        Convert integer columns
        """

        #MTC
        WranglerLogger.info(
            "Converting variable type to mtc standard"
        )

        int_col_names = self.parameters.int_col
        #/MTC
        #MC
        """
        WranglerLogger.info("Converting variable type to MetCouncil standard")

        if not int_col_names:
            int_col_names = self.parameters.int_col
        #/MC
        """
        ##Why are we doing this?
        # int_col_names.remove("lanes")

        for c in list(set(self.links_df.columns) & set(int_col_names)):
            self.links_df[c] = self.links_df[c].replace(np.nan, 0)
            # REPLACE BLANKS WITH ZERO FOR INTEGER COLUMNS
            self.links_df[c] = self.links_df[c].replace('', 0)
            try:
                self.links_df[c] = self.links_df[c].replace(np.nan, 0)
                self.links_df[c] = self.links_df[c].replace("", 0)
                self.links_df[c] = self.links_df[c].astype(int)
            except ValueError:
                try:
                    self.links_df[c] = self.links_df[c].astype(float)
                    self.links_df[c] = self.links_df[c].astype(int)
                except:
                    msg = f"Could not convert column {c} to integer."
                    WranglerLogger.error(msg)
                    raise ValueError(msg)
            except:
                self.links_df[c] = self.links_df[c].astype(float)
                self.links_df[c] = self.links_df[c].astype(int)

        for c in list(set(self.nodes_df.columns) & set(int_col_names)):
            self.nodes_df[c] = self.nodes_df[c].replace("", 0)
            self.nodes_df[c] = self.nodes_df[c].astype(int)

    def fill_na(self):
        """
        Fill na values from create_managed_lane_network()
        """

        WranglerLogger.info("Filling nan for network from network wrangler")

        num_col = self.parameters.int_col + self.parameters.float_col

        for x in list(self.links_df.columns):
            if x in num_col:
                self.links_df[x].fillna(0, inplace=True)
                self.links_df[x] = self.links_df[x].apply(
                    lambda k: 0 if k in [np.nan, "", float("nan"), "NaN"] else k
                )

            else:
                self.links_df[x].fillna("", inplace=True)

        for x in list(self.nodes_df.columns):
            if x in num_col:
                self.nodes_df[x].fillna(0, inplace=True)
            else:
                self.nodes_df[x].fillna("", inplace=True)


    def roadway_standard_to_met_council_network(self, output_epsg=None):
        """
        Rename and format roadway attributes to be consistent with what metcouncil's model is expecting.
        #MC
        Args:
            output_epsg (int): epsg number of output network.

        Returns:
            None
        """

        WranglerLogger.info(
            "Renaming roadway attributes to be consistent with what metcouncil's model is expecting"
        )

        """
        Verify inputs
        """

        output_epsg = output_epsg if output_epsg else self.parameters.output_epsg

        """
        Start actual process
        """
        if "managed" in self.links_df.columns:
            WranglerLogger.info("Creating managed lane network.")
            self.create_managed_lane_network(in_place=True)

            # when ML and assign_group projects are applied together, assign_group is filled as "" by wrangler for ML links
            for c in ModelRoadwayNetwork.CALCULATED_VALUES:
                if c in self.links_df.columns and c in self.parameters.int_col:
                    self.links_df[c] = self.links_df[c].replace("", 0)
        else:
            WranglerLogger.info("Didn't detect managed lanes in network.")

        self.calculate_centroidconnect(self.parameters)
        self.create_calculated_variables()
        self.calculate_distance(overwrite=True)

        self.fill_na()
        # no method to calculate price yet, will be hard coded in project card
        WranglerLogger.info("Splitting variables by time period and category")
        self.split_properties_by_time_period_and_category()
        self.convert_int()

        self.links_metcouncil_df = self.links_df.copy()
        self.nodes_metcouncil_df = self.nodes_df.copy()

        self.links_metcouncil_df = pd.merge(
            self.links_metcouncil_df.drop(
                "geometry", axis=1
            ),  # drop the stick geometry in links_df
            self.shapes_df[["shape_id", "geometry"]],
            how="left",
            on="shape_id",
        )

        self.links_metcouncil_df.crs = "EPSG:4326"
        self.nodes_metcouncil_df.crs = "EPSG:4326"
        WranglerLogger.info("Setting Coordinate Reference System to EPSG 26915")
        self.links_metcouncil_df = self.links_metcouncil_df.to_crs(epsg=26915)
        self.nodes_metcouncil_df = self.nodes_metcouncil_df.to_crs(epsg=26915)

        self.nodes_metcouncil_df["X"] = self.nodes_metcouncil_df.geometry.apply(
            lambda g: g.x
        )
        self.nodes_metcouncil_df["Y"] = self.nodes_metcouncil_df.geometry.apply(
            lambda g: g.y
        )

        # CUBE expect node id to be N
        self.nodes_metcouncil_df.rename(columns={"model_node_id": "N"}, inplace=True)

    def rename_variables_for_dbf(
        self,
        input_df,
        variable_crosswalk: str = None,
        output_variables: list = None,
        convert_geometry_to_xy=False,
    ):
        """
        Rename attributes for DBF/SHP, make sure length within 10 chars.

        Args:
            input_df (dataframe): Network standard DataFrame.
            variable_crosswalk (str): File path to variable name crosswalk from network standard to DBF names.
            output_variables (list): List of strings for DBF variables.
            convert_geometry_to_xy (bool): True if converting node geometry to X/Y

        Returns:
            dataframe

        """
        WranglerLogger.info("Renaming variables so that they are DBF-safe")

        """
        Verify inputs
        """

        variable_crosswalk = (
            variable_crosswalk
            if variable_crosswalk
            else self.parameters.net_to_dbf_crosswalk
        )

        output_variables = (
            output_variables if output_variables else self.parameters.output_variables
        )

        """
        Start actual process
        """

        crosswalk_df = pd.read_csv(variable_crosswalk)
        WranglerLogger.debug(
            "Variable crosswalk: {} \n {}".format(variable_crosswalk, crosswalk_df)
        )
        net_to_dbf_dict = dict(zip(crosswalk_df["net"], crosswalk_df["dbf"]))

        dbf_name_list = []

        dbf_df = copy.deepcopy(input_df)

        # only write out variables that we specify
        # if variable is specified in the crosswalk, rename it to that variable
        for c in dbf_df.columns:
            if c in output_variables:
                try:
                    dbf_df.rename(columns={c: net_to_dbf_dict[c]}, inplace=True)
                    dbf_name_list += [net_to_dbf_dict[c]]
                except:
                    dbf_name_list += [c]

        if "geometry" in dbf_df.columns:
            if str(dbf_df["geometry"].iloc[0].geom_type) == "Point":
                dbf_df["X"] = dbf_df.geometry.apply(lambda g: g.x)
                dbf_df["Y"] = dbf_df.geometry.apply(lambda g: g.y)
                dbf_name_list += ["X", "Y"]

        WranglerLogger.debug("DBF Variables: {}".format(",".join(dbf_name_list)))

        return dbf_df[dbf_name_list]

    def write_roadway_as_shp(
        self,
        output_dir,
        node_output_variables: list = None,
        link_output_variables: list = None,
        data_to_csv: bool = True,
        data_to_dbf: bool = False,
        output_link_shp: str = None,
        output_node_shp: str = None,
        output_link_csv: str = None,
        output_node_csv: str = None,
        output_gpkg: str = None,
        output_link_gpkg_layer: str = None,
        output_node_gpkg_layer: str = None,
        output_gpkg_link_filter: str = None
    ):
        """
        Write out dbf/shp/gpkg for cube.  Write out csv in addition to shp with full length variable names.

        Args:
            output_dir (str): File path to directory
            node_output_variables (list): List of strings for node output variables.
            link_output_variables (list): List of strings for link output variables.
            data_to_csv (bool): True if write network in csv format.
            data_to_dbf (bool): True if write network in dbf/shp format.
            output_link_shp (str): File name to output link dbf/shp.
            output_node_shp (str): File name of output node dbf/shp.
            output_link_csv (str): File name to output link csv.
            output_node_csv (str): File name to output node csv.
            output_gpkg (str): File name to output GeoPackage.
            output_link_gpkg_layer (str): Layer name within output_gpkg to output links.
            output_node_gpkg_layer (str): Layer name within output_gpkg to output links.
            output_gpkg_link_filter (str): Optional column name to additional output link subset layers

        Returns:
            None
        """

        WranglerLogger.info("Writing Network as Shapefile")
        WranglerLogger.debug(
            "Output Variables: \n - {}".format(
                "\n - ".join(self.parameters.output_variables)
            )
        )

        """
        Verify inputs
        """

        if self.nodes_mtc_df is None:
            self.roadway_standard_to_met_council_network()

        WranglerLogger.debug(
            "Network Link Variables: \n - {}".format(
                "\n - ".join(self.links_mtc_df.columns)
            )
        )
        WranglerLogger.debug(
            "Network Node Variables: \n - {}".format(
                "\n - ".join(self.nodes_mtc_df.columns)
            )
        )

        link_output_variables = (
            link_output_variables
            if link_output_variables
            else [
                c
                for c in self.links_mtc_df.columns
                if c in self.parameters.output_variables
            ]
        )

        node_output_variables = (
            node_output_variables
            if node_output_variables
            else [
                c
                for c in self.nodes_mtc_df.columns
                if c in self.parameters.output_variables
            ]
        )

        # unless specified that all the data goes to the DBF, only output A and B
        dbf_link_output_variables = (
            #MTC
            link_output_variables if link_output_variables else ["A", "B", "geometry"]
            #MC
            #link_output_variables if data_to_dbf else ["A", "B", "shape_id", "geometry"]
        )

        # Removing code to set this to versions from parameters
        # User can use these as arg

        """
        Start Process
        """
        # rename these to short only for shapefile option
        if output_node_shp:
            WranglerLogger.info("Renaming DBF Node Variables")
            nodes_dbf_df = self.rename_variables_for_dbf(self.nodes_mtc_df, output_variables=node_output_variables)
        else:
            WranglerLogger.debug("nodes_mtc_df columns: {}".format(list(self.nodes_mtc_df.columns)))
            nodes_dbf_df = self.nodes_mtc_df[node_output_variables]
        
        if output_link_shp:
            WranglerLogger.info("Renaming DBF Link Variables")
            links_dbf_df = self.rename_variables_for_dbf(self.links_mtc_df, output_variables=dbf_link_output_variables)
        else:
            WranglerLogger.debug("links_mtc_df columns: {}".format(list(self.links_mtc_df.columns)))
            links_dbf_df = self.links_mtc_df[dbf_link_output_variables]

        links_dbf_df = gpd.GeoDataFrame(links_dbf_df, geometry=links_dbf_df["geometry"])

        # temp debug
        WranglerLogger.debug("links_dbf_df.loc[(links_dbf_df.A.isin([7063066,2563066]))|(links_dbf_df.B.isin([7063066,2563066]))]:\n{}".format(
            links_dbf_df.loc[(links_dbf_df.A.isin([7063066,2563066]))|(links_dbf_df.B.isin([7063066,2563066]))]
        ))

        if output_node_shp:
            WranglerLogger.info("Writing Node Shapes: {}".format(os.path.join(output_dir, output_node_shp)))
            nodes_dbf_df.to_file(os.path.join(output_dir, output_node_shp))

        if output_gpkg and output_node_gpkg_layer:
            WranglerLogger.info("Writing GeoPackage {} with Node Layer {}".format(os.path.join(output_dir, output_gpkg), output_node_gpkg_layer))
            nodes_dbf_df.to_file(os.path.join(output_dir, output_gpkg), layer=output_node_gpkg_layer, driver="GPKG")

        if output_link_shp:
            WranglerLogger.info("Writing Link Shapes: {}".format(os.path.join(output_dir, output_link_shp)))
            links_dbf_df.to_file(os.path.join(output_dir, output_link_shp))

        # debug test
        link_schema = {
            "properties": {
                "A"             : "int:8",
                "B"             : "int:8",
                "model_link_id" : "int:10",
                "shstGeometryId": "str:32",
                "name"          : "str:84",
                "ft"            : "int:2",
                "assignable"    : "int:18",
                "cntype"        : "str:80",
                "distance"      : "float",
                "county"        : "str:15",
                "bike_access"   : "int:2",
                "drive_access"  : "int:2",
                "walk_access"   : "int:2",
                "rail_only"     : "int:2",
                "bus_only"      : "int:2",
                "transit"       : "int:2",
                "managed"       : "int:2",
                "tollbooth"     : "int:2",
                "tollseg"       : "int:2",
                "segment_id"    : "int:4",
                "lanes_EA"      : "int:2",
                "lanes_AM"      : "int:2",
                "lanes_MD"      : "int:2",
                "lanes_PM"      : "int:2",
                "lanes_EV"      : "int:2",
                "useclass_EA"   : "int:2",
                "useclass_AM"   : "int:2",
                "useclass_MD"   : "int:2",
                "useclass_PM"   : "int:2",
                "useclass_EV"   : "int:2",
                "nmt2010"       : "int:2",
                "nmt2020"       : "int:2",
            },
            "geometry": "LineString"
        }
        if output_gpkg and output_link_gpkg_layer:
            WranglerLogger.info("Writing GeoPackage {} with Link Layer {}".format(os.path.join(output_dir, output_gpkg), output_link_gpkg_layer))
            links_dbf_df.to_file(os.path.join(output_dir, output_gpkg), layer=output_link_gpkg_layer, schema=link_schema, driver="GPKG")

            # output additional link layers if filter column is specified
            # e.g. if county-subsets are output
            if output_gpkg_link_filter:
                link_value_counts = links_dbf_df[output_gpkg_link_filter].value_counts()
                for filter_val,filter_count in link_value_counts.items():
                    gpkg_layer_name = "{}_{}".format(output_link_gpkg_layer, filter_val)
                    gpkg_layer_name = gpkg_layer_name.replace(" ","_")
                    WranglerLogger.info("Writing GeoPackage {} with Link Layer {} for {} rows".format(
                        os.path.join(output_dir, output_gpkg), gpkg_layer_name, filter_count))
                    links_dbf_df.loc[ links_dbf_df[output_gpkg_link_filter]==filter_val ].to_file(
                        os.path.join(output_dir, output_gpkg), layer=gpkg_layer_name, schema=link_schema, driver="GPKG")




        if data_to_csv:
            WranglerLogger.info(
                "Writing Network Data to CSVs:\n - {}\n - {}".format(
                    output_link_csv, output_node_csv
                )
            )
            self.links_mtc_df[link_output_variables].to_csv(
                output_link_csv, index=False
            )
            self.nodes_mtc_df[node_output_variables].to_csv(
                output_node_csv, index=False
            )


    # this should be moved to util
    @staticmethod
    def dataframe_to_fixed_width(df):
        """
        Convert dataframe to fixed width format, geometry column will not be transformed.

        Args:
            df (pandas DataFrame).

        Returns:
            pandas dataframe:  dataframe with fixed width for each column.
            dict: dictionary with columns names as keys, column width as values.
        """
        WranglerLogger.info("Starting fixed width conversion")
        if 'name' in df.columns:
            df['name']=df['name'].apply(lambda x: x.strip().split(',')[0].replace("[",'').replace("'nan'","").replace("nan","").replace("'",""))
                                                                                                                                     

        # get the max length for each variable column
        max_width_dict = dict(
            [
                (v, df[v].apply(lambda r: len(str(r)) if r != None else 0).max())
                for v in df.columns.values
                if v != "geometry"
            ]
        )

        fw_df = df.drop("geometry", axis=1).copy()
        for c in fw_df.columns:
            fw_df[c] = fw_df[c].apply(lambda x: str(x))
            fw_df["pad"] = fw_df[c].apply(lambda x: " " * (max_width_dict[c] - len(x)))
            fw_df[c] = fw_df.apply(lambda x: x["pad"] + x[c], axis=1)

        return fw_df, max_width_dict

    def write_roadway_as_fixedwidth(
        self,
        output_dir,
        node_output_variables: list = None,
        link_output_variables: list = None,
        output_link_txt: str = None,
        output_node_txt: str = None,
        output_link_header_width_txt: str = None,
        output_node_header_width_txt: str = None,
        output_cube_network_script: str = None,
        drive_only: bool = False,
    ):
        """
        Writes out fixed width file.

        This function does:
        1. write out link and node fixed width data files for cube.
        2. write out header and width correspondence.
        3. write out cube network building script with header and width specification.

        Args:
            output_dir (str): File path to where links, nodes and script will be written and run
            node_output_variables (list): list of node variable names.
            link_output_variables (list): list of link variable names.
            output_link_txt (str): File name of output link database (within output_dir)
            output_node_txt (str): File name of output node database (within output_dir)
            output_link_header_width_txt (str): File name of link column width records (within output_dir)
            output_node_header_width_txt (str): File name of node column width records (within output_dir)
            output_cube_network_script (str): File name of CUBE network building script (within output_dir)
            drive_only (bool): If True, only writes drive nodes and links

        Returns:
            None

        """

        """
        Verify inputs
        """

        if self.nodes_mtc_df is None:
            self.roadway_standard_to_mtc_network()

        WranglerLogger.debug(
            "Network Link Variables: \n - {}".format(
                "\n - ".join(self.links_mtc_df.columns)
            )
        )
        WranglerLogger.debug(
            "Network Node Variables: \n - {}".format(
                "\n - ".join(self.nodes_mtc_df.columns)
            )
        )

        link_output_variables = (
            link_output_variables
            if link_output_variables
            else [
                c
                for c in self.links_mtc_df.columns
                if c in self.parameters.output_variables
            ]
        )

        node_output_variables = (
            node_output_variables
            if node_output_variables
            else [
                c
                for c in self.nodes_mtc_df.columns
                if c in self.parameters.output_variables
            ]
        )

        output_link_txt = (
            output_link_txt if output_link_txt else self.parameters.output_link_txt
        )

        output_node_txt = (
            output_node_txt if output_node_txt else self.parameters.output_node_txt
        )

        output_link_header_width_txt = (
            output_link_header_width_txt
            if output_link_header_width_txt
            else self.parameters.output_link_header_width_txt
        )

        output_node_header_width_txt = (
            output_node_header_width_txt
            if output_node_header_width_txt
            else self.parameters.output_node_header_width_txt
        )

        output_cube_network_script = (
            output_cube_network_script
            if output_cube_network_script
            else self.parameters.output_cube_network_script
        )

        """
        Start Process
        """
        #MTC
        link_ff_df, link_max_width_dict = self.dataframe_to_fixed_width(
            self.links_mtc_df[link_output_variables]
        )

        if drive_only:
            link_ff_df = link_ff_df.loc[link_ff_df['drive_access'] == 1]
        #/MTC
        """
        #MC
        link_ff_df, link_max_width_dict = self.dataframe_to_fixed_width(
            self.links_metcouncil_df[link_output_variables]
        )

        if drive_only:
            link_ff_df = link_ff_df.loc[link_ff_df["drive_access"] == 1]
        #/MC
        """
        WranglerLogger.info("Writing out link database")

        link_ff_df.to_csv(os.path.join(output_dir, output_link_txt), sep=";", index=False, header=False)

        # write out header and width correspondence
        WranglerLogger.info("Writing out link header and width ----")
        link_max_width_df = DataFrame(
            list(link_max_width_dict.items()), columns=["header", "width"]
        )
        link_max_width_df.to_csv(os.path.join(output_dir, output_link_header_width_txt), index=False)

        #MTC
        node_ff_df, node_max_width_dict = self.dataframe_to_fixed_width(
            self.nodes_mtc_df[node_output_variables]
        )
        #/MTC
        """
        #MC
        node_ff_df, node_max_width_dict = self.dataframe_to_fixed_width(
            self.nodes_metcouncil_df[node_output_variables]
        )
        #/MC
        """
        WranglerLogger.info("Writing out node database")

        if drive_only:
            node_ff_df = node_ff_df.loc[node_ff_df["drive_node"] == 1]


        node_ff_df.to_csv(os.path.join(output_dir, output_node_txt), sep=";", index=False, header=False)

        # write out header and width correspondence
        WranglerLogger.info("Writing out node header and width")
        node_max_width_df = DataFrame(
            list(node_max_width_dict.items()), columns=["header", "width"]
        )
        node_max_width_df.to_csv(os.path.join(output_dir, output_node_header_width_txt), index=False)

        # write out cube script
        s = 'RUN PGM = NETWORK MSG = "Read in network from fixed width file" \n'
        s += 'FILEI LINKI[1] = "{}",'.format(output_link_txt)
        start_pos = 1
        for i in range(len(link_max_width_df)):
            s += " VAR=" + link_max_width_df.header.iloc[i]

            if (
                self.links_mtc_df.dtypes.loc[link_max_width_df.header.iloc[i]]
                == "O"
            ):
                s += "(C" + str(link_max_width_df.width.iloc[i]) + ")"

            s += (
                ", BEG="
                + str(start_pos)
                + ", LEN="
                + str(link_max_width_df.width.iloc[i])
                + ","
            )

            start_pos += link_max_width_df.width.iloc[i] + 1

        s = s[:-1]
        s += "\n"
        s += 'FILEI NODEI[1] = "{}",'.format(output_node_txt)
        start_pos = 1
        for i in range(len(node_max_width_df)):
            s += " VAR=" + node_max_width_df.header.iloc[i]

            if (
                self.nodes_mtc_df.dtypes.loc[node_max_width_df.header.iloc[i]]
                == "O"
            ):
                s += "(C" + str(node_max_width_df.width.iloc[i]) + ")"

            s += (
                ", BEG="
                + str(start_pos)
                + ", LEN="
                + str(node_max_width_df.width.iloc[i])
                + ","
            )

            start_pos += node_max_width_df.width.iloc[i] + 1

        s = s[:-1]
        s += '\n'
        s += 'FILEO NETO = "complete_network.net"\n\n'
        s += ' ZONES = {}\n\n'.format(self.parameters.zones)
        s += '; Trim leading whitespace from string variables\n'
        # todo: The below should be built above based on columns that are strings
        s += ' phase=NODEMERGE\n'
        s += '  county = LTRIM(county)\n'
        s += ' endphase\n'
        s += ' phase=LINKMERGE\n'
        s += '  name = LTRIM(name)\n'
        s += '  county = LTRIM(county)\n'
        s += '  cntype = LTRIM(cntype)\n'
        s += ' endphase\n'
        s += '\nENDRUN\n'

        with open(os.path.join(output_dir, output_cube_network_script), "w") as f:
            f.write(s)
 
        # run the cube script to create the cube network
        import subprocess
        env = copy.copy(os.environ)
        cube_cmd = '"C:\\Program Files\\Citilabs\\CubeVoyager\\runtpp.exe" {}'.format(output_cube_network_script)
        try:
            WranglerLogger.info("Running [{}] in cwd [{}]".format(cube_cmd, output_dir))
            ret = subprocess.run(cube_cmd, cwd=output_dir, capture_output=True, check=True)

            WranglerLogger.info("return code: {}".format(ret.returncode))

            for line in ret.stdout.decode('utf-8').split('\r\n'):
                if len(line) > 0: WranglerLogger.info("stdout: {}".format(line))

            for line in ret.stderr.decode('utf-8').split('\r\n'):
                if len(line) > 0: WranglerLogger.info("stderr: {}".format(line))
        
        except Exception as e:
            WranglerLogger.error(e)
