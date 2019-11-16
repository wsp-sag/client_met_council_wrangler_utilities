import glob
import os

import geopandas as gpd
import pandas as pd

from geopandas import GeoDataFrame
from pandas import DataFrame

from network_wrangler import RoadwayNetwork
from .Parameters import Parameters
from .Logger import WranglerLogger


class ModelRoadwayNetwork(RoadwayNetwork):
    def __init__(
        self, nodes: GeoDataFrame, links: DataFrame, shapes: GeoDataFrame, parameters={}
    ):
        super().__init__(nodes, links, shapes)

        # will have to change if want to alter them
        self.parameters = Parameters(**parameters)

        ##todo also write to file
        # WranglerLogger.debug("Used PARAMS\n", '\n'.join(['{}: {}'.format(k,v) for k,v in self.parameters.__dict__.items()]))

    @staticmethod
    def read(
        link_file: str,
        node_file: str,
        shape_file: str,
        fast: bool = False,
        parameters={},
    ):
        # road_net =  super().read(link_file, node_file, shape_file, fast=fast)
        road_net = RoadwayNetwork.read(link_file, node_file, shape_file, fast=fast)

        m_road_net = ModelRoadwayNetwork(
            road_net.nodes_df,
            road_net.links_df,
            road_net.shapes_df,
            parameters=parameters,
        )

        return m_road_net

    @staticmethod
    def from_RoadwayNetwork(roadway_network_object, parameters={}):
        return ModelRoadwayNetwork(
            roadway_network_object.nodes_df,
            roadway_network_object.links_df,
            roadway_network_object.shapes_df,
            parameters=parameters,
        )

    def split_properties_by_time_period_and_category(self, properties_to_split=None):
        """
        Splits properties by time period, assuming a variable structure of

        Params
        ------
        properties_to_split: dict
             dictionary of output variable prefix mapped to the source variable and what to stratify it by
             e.g.
             {
                 'transit_priority' : {'v':'transit_priority', 'times_periods':DEFAULT_TIME_PERIOD_TO_TIME },
                 'traveltime_assert' : {'v':'traveltime_assert', 'times_periods':DEFAULT_TIME_PERIOD_TO_TIME },
                 'lanes' : {'v':'lanes', 'times_periods':DEFAULT_TIME_PERIOD_TO_TIME },
                 'price' : {'v':'price', 'times_periods':DEFAULT_TIME_PERIOD_TO_TIME ,'categories': DEFAULT_CATEGORIES},
                 'access' : {'v':'access', 'times_periods':DEFAULT_TIME_PERIOD_TO_TIME},
             }

        """
        import itertools

        if properties_to_split == None:
            properties_to_split = self.parameters.properties_to_split

        for out_var, params in properties_to_split.items():
            if params["v"] not in self.links_df.columns:
                raise ValueError(
                    "Specified variable to split: {} not in network variables: {}".format(
                        params["v"], str(self.links_df.columns)
                    )
                )
            if params.get("time_periods") and params.get("categories"):
                for time_suffix, category_suffix in itertools.product(
                    params["time_periods"], params["categories"]
                ):
                    self.links_df[
                        out_var + "_" + time_suffix + "_" + category_suffix
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
        Params
        -------
        """

        for method in self.parameters.calculated_variables_roadway:
            eval(method)

    def calculate_county(self, network_variable="county"):
        """
        This uses the centroid of the geometry field to determine which county it should be labeled.
        This isn't perfect, but it much quicker than other methods.

        params
        -------

        """

        centroids_gdf = self.links_df.copy()
        centroids_gdf["geometry"] = centroids_gdf["geometry"].centroid

        county_gdf = gpd.read_file(self.parameters.county_shape)
        county_gdf = county_gdf.to_crs(epsg=RoadwayNetwork.EPSG)
        joined_gdf = gpd.sjoin(centroids_gdf, county_gdf, how="left", op="intersects")

        self.links_df[network_variable] = joined_gdf[
            self.parameters.county_variable_shp
        ]

    def calculate_area_type(
        self,
        network_variable="area_type",
        area_type_shape=None,
        area_type_shape_variable=None,
        area_type_codes_dict=None,
    ):
        """
        This uses the centroid of the geometry field to determine which area type it should be labeled.
        PER PRD
        ##TODO docstrings
        params
        -------

        """
        WranglerLogger.info("Calculating Area Type from Spatial Data")

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

        """
        Start actual process
        """
        centroids_gdf = self.links_df.copy()
        centroids_gdf["geometry"] = centroids_gdf["geometry"].centroid

        WranglerLogger.debug("Reading Area Type Shapefile {}".format(area_type_shape))
        area_type_gdf = gpd.read_file(area_type_shape)
        area_type_gdf = area_type_gdf.to_crs(epsg=RoadwayNetwork.EPSG)

        joined_gdf = gpd.sjoin(
            centroids_gdf, area_type_gdf, how="left", op="intersects"
        )

        joined_gdf[area_type_shape_variable] = (
            joined_gdf[area_type_shape_variable]
            .map(area_type_codes_dict)
            .fillna(10)
            .astype(int)
        )

        WranglerLogger.debug("Area Type Codes Used: {}".format(area_type_codes_dict))

        self.links_df[network_variable] = joined_gdf[area_type_shape_variable]

        WranglerLogger.info(
            "Finished Calculating Area Type from Spatial Data into variable: {}".format(
                network_variable
            )
        )

    def calculate_centroid_connector(
        self,
        network_variable="centroid_connector",
        as_integer=True,
        highest_taz_number=None,
    ):
        """
        Params
        ------
        network_variable: str
          variable that should be written to in the network
        as_integer: bool
          if true, will convert true/false to 1/0s
        """
        WranglerLogger.info("Calculating Centroid Connectors")
        """
        Verify inputs
        """
        highest_taz_number = (
            highest_taz_number
            if highest_taz_number
            else self.parameters.highest_taz_number
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
            self.links_df[network_variable] = self.links_df[network_variable].astype(
                int
            )
        WranglerLogger.info(
            "Finished calculating centroid connector variable: {}".format(
                network_variable
            )
        )

    def calculate_mpo(
        self,
        county_network_variable="county",
        network_variable="mpo",
        as_integer=True,
        mpo_counties=None,
    ):
        """
        Params
        ------
        county_variable: string
          name of the variable where the county names are stored.
        network_variable: string
          name of the variable that should be written to
        as_integer: bool
          if true, will convert true/false to 1/0s
        """
        WranglerLogger.info("Calculating MPO")
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

        WranglerLogger.debug("MPO Counties: {}".format(",".join(mpo_counties)))

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

    def calculate_assignment_group(
        self,
        network_variable="assignment_group",
        mrcc_roadway_class_shape=None,
        mrcc_shst_data=None,
        mrcc_roadway_class_variable_shp=None,
        mrcc_assgngrp_dict=None,
        widot_roadway_class_shape=None,
        widot_shst_data=None,
        widot_roadway_class_variable_shp=None,
        widot_assgngrp_dict=None,
        osm_assgngrp_dict=None,
    ):
        """
        join network with mrcc and widot roadway data by shst js matcher returns
        """

        WranglerLogger.info("Calculating Assignment Group")

        """
        Verify inputs
        """

        for varname, var in {
            "mrcc_roadway_class_shape": mrcc_roadway_class_shape,
            "widot_roadway_class_shape": widot_roadway_class_shape,
            "mrcc_shst_data": mrcc_shst_data,
            "widot_shst_data": widot_shst_data,
        }.items():
            var = var if var else self.parameters.__dict__[varname]
            if not var:
                msg = "'{}' not found in method or lasso parameters.".format(varname)
                WranglerLogger.error(msg)
                raise ValueError(msg)
            if not os.path.exists(var):
                msg = "{}' not found at following location: {}.".format(varname, var)
                WranglerLogger.error(msg)
                raise ValueError(msg)

        for varname, var in {
            "mrcc_roadway_class_variable_shp": mrcc_roadway_class_variable_shp,
            "widot_roadway_class_variable_shp": widot_roadway_class_variable_shp,
            "mrcc_assgngrp_dict": mrcc_assgngrp_dict,
            "widot_assgngrp_dict": widot_assgngrp_dict,
            "osm_assgngrp_dict": osm_assgngrp_dict,
            "network_variable": network_variable,
        }.items():
            var = var if var else self.parameters.__dict__[varname]
            if not var:
                msg = "'{}' not found in method or lasso parameters.".format(varname)
                WranglerLogger.error(msg)
                raise ValueError(msg)

        """
        Start actual process
        """

        WranglerLogger.debug("Calculating Centroid Connectors")
        self.calculate_centroid_connector()

        mrcc_gdf = gpd.read_file(mrcc_roadway_class_shape)
        mrcc_gdf["LINK_ID"] = range(1, 1 + len(mrcc_gdf))
        mrcc_shst_ref_df = ModelRoadwayNetwork.read_match_result(mrcc_shst_data)

        widot_gdf = gpd.read_file(widot_roadway_class_shape)
        widot_gdf["LINK_ID"] = range(1, 1 + len(widot_gdf))
        widot_shst_ref_df = ModelRoadwayNetwork.read_match_result(widot_shst_data)

        join_gdf = ModelRoadwayNetwork.get_attribute(
            self.links_df,
            "shstGeometryId",
            mrcc_shst_ref_df,
            mrcc_gdf,
            mrcc_roadway_class_variable_shp,
        )

        join_gdf = ModelRoadwayNetwork.get_attribute(
            join_gdf,
            "shstGeometryId",
            widot_shst_ref_df,
            widot_gdf,
            widot_roadway_class_variable_shp,
        )

        osm_asgngrp_crosswalk_df = pd.read_csv(osm_assgngrp_dict)
        mrcc_asgngrp_crosswalk_df = pd.read_excel(
            mrcc_assgngrp_dict,
            sheet_name="mrcc_ctgy_asgngrp_crosswalk",
            dtype={"ROUTE_SYS": str, "ROUTE_SYS_ref": str, "assignment_group": int},
        )
        widot_asgngrp_crosswak_df = pd.read_csv(widot_assgngrp_dict)

        join_gdf = pd.merge(
            join_gdf,
            osm_asgngrp_crosswalk_df.rename(
                columns={"assignment_group": "assignment_group_osm"}
            ),
            how="left",
            on="roadway",
        )

        print(join_gdf.columns)
        print(mrcc_asgngrp_crosswalk_df.columns)

        join_gdf = pd.merge(
            join_gdf,
            mrcc_asgngrp_crosswalk_df.rename(
                columns={"assignment_group": "assignment_group_mrcc"}
            ),
            how="left",
            on=mrcc_roadway_class_variable_shp,
        )

        join_gdf = pd.merge(
            join_gdf,
            widot_asgngrp_crosswak_df.rename(
                columns={"assignment_group": "assignment_group_widot"}
            ),
            how="left",
            on=widot_roadway_class_variable_shp,
        )

        def _set_asgngrp(x):
            try:
                if x.centroid_connector == 1:
                    return 9
                elif x.assignment_group_mrcc > 0:
                    return int(x.assignment_group_mrcc)
                elif x.assignment_group_widot > 0:
                    return int(x.assignment_group_widot)
                else:
                    return int(x.assignment_group_osm)
            except:
                return 0

        join_gdf[network_variable] = join_gdf.apply(lambda x: _set_asgngrp(x), axis=1)

        self.links_df[network_variable] = join_gdf[network_variable]

        WranglerLogger.info(
            "Finished calculating assignment group variable: {}".format(
                network_variable
            )
        )

    def calculate_roadway_class(
        self, network_variable="roadway_class", roadway_class_dict=None
    ):
        """
        roadway_class is a lookup based on assignment group

        """
        WranglerLogger.info("Calculating Roadway Class")

        """
        Verify inputs
        """
        roadway_class_dict = (
            roadway_class_dict
            if roadway_class_dict
            else self.parameters.roadway_class_dict
        )

        if not roadway_class_dict:
            msg = msg = "'roadway_class_dict' not found in method or lasso parameters."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        """
        Start actual process
        """

        asgngrp_rc_num_crosswalk_df = pd.read_csv(roadway_class_dict)

        join_gdf = pd.merge(
            self.links_df,
            asgngrp_rc_num_crosswalk_df,
            how="left",
            on="assignment_group",
        )

        self.links_df[network_variable] = join_gdf[network_variable]

        WranglerLogger.info(
            "Finished calculating roadway class variable: {}".format(network_variable)
        )

    def add_variable_using_shst_reference(
        self,
        var_shst_csvdata=None,
        shst_csv_variable=None,
        network_variable=None,
        network_var_type=int,
        overwrite_existing = False,
    ):
        """
        join the network with data, via SHST API node match result
        """
        WranglerLogger.info("Adding Variable {} using Shared Streets Reference from {}".format(network_variable, var_shst_csvdata))

        var_shst_df = pd.read_csv(var_shst_csvdata)

        if "shstReferenceId" not in var_shst_df.columns:
            msg = "'shstReferenceId' required but not found in {}".format(var_shst_data)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        join_gdf = pd.merge(
            self.links_df, var_shst_df[["shstReferenceId",shst_csv_variable]], how="left", on="shstReferenceId"
        )

        join_gdf[shst_csv_variable].fillna(0, inplace=True)

        if network_variable in self.links_df.columns and not overwrite_existing:
            join_gdf.loc[join_gdf[network_variable]>0][network_variable] = join_gdf.loc[self.links_df[network_variable]>0][shst_csv_variable]
        else:
            join_gdf[network_variable] = join_gdf[shst_csv_variable].astype(
                network_var_type
            )

        self.links_df[network_variable] = join_gdf[network_variable]

        WranglerLogger.info("Added variable: {} using Shared Streets Reference".format(network_variable))

    def add_counts(
        self,
        mndot_count_shst_data=None,
        widot_count_shst_data=None,
        mndot_count_variable_shp=None,
        widot_count_variable_shp=None,
        network_variable='AADT',
    ):

        """
        join the network with count node data, via SHST API node match result
        """
        WranglerLogger.info("Adding Counts")

        """
        Verify inputs
        """

        mndot_count_shst_data = mndot_count_shst_data if mndot_count_shst_data else self.parameters.mndot_count_shst_data
        widot_count_shst_data = widot_count_shst_data if widot_count_shst_data else self.parameters.widot_count_shst_data
        mndot_count_variable_shp = mndot_count_variable_shp if mndot_count_variable_shp else self.parameters.mndot_count_variable_shp
        widot_count_variable_shp = widot_count_variable_shp if widot_count_variable_shp else self.parameters.widot_count_variable_shp

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
        print('MNDOT3',mndot_count_shst_data )
        #Add Minnesota Counts
        self.add_variable_using_shst_reference(
            var_shst_csvdata=mndot_count_shst_data,
            shst_csv_variable=mndot_count_variable_shp,
            network_variable=network_variable,
            network_var_type=int,
            overwrite_existing = True,
        )

        #Add Wisconsin Counts, but don't overwrite Minnesota
        self.add_variable_using_shst_reference(
            var_shst_csvdata=widot_count_shst_data,
            shst_csv_variable=widot_count_variable_shp,
            network_variable=network_variable,
            network_var_type=int,
            overwrite_existing = False,
        )

        WranglerLogger.info(
            "Finished adding counts variable: {}".format(network_variable)
        )

    @staticmethod
    def read_match_result(path):
        """
        read the shst geojson match returns

        return shst dataframe
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

        join_refId_df = pd.merge(
            links_df,
            source_shst_ref_df[[join_key, "pp_link_id", "score"]].rename(
                columns={"pp_link_id": "source_link_id", "score": "source_score"}
            ),
            how="left",
            on=join_key,
        )

        join_refId_df = pd.merge(
            join_refId_df,
            source_gdf[["LINK_ID", field_name]].rename(
                columns={"LINK_ID": "source_link_id"}
            ),
            how="left",
            on="source_link_id",
        )

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

        return join_refId_df[links_df.columns.tolist() + [field_name]]

    def roadway_standard_to_dbf_for_cube(self):
        """
        rename attributes for dbf
        """

        self.create_calculated_variables()
        self.split_properties_by_time_period_and_category(
            {
                "transit_priority": {
                    "v": "transit_priority",
                    "time_periods": Parameters.DEFAULT_TIME_PERIOD_TO_TIME,
                    #'categories': Parameters.DEFAULT_CATEGORIES
                },
                "traveltime_assert": {
                    "v": "traveltime_assert",
                    "time_periods": Parameters.DEFAULT_TIME_PERIOD_TO_TIME,
                },
                "lanes": {
                    "v": "lanes",
                    "time_periods": Parameters.DEFAULT_TIME_PERIOD_TO_TIME,
                },
            }
        )

        links_dbf_df = self.links_df.copy()
        links_dbf_df = links_dbf_df.to_crs(epsg=26915)

        nodes_dbf_df = self.nodes_df.copy()
        nodes_dbf_df = nodes_dbf_df.to_crs(epsg=26915)

        nodes_dbf_df = nodes_dbf_df.reset_index()
        nodes_dbf_df.rename(columns={"index": "osm_node_id"}, inplace=True)

        crosswalk_df = pd.read_csv(self.parameters.net_to_dbf)
        print(crosswalk_df.info())
        net_to_dbf_dict = dict(zip(crosswalk_df["net"], crosswalk_df["dbf"]))

        links_dbf_name_list = []
        nodes_dbf_name_list = []

        for c in links_dbf_df.columns:
            if c in self.parameters.output_variables:
                try:
                    links_dbf_df.rename(columns={c: net_to_dbf_dict[c]}, inplace=True)
                    links_dbf_name_list += [net_to_dbf_dict[c]]
                except:
                    links_dbf_name_list += [c]

        for c in nodes_dbf_df.columns:
            if c in self.parameters.output_variables:
                try:
                    nodes_dbf_df.rename(columns={c: net_to_dbf_dict[c]}, inplace=True)
                    nodes_dbf_name_list += [net_to_dbf_dict[c]]
                except:
                    nodes_dbf_name_list += [c]
            if c == "geometry":
                nodes_dbf_df["X"] = nodes_dbf_df.geometry.apply(lambda g: g.x)
                nodes_dbf_df["Y"] = nodes_dbf_df.geometry.apply(lambda g: g.y)
                nodes_dbf_name_list += ["X", "Y"]

        return links_dbf_df[links_dbf_name_list], nodes_dbf_df[nodes_dbf_name_list]

    def write_cube_roadway(self):
        """
        write out dbf/shp for cube
        write out csv in addition to shp with full length variable names
        """
        links_dbf_df, nodes_dbf_df = self.roadway_standard_to_dbf_for_cube()

        link_output_variables = [
            c for c in self.links_df if c in self.parameters.output_variables
        ]
        node_output_variables = [
            c for c in self.nodes_df if c in self.parameters.output_variables
        ]

        self.links_df[link_output_variables].to_csv(
            self.parameters.output_link_csv, index=False
        )
        self.nodes_df[node_output_variables].to_csv(
            self.parameters.output_node_csv, index=False
        )

        links_dbf_df.to_file(self.parameters.output_link_shp)
        nodes_dbf_df.to_file(self.parameters.output_node_shp)
