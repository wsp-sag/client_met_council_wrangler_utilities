import os
import pyproj
from .logger import WranglerLogger


from pyproj import CRS


def get_base_dir(lasso_base_dir=os.getcwd()):
    d = lasso_base_dir
    for i in range(3):
        if "metcouncil_data" in os.listdir(d):

            WranglerLogger.info("Lasso base directory set as: {}".format(d))
            return d
        d = os.path.dirname(d)

    msg = "Cannot find Lasso base directory from {}, please input using keyword in parameters: `lasso_base_dir =` ".format(
        lasso_base_dir
    )
    WranglerLogger.error(msg)
    raise (ValueError(msg))


class Parameters:
    """A class representing all the parameters defining the networks
    including time of day, categories, etc.

    Parameters can be set at runtime by initializing a parameters instance
    with a keyword argument setting the attribute.  Parameters that are
    not explicitly set will use default parameters listed in this class.
    .. highlight:: python

    Attr:
        time_period_to_time (dict): Maps time period abbreviations used in
            Cube to time of days used on gtfs and highway network standard
            Default:
            ::
                {
                    "EA": ("3:00", "6:00"),
                    "AM": ("6:00, "10:00"),
                    "MD": ("10:00", "15:00"),
                    "PM": ("15:00", "19:00"),
                    "EV": ("19:00", "3:00"),
                }
        cube_time_periods (dict):  Maps cube time period numbers used in
            transit line files to the time period abbreviations in time_period_to_time
            dictionary.
            Default:
            ::
                {"1": "EA", "2": "AM", "3": "MD", "4": "PM", "5": "EV"}
        categories (dict): Maps demand category abbreviations to a list of
            network categories they are allowed to use.
            Default:
            ::
                {
                    # suffix, source (in order of search)
                    "sov": ["sov", "default"],
                    "hov2": ["hov2", "default", "sov"],
                    "hov3": ["hov3", "hov2", "default", "sov"],
                    "truck": ["trk", "sov", "default"],
                }
        properties_to_split (dict): Dictionary mapping variables in standard
            roadway network to categories and time periods that need to be
            split out in final model network to get variables like LANES_AM.
            Default:
            ::
                {
                    "lanes": {
                        "v": "lanes",
                        "time_periods": self.time_periods_to_time
                    },
                    "ML_lanes": {
                        "v": "ML_lanes",
                        "time_periods": self.time_periods_to_time
                    },
                    "use": {
                        "v": "use",
                        "time_periods": self.time_periods_to_time
                    },
                }

        county_shape (str): File location of shapefile defining counties.
            Default:
            ::
                r"metcouncil_data/county/cb_2017_us_county_5m.shp"

        county_variable_shp (str): Property defining the county n ame in
            the county_shape file.
            Default:
            ::
                NAME
        lanes_lookup_file (str): Lookup table of number of lanes for different data sources.
            Default:
            ::
                r"metcouncil_data/lookups/lanes.csv"
        centroid_connect_lanes (int): Number of lanes for centroid connectors.
            Default:
            ::
                1
        mpo_counties (list): list of county names within MPO boundary.
            Default:
            ::
                [
                    "ANOKA",
                    "DAKOTA",
                    "HENNEPIN",
                    "RAMSEY",
                    "SCOTT",
                    "WASHINGTON",
                    "CARVER",
                ]

        taz_shape (str):
            Default:
            ::
                r"metcouncil_data/TAZ/TAZOfficialWCurrentForecasts.shp"
        taz_data (str):
            Default:
            ::
                ??
        highest_taz_number (int): highest TAZ number in order to define
            centroid connectors.
            Default:
            ::
                3100

        output_variables (list): list of variables to output in final model
            network.
            Default:
            ::
                [
                    "model_link_id",
                    "link_id",
                    "A",
                    "B",
                    "shstGeometryId",
                    "distance",
                    "roadway",
                    "name",
                    "roadway_class",
                    "bike_access",
                    "walk_access",
                    "drive_access",
                    "truck_access",
                    "trn_priority_EA",
                    "trn_priority_AM",
                    "trn_priority_MD",
                    "trn_priority_PM",
                    "trn_priority_EV",
                    "ttime_assert_EA",
                    "ttime_assert_AM",
                    "ttime_assert_MD",
                    "ttime_assert_PM",
                    "ttime_assert_EV",
                    "lanes_EA",
                    "lanes_AM",
                    "lanes_MD",
                    "lanes_PM",
                    "lanes_EV",
                    "price_sov_EA",
                    "price_hov2_EA",
                    "price_hov3_EA",
                    "price_truck_EA",
                    "price_sov_AM",
                    "price_hov2_AM",
                    "price_hov3_AM",
                    "price_truck_AM",
                    "price_sov_MD",
                    "price_hov2_MD",
                    "price_hov3_MD",
                    "price_truck_MD",
                    "price_sov_PM",
                    "price_hov2_PM",
                    "price_hov3_PM",
                    "price_truck_PM",
                    "price_sov_EV",
                    "price_hov2_EV",
                    "price_hov3_EV",
                    "price_truck_EV",
                    "roadway_class_idx",
                    "facility_type",
                    "county",
                    "centroidconnect",
                    "model_node_id",
                    "N",
                    "osm_node_id",
                    "bike_node",
                    "transit_node",
                    "walk_node",
                    "drive_node",
                    "geometry",
                    "X",
                    "Y",
                    "ML_lanes_EA",
                    "ML_lanes_AM",
                    "ML_lanes_MD",
                    "ML_lanes_PM",
                    "ML_lanes_EV",
                    "segment_id",
                    "managed",
                    "bus_only",
                    "rail_only"
                ]

        osm_facility_type_dict (dict): Mapping between OSM Roadway variable
            and facility type. Default:

        area_type_shape (str):   Location of shapefile defining area type.
            Default:
            ::
                r"metcouncil_data/area_type/ThriveMSP2040CommunityDesignation.shp"
        area_type_variable_shp (str): property in area_type_shape with area
            type in it.
            Default:
            ::
                "COMDES2040"
        area_type_code_dict (dict): Mapping of the area_type_variable_shp to
            the area type code used in the MetCouncil cube network.
            Default:
            ::
                {
                    23: 4,  # urban center
                    24: 3,
                    25: 2,
                    35: 2,
                    36: 1,
                    41: 1,
                    51: 1,
                    52: 1,
                    53: 1,
                    60: 1,
                }
        downtown_area_type_shape (str): Location of shapefile defining downtown area type.
            Default:
            ::
                r"metcouncil_data/area_type/downtownzones_TAZ.shp"
        downtown_area_type (int): Area type integer for downtown.
            Default:
            ::
                5
        mrcc_roadway_class_shape (str): Shapefile of MRCC links with a property
            associated with roadway class. Default:
            ::
                r"metcouncil_data/mrcc/trans_mrcc_centerlines.shp"
        mrcc_roadway_class_variable_shp (str): The property in mrcc_roadway_class_shp
            associated with roadway class. Default:
            ::
                "ROUTE_SYS"
        widot_roadway_class_shape (str): Shapefile of Wisconsin links with a property
            associated with roadway class. Default:
            ::
                r"metcouncil_data/Wisconsin_Lanes_Counts_Median/WISLR.shp"
        widot_roadway_class_variable_shp (str):  The property in widot_roadway_class_shape
            associated with roadway class.Default:
            ::
                "RDWY_CTGY_"
        mndot_count_shape (str):  Shapefile of MnDOT links with a property
            associated with counts. Default:
            ::
                r"metcouncil_data/count_mn/AADT_2017_Count_Locations.shp"
        mndot_count_variable_shp (str): The property in mndot_count_shape
            associated with counts. Default:

            ::
                "lookups/osm_highway_facility_type_crosswalk.csv"
        legacy_tm2_attributes (str): CSV file of link attributes by
            shStReferenceId from Legacy TM2 network. Default:
            ::
                "lookups/legacy_tm2_attributes.csv"
        osm_lanes_attributes (str): CSV file of number of lanes by shStReferenceId
            from OSM. Default:
            ::
                "lookups/osm_lanes_attributes.csv"
        tam_tm2_attributes (str): CSV file of link attributes by
            shStReferenceId from TAM TM2 network. Default:
            ::
                "lookups/tam_tm2_attributes.csv"
        tom_tom_attributes (str): CSV file of link attributes by
            shStReferenceId from TomTom network. Default:
            ::
                "lookups/tomtom_attributes.csv"
        sfcta_attributes (str): CSV file of link attributes by
            shStReferenceId from SFCTA network. Default:
            ::
                "lookups/sfcta_attributes.csv"
        output_epsg (int): EPSG type of geographic projection for output
            shapefiles. Default:
            ::
                102646
        output_link_shp (str): Output shapefile for roadway links. Default:
            ::
                r"tests/scratch/links.shp"
        output_node_shp (str):  Output shapefile for roadway nodes. Default:
            ::
                r"tests/scratch/nodes.shp"
        output_link_csv (str):  Output csv for roadway links. Default:
            ::
                r"tests/scratch/links.csv"
        output_node_csv (str): Output csv for roadway nodes. Default:
            ::
                r"tests/scratch/nodes.csv"
        output_link_txt (str): Output fixed format txt for roadway links. Default:
            ::
                r"tests/scratch/links.txt"
        output_node_txt (str): Output fixed format txt for roadway nodes. Default:
            ::
                r"tests/scratch/nodes.txt"
        output_link_header_width_txt (str): Header for txt roadway links. Default:
            ::
                r"tests/scratch/links_header_width.txt"
        output_node_header_width_txt (str): Header for txt for roadway Nodes. Default:
            ::
                r"tests/scratch/nodes_header_width.txt"
        output_cube_network_script (str): Cube script for importing
            fixed-format roadway network. Default:
            ::
                r"tests/scratch/make_complete_network_from_fixed_width_file.s



    """

    def __init__(self, **kwargs):
        """
        Time period and category  splitting info
        """
        if "time_periods_to_time" in kwargs:
            self.time_periods_to_time = kwargs.get("time_periods_to_time")
        else:
            self.time_period_to_time = {
                "EA": ("3:00", "6:00"),
                "AM": ("6:00", "10:00"),
                "MD": ("10:00", "15:00"),
                "PM": ("15:00", "19:00"),
                "EV": ("19:00", "3:00"),
            }

        #MTC
        self.cube_time_periods = {
            "1": "EA",
            "2": "AM",
            "3": "MD",
            "4": "PM",
            "5": "EV",
        }

        """
        #MC
        self.route_type_bus_mode_dict = {"Urb Loc": 5, "Sub Loc": 6, "Express": 7}

        self.route_type_mode_dict = {0: 8, 2: 9}

        self.cube_time_periods = {"1": "AM", "2": "MD"}
        self.cube_time_periods_name = {"AM": "pk", "MD": "op"}
        """
        if "categories" in kwargs:
            self.categories = kwargs.get("categories")
        else:
            self.categories = {
                # suffix, source (in order of search)
                "sov": ["sov", "default"],
                "hov2": ["hov2", "default", "sov"],
                "hov3": ["hov3", "hov2", "default", "sov"],
                "truck": ["trk", "sov", "default"],
            }

        # prefix, source variable, categories
        self.properties_to_split = {
            "lanes": {
                "v": "lanes",
                "time_periods": self.time_period_to_time,
            },
            "ML_lanes": {
                "v": "ML_lanes",
                "time_periods": self.time_period_to_time,
            },
            "useclass": {
                "v": "useclass",
                "time_periods": self.time_period_to_time,
            },
        }

        """
        Details for calculating the county based on the centroid of the link.
        The NAME varible should be the name of a field in shapefile.
        """
        #MTC
        if 'lasso_base_dir' in kwargs:
            self.base_dir = get_base_dir(lasso_base_dir = kwargs.get("lasso_base_dir"))
        else:
            self.base_dir = get_base_dir()

        if 'data_file_location' in kwargs:
            self.data_file_location =  kwargs.get("data_file_location")
        else:
            self.data_file_location = os.path.join(self.base_dir,  "mtc_data")

        #MC
        if "lasso_base_dir" in kwargs:
            self.base_dir = get_base_dir(lasso_base_dir=kwargs.get("lasso_base_dir"))
        else:
            self.base_dir = get_base_dir()
        """
        if "data_file_location" in kwargs:
            self.data_file_location = kwargs.get("data_file_location")
        else:
            self.data_file_location = os.path.join(self.base_dir, "metcouncil_data")
        """

         #--------
        if "settings_location" in kwargs:
            self.settings_location = kwargs.get("settings_location")
        else:
            self.settings_location = os.path.join(self.base_dir, "examples", "settings")

        if "scratch_location" in kwargs:
            self.scratch_location = kwargs.get("scratch_location")
        else:
            self.scratch_location = os.path.join(self.base_dir, "tests", "scratch")

        ### COUNTIES

        self.county_shape = os.path.join(
            self.data_file_location, "county", "county.shp"
        )
        self.county_variable_shp = "NAME"

        #MTC
        self.county_code_dict = {
            'San Francisco':1,
            'San Mateo':2,
            'Santa Clara':3,
            'Alameda':4,
            'Contra Costa':5,
            'Solano':6,
            'Napa':7,
            'Sonoma':8,
            'Marin':9,
            'San Joaquin':11,
            'External':10,
            }

        self.county_centroid_range_dict = {
            'San Francisco':range(1,100000),
            'San Mateo':range(100001,200000),
            'Santa Clara':range(200001,300000),
            'Alameda':range(300001,400000),
            'Contra Costa':range(400001,500000),
            'Solano':range(500001,600000),
            'Napa':range(600001,700000),
            'Sonoma':range(700001,800000),
            'Marin':range(800001,900000),
            'External':range(900001,1000000)
            }

        self.county_node_range_dict = {
            'San Francisco':range(1000000,1500000),
            'San Mateo':range(1500000,2000000),
            'Santa Clara':range(2000000,2500000),
            'Alameda':range(2500000,3000000),
            'Contra Costa':range(3000000,3500000),
            'Solano':range(3500000,4000000),
            'Napa':range(4000000,4500000),
            'Sonoma':range(4500000,5000000),
            'Marin':range(5000000,5500000),
            }

        self.county_hov_node_range_dict = {
            'San Francisco':range(5500000,6000000),
            'San Mateo':range(6000000,6500000),
            'Santa Clara':range(6500000,7000000),
            'Alameda':range(7000000,7500000),
            'Contra Costa':range(7500000,8000000),
            'Solano':range(8000000,8500000),
            'Napa':range(8500000,9000000),
            'Sonoma':range(9000000,9500000),
            'Marin':range(9500000,10000000),
            }

        self.county_link_range_dict = {
            'San Francisco':range(1,1000000),
            'San Mateo':range(1000000,2000000),
            'Santa Clara':range(2000000,3000000),
            'Alameda':range(3000000,4000000),
            'Contra Costa':range(4000000,5000000),
            'Solano':range(5000000,6000000),
            'Napa':range(6000000,7000000),
            'Sonoma':range(7000000,8000000),
            'Marin':range(8000000,9000000)
            }

        #MC
        """
        self.county_code_dict = {
            "Anoka": 1,
            "Carver": 2,
            "Dakota": 3,
            "Hennepin": 4,
            "Ramsey": 5,
            "Scott": 6,
            "Washington": 7,
            "external": 10,
            "Chisago": 11,
            "Goodhue": 12,
            "Isanti": 13,
            "Le Sueur": 14,
            "McLeod": 15,
            "Pierce": 16,
            "Polk": 17,
            "Rice": 18,
            "Sherburne": 19,
            "Sibley": 20,
            "St. Croix": 21,
            "Wright": 22,
        }
        """

        self.mpo_counties = [
            1,
            3,
            4,
            5,
            6,
            7,
            8,
            9
        ]

        self.taz_N_list = list(range(1, 10000)) + list(range(100001, 110000)) + list(range(200001, 210000)) + list(range(300001, 310000))\
        + list(range(400001, 410000)) + list(range(500001, 510000)) + list(range(600001, 610000)) + list(range(700001, 710000))\
        + list(range(800001, 810000)) + list(range(900001, 1000000))

        self.maz_N_list = list(range(10001, 90000)) + list(range(110001, 190000)) + list(range(210001, 290000)) + list(range(310001, 390000))\
        + list(range(410001, 490000)) + list(range(510001, 590000)) + list(range(610001, 690000)) + list(range(710001, 790000))\
        + list(range(810001, 890000))

        self.tap_N_list = list(range(90001, 99999)) + list(range(190001, 199999)) + list(range(290001, 299999)) + list(range(390001, 399999))\
        + list(range(490001, 499999)) + list(range(590001, 599999)) + list(range(690001, 699999)) + list(range(790001, 799999))\
        + list(range(890001, 899999))

        self.tap_N_start = {
            "San Francisco" : 90001,
            "San Mateo" : 190001,
            "Santa Clara" : 290001,
            "Alameda" : 390001,
            "Contra Costa" : 490001,
            "Solano" : 590001,
            "Napa" : 690001,
            "Sonoma" : 790001,
            "Marin" : 890001
        }

        #MTC
        self.osm_facility_type_dict = os.path.join(
            self.data_file_location, "lookups", "osm_highway_facility_type_crosswalk.csv"
        )
        #MC
        ### Lanes
        self.lanes_lookup_file = os.path.join(
            self.data_file_location, "lookups", "lanes.csv"
        )

        ###  TAZS

        self.taz_shape = os.path.join(
            self.data_file_location, "TAZ", "TAZOfficialWCurrentForecasts.shp"
        )
        ######
        #MTC
        self.osm_lanes_attributes = os.path.join(
            self.data_file_location, "lookups", "osm_lanes_attributes.csv"
        )

        self.legacy_tm2_attributes = os.path.join(
            self.data_file_location, "lookups", "legacy_tm2_attributes.csv"
        )

        self.assignable_analysis = os.path.join(
            self.data_file_location, "lookups", "assignable_analysis_links.csv"
        )
        ###
        ### AREA TYPE - MC
        self.area_type_shape = os.path.join(
            self.data_file_location,
            "area_type",
            "ThriveMSP2040CommunityDesignation.shp",
        )
        self.area_type_variable_shp = "COMDES2040"
        # area type map from raw data to model category

        # source https://metrocouncil.org/Planning/Publications-And-Resources/Thrive-MSP-2040-Plan-(1)/7_ThriveMSP2040_LandUsePoliciesbyCD.aspx
        # urban center
        # urban
        # suburban
        # suburban edge
        # emerging suburban edge
        # rural center
        # diversified rural
        # rural residential
        # agricultural
        self.area_type_code_dict = {
            23: 4,  # urban center
            24: 3,
            25: 2,
            35: 2,
            36: 1,
            41: 1,
            51: 1,
            52: 1,
            53: 1,
            60: 1,
        }

        self.downtown_area_type_shape = os.path.join(
            self.data_file_location,
            "area_type",
            "downtownzones_TAZ.shp",
        )

        self.downtown_area_type = int(5)

        self.centroid_connect_lanes = int(1)

        self.osm_assgngrp_dict = os.path.join(
            self.data_file_location, "lookups", "osm_highway_asgngrp_crosswalk.csv"
        )
        self.mrcc_roadway_class_shape = os.path.join(
            self.data_file_location, "mrcc", "trans_mrcc_centerlines.shp"
        )
        ####
        ###MTC
        self.tam_tm2_attributes = os.path.join(
            self.data_file_location, "lookups", "tam_tm2_attributes.csv"
        )

        self.sfcta_attributes = os.path.join(
            self.data_file_location, "lookups", "sfcta_attributes.csv"
        )

        self.tomtom_attributes = os.path.join(
            self.data_file_location, "lookups", "tomtom_attributes.csv"
        )

        self.pems_attributes = os.path.join(
            self.data_file_location, "lookups", "pems_attributes.csv"
        )

        self.centroid_file = os.path.join(
            self.data_file_location, "centroid", "centroid_node.pickle"
        )
        ####
        ###MC
        self.widot_shst_data = os.path.join(
            self.data_file_location,
            "Wisconsin_Lanes_Counts_Median",
            "widot.out.matched.geojson",
        )
        ####

        self.centroid_connector_link_file = os.path.join(
            self.data_file_location, "centroid", "cc_link.pickle"
        )

        self.centroid_connector_shape_file = os.path.join(
            self.data_file_location, "centroid", "cc_shape.pickle"
        )

        self.tap_file = os.path.join(
            self.data_file_location, "tap", "tap_node.pickle"
        )

        self.tap_connector_link_file = os.path.join(
            self.data_file_location, "tap", "tap_link.pickle"
        )

        self.tap_connector_shape_file = os.path.join(
            self.data_file_location, "tap", "tap_shape.pickle"
        )

        self.net_to_dbf_crosswalk = os.path.join(
            self.settings_location, "net_to_dbf.csv"
        )

        ###MTC
        self.log_to_net_crosswalk = os.path.join(self.settings_location, "log_to_net.csv")

        self.emme_name_crosswalk_file = os.path.join(
            self.data_file_location, "lookups", "emme_attribute_names.csv"
        )
        ####
        #MC
        self.mndot_count_variable_shp = "AADT_mn"

        self.widot_county_shape = os.path.join(
            self.data_file_location,
            "Wisconsin_Lanes_Counts_Median",
            "TRADAS_(counts).shp",
        )
        ###
        ###MTC
        self.mode_crosswalk_file = os.path.join(
            self.data_file_location, "lookups", "gtfs_to_tm2_mode_crosswalk.csv"
        )

        self.veh_cap_crosswalk_file = os.path.join(
            self.data_file_location, "lookups", "transitSeatCap.csv"
        )

        self.faresystem_crosswalk_file = os.path.join(
            self.data_file_location, "lookups", "faresystem_crosswalk.txt"
        )

        # https://app.asana.com/0/12291104512575/1200287255197808/f
        self.fare_2015_to_2010_deflator = 0.927
        ####
        #MC
        self.widot_count_variable_shp = "AADT_wi"

        self.net_to_dbf_crosswalk = os.path.join(
            self.settings_location, "net_to_dbf.csv"
        )

        self.log_to_net_crosswalk = os.path.join(
            self.settings_location, "log_to_net.csv"
        )

        self.subregion_boundary_file = os.path.join(
            self.data_file_location, 'emme', 'subregion_boundary_for_active_modes.shp'
        )

        self.subregion_boundary_id_variable = 'subregion'
        ####

        self.output_variables = [
            "model_link_id",
            "link_id",
            "A",
            "B",
            "shstGeometryId",
            #MTC
            'name',
            "distance",
            "roadway",
            #"name",
            #MC
            #"shape_id",
            #"distance",
            #"roadway",
            #"name",
            #"roadway_class",
            ####
            "bike_access",
            "walk_access",
            "drive_access",
            "truck_access",
            "lanes_EA",
            "lanes_AM",
            "lanes_MD",
            "lanes_PM",
            "lanes_EV",
            "county",
            "model_node_id",
            "N",
            "osm_node_id",
            "geometry",
            "X",
            "Y",
            "segment_id",
            "managed",
            "bus_only",
            "rail_only",
            "pnr",      
            #MTC
            "assignable",
            "cntype",
            "useclass_AM",
            "useclass_MD",
            "useclass_PM",
            "useclass_EV",
            "useclass_EA",
            "transit",
            "tollbooth",
            "tollseg",
            "ft",
            "tap_drive",
            "tollbooth",
            "tollseg",
            "farezone",
            "tap_id",
            ####
            #MC
            "bike_facility",
            "mrcc_id",
            "ROUTE_SYS",  # mrcc functional class
            ####
            #bi-county
            "nmt2010",
            "nmt2020",
            "BRT",
            "has_transit"
        ]

        self.output_link_shp = os.path.join(self.scratch_location, "links.shp")
        self.output_node_shp = os.path.join(self.scratch_location, "nodes.shp")
        self.output_link_csv = os.path.join(self.scratch_location, "links.csv")
        self.output_node_csv = os.path.join(self.scratch_location, "nodes.csv")
        self.output_link_txt = os.path.join(self.scratch_location, "links.txt")
        self.output_node_txt = os.path.join(self.scratch_location, "nodes.txt")
        self.output_link_header_width_txt = os.path.join(
            self.scratch_location, "links_header_width.txt"
        )
        self.output_node_header_width_txt = os.path.join(
            self.scratch_location, "nodes_header_width.txt"
        )
        self.output_cube_network_script = os.path.join(
            self.scratch_location, "make_complete_network_from_fixed_width_file.s"
        )
        self.output_dir = os.path.join(self.scratch_location)
        self.output_proj = CRS("ESRI:102646")
        self.output_proj4 = '+proj=lcc +lat_1=32.78333333333333 +lat_2=33.88333333333333 +lat_0=32.16666666666666 +lon_0=-116.25 +x_0=2000000 +y_0=500000.0000000002 +ellps=GRS80 +datum=NAD83 +to_meter=0.3048006096012192 +no_defs'
        self.prj_file = os.path.join(self.data_file_location, 'projection', '102646.prj')
        self.wkt_projection = 'PROJCS["NAD_1983_StatePlane_California_VI_FIPS_0406_Feet",GEOGCS["GCS_North_American_1983",DATUM["North_American_Datum_1983",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["False_Easting",6561666.666666666],PARAMETER["False_Northing",1640416.666666667],PARAMETER["Central_Meridian",-116.25],PARAMETER["Standard_Parallel_1",32.78333333333333],PARAMETER["Standard_Parallel_2",33.88333333333333],PARAMETER["Latitude_Of_Origin",32.16666666666666],UNIT["Foot_US",0.30480060960121924],AUTHORITY["EPSG","102646"]]'

        self.fare_matrix_output_variables = ["faresystem", "origin_farezone", "destination_farezone", "price"]

        self.zones = 6593
        """
        Create all the possible headway variable combinations based on the cube time periods setting
        """
        self.time_period_properties_list = [
            p + "[" + str(t) + "]"
            for p in ["HEADWAY", "FREQ"]
            for t in self.cube_time_periods.keys()
        ]

        self.int_col = [
            "model_link_id",
            "model_node_id",
            "A",
            "B",
            #MTC
            #"county",
            ###
            #MC
            # "lanes",
            "lanes_AM",
            "lanes_MD",
            "lanes_PM",
            "lanes_NT",
            "roadway_class",
            "assign_group",
            #"county",
            "area_type",
            "trn_priority",
            "AADT",
            "count_AM",
            "count_MD",
            "count_PM",
            "count_NT",
            "count_daily",
            "centroidconnect",
            "bike_facility",
            ####
            "drive_access",
            "walk_access",
            "bike_access",
            "truck_access",
            #MTC
            "ML_lanes_AM",
            "ML_lanes_MD",
            "ML_lanes_PM",
            "ML_lanes_EV",
            "ML_lanes_EA",
            ###
            #MC
            "drive_node",
            "walk_node",
            "bike_node",
            "transit_node",
            # "ML_lanes",
            "ML_lanes_AM",
            "ML_lanes_MD",
            "ML_lanes_PM",
            "ML_lanes_NT",
            ####
            "segment_id",
            "managed",
            "bus_only",
            "rail_only",
            "transit",
            ##MTC
            "ft",
            "assignable",
            "lanes_AM",
            "lanes_MD",
            "lanes_PM",
            "lanes_EA",
            "lanes_EV",
            "useclass_AM",
            "useclass_EA",
            "useclass_MD",
            "useclass_PM",
            "useclass_EV",
            "tollseg",
            "tollbooth",
            "farezone",
            "tap_id",
            ####
            #bi-county
            "nmt2010",
            "nmt2020",
            "BRT",
            "has_transit"               
        ]

        self.float_col = [
            "distance",
            "price",
            "X",
            "Y"
            "mrcc_id",
        ]

        self.float_col = ["distance", "ttime_assert", "price", "X", "Y"]

        self.string_col = [
            "osm_node_id",
            "name",
            "pnr",      
            "roadway",
            "shstGeometryId",
            "access_AM",
            "access_MD",
            "access_PM",
            "access_NT",
            "ROUTE_SYS",
        ]

        # pnr parameters
        self.pnr_node_location = os.path.join(
            self.data_file_location, "lookups", "pnr_stations.csv"
        )

        self.drive_buffer =  6

        #self.network_build_crs = CRS("EPSG:2875")
        #self.project_card_crs = CRS("EPSG:4326")
        #self.transformer = pyproj.Transformer.from_crs(
        #    self.network_build_crs, self.project_card_crs, always_xy=True
        #)

        self.__dict__.update(kwargs)
