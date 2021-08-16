import re
import os

import pytest

from lasso import Parameters, ModelRoadwayNetwork
from network_wrangler import RoadwayNetwork

"""
Run tests from bash/shell
Run just the tests labeled project using `pytest -m roadway`
To run with print statments, use `pytest -s -m roadway`
"""

STPAUL_DIR = os.path.join(os.getcwd(), "examples", "stpaul")

STPAUL_SHAPE_FILE = os.path.join(STPAUL_DIR, "shape.geojson")
STPAUL_LINK_FILE = os.path.join(STPAUL_DIR, "link.json")
STPAUL_NODE_FILE = os.path.join(STPAUL_DIR, "node.geojson")


@pytest.mark.roadway
@pytest.mark.travis
def test_parameter_read(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    params = Parameters()
    print(params.__dict__)
    ## todo write an assert that actually tests something


@pytest.mark.roadway
@pytest.mark.travis
def test_network_calculate_variables(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    net.calculate_county()
    print("County  Frequency")
    print(net.links_df["county"].value_counts())

    net.calculate_mpo()
    print("MPO   Frequency")
    print(net.links_df["mpo"].value_counts())
    ## todo write an assert that actually tests something


@pytest.mark.roadway
@pytest.mark.travis
def test_network_split_variables_by_time(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    net.split_properties_by_time_period_and_category()
    assert "trn_priority_AM" in net.links_df.columns
    print(net.links_df.info())
    ## todo write an assert that actually tests something


@pytest.mark.roadway
@pytest.mark.travis
def test_calculate_area_type(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    model_road_net = ModelRoadwayNetwork.from_RoadwayNetwork(net)

    model_road_net.calculate_area_type()
    assert "area_type" in net.links_df.columns
    print("Area Type  Frequency")
    print(net.links_df.area_type.value_counts())
    ## todo write an assert that actually tests something

@pytest.mark.roadway
@pytest.mark.travis
def test_calculate_assign_group_rdclass(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    net.calculate_assign_group_and_roadway_class()
    assert "assign_group" in net.links_df.columns
    assert "roadway_class" in net.links_df.columns
    print("centroid connector Frequency")
    print(net.links_df.centroidconnect.value_counts())
    print("Assign Group Frequency for all links")
    print(net.links_df.assign_group.value_counts())
    print("Assign Group  Frequency for drive links")
    print(net.links_df[net.links_df.drive_access == 1].assign_group.value_counts())
    print("Roadway Class  Frequency for drive links")
    print(net.links_df[net.links_df.drive_access == 1].roadway_class.value_counts())
    ## todo write an assert that actually tests something

@pytest.mark.roadway
@pytest.mark.travis
def test_calculate_count(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    net.add_counts()
    assert "AADT" in net.links_df.columns
    print(net.links_df[net.links_df.drive_access == 1].AADT.value_counts())
    ## todo write an assert that actually tests something


@pytest.mark.roadway
@pytest.mark.travis
def test_write_cube_roadway(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    net.write_roadway_as_fixedwidth()
    ## todo write an assert that actually tests something


@pytest.mark.roadway
@pytest.mark.travis
def test_write_roadway_as_shape(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    net.write_roadway_as_shp()
    ## todo write an assert that actually tests something
