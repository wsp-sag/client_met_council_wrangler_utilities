import re
import os

import pytest

from lasso import Parameters, ModelRoadwayNetwork, metcouncil
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

@pytest.mark.metcouncil
@pytest.mark.travis
def test_calculate_lanes(request):
    """
    Tests that lanes are computed
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    params = Parameters()

    if "lanes" in net.links_df.columns:
        net.links_df.drop(['lanes'], axis=1)

    l_net = metcouncil.calculate_number_of_lanes(
        roadway_net=net,
        parameters=params,
        overwrite=False,
    )
    assert "lanes" in l_net.links_df.columns
    print("Number of Lanes Frequency for all links")
    print(l_net.links_df.lanes.value_counts())
    ## todo write an assert that actually tests something

@pytest.mark.metcouncil
@pytest.mark.travis
def test_assign_group_roadway_class(request):
    """
    Tests that assign group and roadway class are computed
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    params = Parameters()

    l_net = metcouncil.calculate_assign_group_and_roadway_class(
        roadway_net=net,
        parameters=params,
        overwrite=False,
    )
    assert "assign_group" in l_net.links_df.columns
    assert "roadway_class" in l_net.links_df.columns
    print("Assign Group Frequency for all links")
    print(l_net.links_df.assign_group.value_counts())
    print("Roadway Class Frequency for all links")
    print(l_net.links_df.roadway_class.value_counts())
    ## todo write an assert that actually tests something

@pytest.mark.metcouncil
@pytest.mark.travis
def test_centroidconnect(request):
    """
    Tests that centroid connectors are identified
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    params = Parameters()

    l_net = metcouncil.calculate_centroidconnect(
        roadway_net=net,
        parameters=params,
        overwrite=False,
    )
    assert "centroidconnect" in l_net.links_df.columns
    ## todo write an assert that actually tests something
