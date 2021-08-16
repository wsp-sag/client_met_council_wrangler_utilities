import os
import glob
import re

import pytest

from lasso import Project
from lasso import CubeTransit
from lasso import StandardTransit


"""
Run tests from bash/shell
Run just the tests labeled project using `pytest -m project`
To run with print statments, use `pytest -s -m project`
"""


CUBE_DIR = os.path.join(os.getcwd(), "examples", "cube")
BASE_TRANSIT_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
BASE_ROADWAY_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
SCRATCH_DIR = os.path.join(os.getcwd(), "tests", "scratch")

logfile_list = glob.glob(os.path.join(CUBE_DIR, "st_paul_test.log"))


@pytest.mark.transit
@pytest.mark.travis
@pytest.mark.basic
def test_parse_transit_linefile(request):
    print("\n--Starting:", request.node.name)
    test_lin = """
    ;;<<PT>><<LINE>>;;
    LINE NAME="0_452-111_452_pk1",
    MODE=5,
    HEADWAY[1]=10,
    NODES=
     39249,
     -39240,
     54648

     LINE NAME="0_134-111_134_pk1",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=20,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      39249,
      -39240,
      54648,
      43503,
      -55786,
      -55785,
      55782,
      -55781,
      -55779

     LINE NAME="0_134-111_134_pk0",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=90,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      83733,
      -9533,
      20208,
      84250,
      92566,
      129190
    """

    tn = CubeTransit.create_from_cube(test_lin)
    print("TYPE", tn)
    ex_line_name = tn.lines[1]
    print("Line: {}".format(ex_line_name))
    print("Properties: ", tn.line_properties[ex_line_name])
    print("Nodes: ", tn.shapes[ex_line_name])


@pytest.mark.transit
@pytest.mark.travis
@pytest.mark.basic
def test_read_transit_linefile(request):
    print("\n--Starting:", request.node.name)

    linefilename = os.path.join(CUBE_DIR, "transit.LIN")
    print("Reading: {}".format(linefilename))
    tn = CubeTransit.create_from_cube(linefilename)
    print("Read {} LINES:\n{}".format(len(tn.lines), "\n - ".join(tn.lines)))
    ex_line_name = tn.lines[1]
    print("Line: {}".format(ex_line_name))
    print("Properties: ", tn.line_properties[ex_line_name])
    print("Nodes: ", tn.shapes[ex_line_name])
    ## todo write an assert that actually tests something


@pytest.mark.travis
@pytest.mark.transit
@pytest.mark.basic
def test_create_cube_transit_network_from_dir(request):
    print("\n--Starting:", request.node.name)
    from lasso import CubeTransit

    tn = CubeTransit.create_from_cube(CUBE_DIR)
    print("READ {} LINES:\n{}".format(len(tn.lines), "\n - ".join(tn.lines)))
    print("Source files: {}".format("\n - ".join(tn.source_list)))
    ## todo write an assert that actually tests something


@pytest.mark.parametrize("logfilename", logfile_list)
@pytest.mark.travis
@pytest.mark.roadway
def test_write_roadway_project_card_from_logfile(request, logfilename):
    print("\n--Starting:", request.node.name)

    test_project = Project.create_project(
        roadway_log_file=logfilename, base_roadway_dir=BASE_ROADWAY_DIR
    )
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_roadway_pc_test.yml"))
    ## todo write an assert that actually tests something


@pytest.mark.travis
@pytest.mark.roadway
@pytest.mark.skip(
    reason="Not currently able to automagically create a managed lane project card"
)
def test_write_ml_roadway_project_card_from_logfile(request):
    print("\n--Starting:", request.node.name)

    test_project = Project.create_project(
        roadway_log_file=os.path.join(CUBE_DIR, "ML_log.log"),
        base_roadway_dir=BASE_ROADWAY_DIR,
    )
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_roadway_pc_test.yml"))
    ## todo write an assert that actually tests something


@pytest.mark.travis
@pytest.mark.transit
def test_write_transit_project_card(request):
    print("\n--Starting:", request.node.name)
    test_lin_base = """
    ;;<<PT>><<LINE>>;;
    LINE NAME="0_452-111_452_pk1",
    MODE=5,
    HEADWAY[1]=10,
    NODES=
     39249,
     -39240,
     54648

     LINE NAME="0_134-111_134_pk1",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=20,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      39249,
      -39240,
      54648,
      43503,
      -55786,
      -55785,
      55782,
      -55781,
      -55779

     LINE NAME="0_134-111_134_pk0",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=90,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      83733,
      -9533,
      20208,
      84250,
      92566,
      129190
    """
    test_lin_build = """
    ;;<<PT>><<LINE>>;;
    LINE NAME="0_452-111_452_pk1",
    MODE=5,
    HEADWAY[1]=15,
    NODES=
     39249,
     -39240,
     54648

     LINE NAME="0_134-111_134_pk1",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=20,
      MODE=6,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      39249,
      -39240,
      54648,
      43503,

     LINE NAME="0_134-111_134_pk0",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=120,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      84250,
      92566,
      129190
    """
    test_project = Project.create_project(
        base_transit_source=test_lin_base,
        build_transit_source=test_lin_build,
        project_name="test suite small changes",
    )
    print("")
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_transit_test.yml"))
    ## todo write an assert that actually tests something


@pytest.mark.travis
@pytest.mark.transit
def test_write_transit_project_card(request):
    print("\n--Starting:", request.node.name)
    test_lin_base = """
    ;;<<PT>><<LINE>>;;
    LINE NAME="0_452-111_452_pk1",
    MODE=5,
    HEADWAY[1]=10,
    NODES=
     39249,
     -39240,
     54648

    LINE NAME="0_134-111_134_pk1",
     LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
     HEADWAY[1]=20,
     MODE=5,
     ONEWAY=T,
     OPERATOR=3,
    NODES=
     39249,
     -39240,
     54648,

    """
    test_lin_build = """
    ;;<<PT>><<LINE>>;;
    LINE NAME="0_452-111_452_pk1",
    MODE=5,
    HEADWAY[1]=15,
    HEADWAY[2]=10,
    NODES=
     39249,
     -39240,
     54648

     LINE NAME="0_134-111_134_pk0",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=120,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      84250,
      92566,
      129190
    """
    test_project = Project.create_project(
        base_transit_source=test_lin_base,
        build_transit_source=test_lin_build,
        project_name="test suite small changes",
    )
    print("")
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_transit_test.yml"))
    ## todo write an assert that actually tests something


@pytest.mark.travis
@pytest.mark.transit
def test_write_transit_project_card_diffing_lin(request):
    print("\n--Starting:", request.node.name)

    test_project = Project.create_project(
        base_transit_source=os.path.join(CUBE_DIR, "transit.LIN"),
        build_transit_source=os.path.join(
            CUBE_DIR, "single_transit_route_attribute_change"
        ),
    )
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_transit_test.yml"))
    ## todo write an assert that actually tests something


@pytest.mark.transit
@pytest.mark.travis
def test_write_transit_project_card_route_shape(request):
    print("\n--Starting:", request.node.name)

    test_project = Project.create_project(
        base_transit_source=os.path.join(CUBE_DIR, "transit.LIN"),
        build_transit_source=os.path.join(CUBE_DIR, "transit_route_shape_change"),
    )
    test_project.write_project_card(
        os.path.join(SCRATCH_DIR, "t_transit_shape_test.yml")
    )
    ## todo write an assert that actually tests something


@pytest.mark.travis
@pytest.mark.transit
def test_read_cube_transit_standard_from_wrangler_object(request):
    print("\n--Starting:", request.node.name)
    from network_wrangler import TransitNetwork

    tnet = TransitNetwork.read(feed_path=BASE_TRANSIT_DIR)
    cube_transit_net = StandardTransit.fromTransitNetwork(tnet)


@pytest.mark.travis
@pytest.mark.transit
def test_read_cube_transit_standard_from_file(request):
    print("\n--Starting:", request.node.name)
    cube_transit_net = StandardTransit.read_gtfs(BASE_TRANSIT_DIR)


@pytest.mark.travis
@pytest.mark.transit
@pytest.mark.menow
def test_read_write_cube_transit_standard_from_wrangler_object(request):
    print("\n--Starting:", request.node.name)
    from network_wrangler import TransitNetwork

    cube_transit_net = StandardTransit.fromTransitNetwork(
        TransitNetwork.read(feed_path=BASE_TRANSIT_DIR)
    )
    cube_transit_net.write_as_cube_lin(os.path.join(SCRATCH_DIR, "t_transit_test.lin"))


@pytest.mark.travis
@pytest.mark.transit
def test_write_cube_transit_standard(request):
    print("\n--Starting:", request.node.name)

    cube_transit_net = StandardTransit.read_gtfs(BASE_TRANSIT_DIR)

    cube_transit_net.write_as_cube_lin(os.path.join(SCRATCH_DIR, "t_transit_test.lin"))
