# Starting Out

## Installation

If you are managing multiple python versions, we suggest using [`virtualenv`](https://virtualenv.pypa.io/en/latest/) or [`conda`](https://conda.io/en/latest/) virtual environments.

Example using a conda environment (recommended) and using the package manager [pip](https://pip.pypa.io/en/stable/) to install Lasso from the source on GitHub.

```bash
conda config --add channels conda-forge
conda create python=3.7 rtree geopandas -n <my_lasso_environment>
conda activate <my_lasso_environment>
pip install git+https://github.com/wsp-sag/Lasso@master
```

Lasso will install `network_wrangler` from the [PyPi](https://pypi.org/project/network-wrangler/) repository because it is included in Lasso's `requirements.txt`.

#### Bleeding Edge
If you want to install a more up-to-date or development version of network wrangler and lasso , you can do so by installing it from the `develop` branch of

```bash
conda config --add channels conda-forge
conda create python=3.7 rtree geopandas -n <my_lasso_environment>
conda activate <my_lasso_environment>
pip install git+https://github.com/wsp-sag/network_wrangler@develop
pip install git+https://github.com/wsp-sag/Lasso@develop
```

#### From Clone
If you are going to be working on Lasso locally, you might want to clone it to your local machine and install it from the clone.  The -e will install it in [editable mode](https://pip.pypa.io/en/stable/reference/pip_install/?highlight=editable#editable-installs).

**if you plan to do development on both network wrangler and lasso locally, consider installing network wrangler from a clone as well!**

```bash
conda config --add channels conda-forge
conda create python=3.7 rtree geopandas osmnx -n <my_lasso_environment>
conda activate <my_lasso_environment>
git clone https://github.com/wsp-sag/Lasso
git clone https://github.com/wsp-sag/network_wrangler
cd network_wrangler
pip install -e .
cd ..
cd Lasso
pip install -e .
```

Notes:

1. The -e installs it in editable mode.
2. If you are not part of the project team and want to contribute code bxack to the project, please fork before you clone and then add the original repository to your upstream origin list per [these directions on github](https://help.github.com/en/articles/fork-a-repo).
3. if you wanted to install from a specific tag/version number or branch, replace `@master` with `@<branchname>`  or `@tag`
4. If you want to make use of frequent developer updates for network wrangler as well, you can also install it from clone by copying the instructions for cloning and installing Lasso for Network Wrangler

If you are going to be doing Lasso development, we also recommend:
 -  a good IDE such as [Atom](http://atom.io), VS Code, Sublime Text, etc.
 with Python syntax highlighting turned on.  
 - [GitHub Desktop](https://desktop.github.com/) to locally update your clones   

## Brief Intro

Lasso is a 'wrapper' around the [Network Wrangler](http://wsp-sag.github.io/network_wrangler) utility.  

Both Lasso and NetworkWrangler are built around the following data schemas:
 - [`roadway network`], which is based on a mashup of Open Street Map and [Shared Streets](http://sharedstreets.io).  In Network Wrangler these are read in from three json files reprsenting: links, shapes, and nodes. Data fields that change by time of day or by user category are represented as nested fields such that any field can be defined for an ad-hoc time-of-day span or user category.
 - [`transit network`], which is based on a frequency-based implementation of the csv-based GTFS; and
 - [`project card`], which is novel to Network Wrangler and stores information about network changes as a result of projects in yml.

In addition, Lasso utilizes the following data schemas:

 - [`MetCouncil Model Roadway Network Schema`], which adds data fields to the `roadway network` schema that MetCouncil uses in their travel model including breaking out data fields by time period.
 - [`MetCouncil Model Transit Network Schema`], which uses the Cube PublicTransport format,  and
 - [`Cube Log Files`], which document changes to the roadway network done in the Cube GUI. Lasso translates these to project cards in order to be used by NetworkWrangler.  
 - [`Cube public transport line files`], which define a set of transit lines in the cube software.

### Components
Network Wrangler has the following atomic parts:

 - _RoadwayNetwork_ object, which represents the `roadway network` data as GeoDataFrames;  
 - _TransitNetwork_ object, which represents the `transit network` data as DataFrames;  
 - _ProjectCard_ object, which represents the data of the `project card`.  Project cards identify the infrastructure that is changing (a selection) and defines the changes; or contains information about a new facility to be constructed or a new service to be run.;  
 - _Scenario_ object, which consist of at least a RoadwayNetwork, and
TransitNetwork.  Scenarios can be based on or tiered from other scenarios.
Scenarios can query and add ProjectCards to describe a set of changes that should be made to the network.

In addition, Lasso has the following atomic parts:  

 - _Project_ object, creates project cards from one of the following: a base and a build transit network in cube format, a base and build highway network, or a base highway network and a Cube log file.
 - _ModelRoadwayNetwork_ object is a subclass of `RoadwayNetwork` and contains MetCouncil-specific methods to define and create MetCouncil-specific variables and export the network to a format that can be read by Cube.
 - _StandardTransit_, an object for holding a standard transit feed as a Partridge object and contains
   methods to manipulate and translate the GTFS data to MetCouncil's Cube Line files.   
 - _CubeTransit_, an object for storing information about transit defined in `Cube public transport line files`
   . Has the capability to parse cube line file properties and shapes into python dictionaries and compare line files and represent changes as Project Card dictionaries.
 - _Parameters_, A class representing all the parameters defining the networks
   including time of day, categories, etc. Parameters can be set at runtime by initializing a parameters instance
   with a keyword argument setting the attribute.  Parameters that are
   not explicitly set will use default parameters listed in this class.

#### RoadwayNetwork

Reads, writes, queries and and manipulates roadway network data, which
is mainly stored in the GeoDataFrames `links_df`, `nodes_df`, and `shapes_df`.

```python
net = RoadwayNetwork.read(
        link_file=MY_LINK_FILE,
        node_file=MY_NODE_FILE,
        shape_file=MY_SHAPE_FILE,
    )
my_selection = {
    "link": [{"name": ["I 35E"]}],
    "A": {"osm_node_id": "961117623"},  # start searching for segments at A
    "B": {"osm_node_id": "2564047368"},
}
net.select_roadway_features(my_selection)

my_change = [
    {
        'property': 'lanes',
        'existing': 1,
        'set': 2,
     },
     {
        'property': 'drive_access',
        'set': 0,
      },
]

my_net.apply_roadway_feature_change(
    my_net.select_roadway_features(my_selection),
    my_change
)

ml_net = net.create_managed_lane_network(in_place=False)

ml_net.is_network_connected(mode="drive"))

_, disconnected_nodes = ml_net.assess_connectivity(
  mode="walk",
  ignore_end_nodes=True
)
ml_net.write(filename=my_out_prefix, path=my_dir)
```
#### TransitNetwork

#### ProjectCard

#### Scenario

Manages sets of project cards and tiering from a base scenario/set of networks.

```python

my_base_scenario = {
    "road_net": RoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    ),
    "transit_net": TransitNetwork.read(STPAUL_DIR),
}

card_filenames = [
    "3_multiple_roadway_attribute_change.yml",
    "multiple_changes.yml",
    "4_simple_managed_lane.yml",
]

project_card_directory = os.path.join(STPAUL_DIR, "project_cards")

project_cards_list = [
    ProjectCard.read(os.path.join(project_card_directory, filename), validate=False)
    for filename in card_filenames
]

my_scenario = Scenario.create_scenario(
  base_scenario=my_base_scenario,
  project_cards_list=project_cards_list,
)
my_scenario.check_scenario_requisites()

my_scenario.apply_all_projects()

my_scenario.scenario_summary()
```

#### Project
Creates project cards by comparing two MetCouncil Model Transit Network files or by reading a cube log file and a base network;  

```python

test_project = Project.create_project(
  base_transit_source=os.path.join(CUBE_DIR, "transit.LIN"),
  build_transit_source=os.path.join(CUBE_DIR, "transit_route_shape_change"),
  )

test_project.evaluate_changes()

test_project.write_project_card(
  os.path.join(SCRATCH_DIR, "t_transit_shape_test.yml")
  )

```

#### ModelRoadwayNetwork
A subclass of network_wrangler's RoadwayNetwork
class which additional understanding about how to translate and write the
network out to the MetCouncil Roadway Network schema.

```Python

net = ModelRoadwayNetwork.read(
      link_file=STPAUL_LINK_FILE,
      node_file=STPAUL_NODE_FILE,
      shape_file=STPAUL_SHAPE_FILE,
      fast=True,
  )

net.write_roadway_as_fixedwidth()

```

#### StandardTransit
Translates the standard GTFS data to MetCouncil's Cube Line files.

```Python
cube_transit_net = StandardTransit.read_gtfs(BASE_TRANSIT_DIR)
cube_transit_net.write_as_cube_lin(os.path.join(WRITE_DIR, "outfile.lin"))
```

#### CubeTransit
Used by the project class and has the capability to:
 - Parse cube line file properties and shapes into python dictionaries  
 - Compare line files and represent changes as Project Card dictionaries  

```python
tn = CubeTransit.create_from_cube(CUBE_DIR)
transit_change_list = tn.evaluate_differences(base_transit_network)
```

#### Parameters
Holds information about default parameters but can
also be initialized to override those parameters at object instantiation using a dictionary.

```Python
# read parameters from a yaml configuration  file
# could also provide as a key/value pair
with open(config_file) as f:
      my_config = yaml.safe_load(f)

# provide parameters at instantiation of ModelRoadwayNetwork
model_road_net = ModelRoadwayNetwork.from_RoadwayNetwork(
            my_scenario.road_net, parameters=my_config.get("my_parameters", {})
        )
# network written with direction from the parameters given
model_road_net.write_roadway_as_shp()

```

### Typical Workflow

Workflows in Lasso and Network Wrangler typically accomplish one of two goals:  
1. Create Project Cards to document network changes as a result of either transit or roadway projects.
2. Create Model Network Files for a scenario as represented by a series of Project Cards layered on top of a base network.

#### Project Cards from Transit LIN Files


#### Project Cards from Cube LOG Files


#### Model Network Files for a Scenario



## Running Quickstart Jupyter Notebooks

To learn basic lasso functionality, please refer to the following jupyter notebooks in the `/notebooks` directory:  

 - `Lasso Project Card Creation Quickstart.ipynb`   
 - `Lasso Scenario Creation Quickstart.ipynb`  

 Jupyter notebooks can be started by activating the lasso conda environment and typing `jupyter notebook`:

 ```bash
 conda activate <my_lasso_environment>
 jupyter notebook
 ```
