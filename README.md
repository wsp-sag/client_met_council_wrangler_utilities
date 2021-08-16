# Lasso
This package of utilities is a wrapper around the [`network_wrangler`](http://github.com/wsp-sag/network_wrangler) package for MetCouncil.  **IMPORTANT** Lasso will automagically install `network_wrangler` for you, so

It aims to have the following functionality:

 1. parse Cube log files and base highway networks and create ProjectCards for Network Wrangler  
 2. parse two Cube transit line files and create ProjectCards for NetworkWrangler  
 3. refine Network Wrangler highway networks to contain specific variables and settings for Metropolitan Council and export them to a format that can be read in by  Citilab's Cube software.

## Installation

### Requirements
Lasso uses Python 3.6 and above.  Requirements are stored in `requirements.txt` but are automatically installed when using `pip` as are development requirements (for now) which are located in `dev-requirements.txt`.

The intallation instructions use the [`conda`](https://conda.io/en/latest/) virtual environment manager and some use the ['git'](https://git-scm.com/downloads) version control software.

### Basic instructions
If you are managing multiple python versions, we suggest using [`virtualenv`](https://virtualenv.pypa.io/en/latest/) or [`conda`](https://conda.io/en/latest/) virtual environments. All commands should executed in a conda command prompt, not powershell or the system command prompt. Do not add conda to the system path during installation. This may cause problems with other programs that require python 2.7 to be placed in the system path.

Example using a conda environment (recommended) and using the package manager [pip](https://pip.pypa.io/en/stable/) to install Lasso from the source on GitHub.

```bash
conda config --add channels conda-forge
conda create python=3.7 rtree geopandas folium osmnx -n <my_lasso_environment>
conda activate <my_lasso_environment>
pip install git+https://github.com/wsp-sag/Lasso@master
```
Lasso can be installed in several ways depending on the user's needs. The above installation is the simplest method and is appropriate when the user does not anticipate needing to update lasso. An update will require rebuilding the network wrangler environment. Installing from clone is slightly more involved and requires the user to have a git manager on their machine, but permits the user to install lasso with the -e, edit, option so that their lasso installation can be updated through pulling new commits from the lasso repo without a full reinstallation.

Lasso will install `network_wrangler` from the [PyPi](https://pypi.org/project/network-wrangler/) repository because it is included in Lasso's `requirements.txt`.

#### Bleeding Edge
If you want to install a more up-to-date or development version of network wrangler, you can do so by installing it from

```bash
conda config --add channels conda-forge
conda create python=3.7 rtree geopandas folium osmnx -n <my_lasso_environment>
conda activate <my_lasso_environment>
pip install git+https://github.com/wsp-sag/network_wrangler@develop
pip install git+https://github.com/wsp-sag/Lasso@develop
```

Note: if you wanted to install from a specific tag/version number or branch, replace `@master` with `@<branchname>`  or `@tag`

#### From Clone
If you are going to be working on Lasso locally, you might want to clone it to your local machine and install it from the clone.  The -e will install it in [editable mode](https://pip.pypa.io/en/stable/reference/pip_install/?highlight=editable#editable-installs).

**if you plan to do development on both network wrangler and lasso locally, consider installing network wrangler from a clone as well!**

```bash
conda config --add channels conda-forge
conda create python=3.7 rtree geopandas folium osmnx -n <my_lasso_environment>
conda activate <my_lasso_environment>
git clone https://github.com/wsp-sag/Lasso
git clone https://github.com/wsp-sag/network_wrangler
cd network_wrangler
pip install -e .
cd ..
cd Lasso
pip install -e .
```

Note: if you are not part of the project team and want to contribute code bxack to the project, please fork before you clone and then add the original repository to your upstream origin list per [these directions on github](https://help.github.com/en/articles/fork-a-repo).

## Documentation

Documentation is located at [https://wsp-sag.github.io/Lasso/](https://wsp-sag.github.io/Lasso/)

Edit the source of the documentation  in the `/docs` folder.

To build the documentation locally requires additional packages found in the `dev_requirements.txt` folder.  

To install these into your conda python environment execute the following from the command line in the Lasso folder:

```bash
conda activate <my_lasso_environment>
pip install -r dev-requirements.txt
```

To build the documentation, navigate to the `/docs` folder and execute the command: `make html`

```bash
cd docs
make html
```

## Usage

To learn basic lasso functionality, please refer to the following jupyter notebooks in the `/notebooks` directory:  

 - `Lasso Project Card Creation Quickstart.ipynb`   
 - `Lasso Scenario Creation Quickstart.ipynb`  

Jupyter notebooks can be started by activating the lasso conda environment and typing `jupyter notebook`:

```bash
conda activate <my_lasso_environment>
jupyter notebook
```

A few other very basic API tips:

#### Transit

**Parse a cube line file**
```python
import lasso

# read in the transit route information in cube line file format
# this will either read in a string or a filename containing the text
base_transit_lines = CubeTransit.create_from_cube(test_lin)

# Explore the base transit lines
print("Read {} LINES:\n{}".format(len(base_transit_lines.lines), "\n - ".join(base_transit_lines.lines)))
ex_line_name = base_transit_lines.lines[1]
print("Line: {}".format(ex_line_name))
print("Properties: ", base_transit_lines.line_properties[ex_line_name])
print("Nodes: ", base_transit_lines.shapes[ex_line_name])
```
**Compare two lines to create a project card describing their differences**

```python
import lasso

test_project = Project.create_project(
        base_transit_source=my_file_with_base_lines,
        build_transit_source=my_file_with_build_lines,
        project_name="My awesome project that will save the koalas.",
    )

test_project.write_project_card(os.path.join("project_card_transit.yml"))
```

**Read a Network Wrangler TransitNetwork standard and write it to Cube**

```python
from network_wrangler import TransitNetwork

wrangler_transit_network = TransitNetwork.read(feed_path=BASE_TRANSIT_DIR_WITH_GTFS)
cube_transit_net = StandardTransit.fromTransitNetwork(wrangler_transit_network)

cube_transit_net.write_as_cube_lin(os.path.join(SCRATCH_DIR, "t_transit_test.lin"))
```
**Read a GTFS (frequency only, not schedule-based) network and write it to Cube**
```python
from Lasso import StandardTransit
from network_wrangler import TransitNetwork

cube_transit_net = StandardTransit.read_gtfs(BASE_TRANSIT_DIR)

cube_transit_net.write_as_cube_lin(os.path.join(SCRATCH_DIR, "t_transit_test.lin"))
```
#### Roadway

**Read a wrangler roadway network standard network from file and write it to fixed width format that cube can read**

Note that this calculates MetCouncil specific variables and also by default will create a "model-ready" network which
separates out managed lanes as parallel links.

```python
from Lasso import ModelRoadwayNetwork

net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

net.write_roadway_as_fixedwidth()
```

**Read a Cube Log File of changes and produce project cards**

```python

from Lasso import Project

lf = Project.read_logfile(logfilename)

my_project = Project.create_project(
    roadway_log_file=logfilename, base_roadway_dir=directory_with_roadway_files
    )

my_project .write_project_card("my_awesome_project.yml",
```
## Troubleshooting

**Issue: Conda is unable to install a library or to update to a specific library version**
Try installing libraries from conda-forge

```bash
conda install -c conda-forge *library*
```

**Issue: User does not have permission to install in directories**
Try running Anaconda Prompt as an administrator.

## Client Contact and Relationship
Repository created in support of Met Council Network Rebuild project. Project lead on the client side is [Rachel Wiken](Rachel.Wiken@metc.state.mn.us). WSP team member responsible for this repository is [David Ory](david.ory@wsp.com).

## Attribution  
This project is built upon the ideas and concepts implemented in the [network wrangler project](https://github.com/sfcta/networkwrangler) by the [San Francisco County Transportation Authority](http://github.com/sfcta) and expanded upon by the [Metropolitan Transportation Commission](https://github.com/BayAreaMetro/NetworkWrangler).
