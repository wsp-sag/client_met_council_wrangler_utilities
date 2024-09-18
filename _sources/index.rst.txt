.. lasso documentation master file, created by
   sphinx-quickstart on Thu Dec  5 15:43:28 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to lasso's documentation!
=================================

This package of utilities is a wrapper around the
[network_wrangler](http://github.com/wsp-sag/network_wrangler) package
for MetCouncil.  It aims to have the following functionality:
1. parse Cube log files and base highway networks and create ProjectCards
   for Network Wrangler
2. parse two Cube transit line files and create ProjectCards for NetworkWrangler
3. refine Network Wrangler highway networks to contain specific variables and
   settings for Metropolitan Council and export them to a format that can
   be read in by  Citilab's Cube software.

.. toctree::
   :maxdepth: 3
   :caption: Contents:

   starting
   setup
   running
   autodoc



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
