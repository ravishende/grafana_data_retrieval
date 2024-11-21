# Grafana Data Retrieval

## Repo for PREMOA Software

> Performance Retrieval and Estimation for Metric Optimization and Analysis

This code base uses Python with PromQL querying to collect data from Nautilus databases about analytics of wifire-quicfire runs.

It collects all of the information that can be found on the [Grafana](https://grafana.nrp-nautilus.io/d/85a562078cdf77779eaa1add43ccec1e/kubernetes-compute-resources-namespace-pods?orgId=1&var-datasource=default&var-cluster=&var-namespace=wifire-quicfire&from=1690454188000&to=1690472188000) website and displays each statistic by Node and Pod, rather than a sum of all pods like Grafana does.

Running `main.py` will collect and print all current information for the header, tables, and graphs.`

Other Features:

1. Collect training data for bp3d (Burn Pro 3D) runs over a time period

   &nbsp; &nbsp; `training_data_collection/` (specifically work_flow.py)

2. Collect training data for any run (BP3D or otherwise) over a period of time, with a wide variety of potential queries.

   &nbsp; &nbsp; `general_td_collection/` (specifically workflow.py)

3. Collect all data from tables.py for a BP3D run

   &nbsp; &nbsp; `table_for_run.py`

4. Get analytics from other Grafana Dashboards - GPUs, storage, etc.

   &nbsp; &nbsp; `dashboards/`

---

### Data Collected

There are 3 main types of data collected:

1. **Header data**:

   &nbsp; &nbsp; Singular datapoints (per pod) on CPU and Memory Utilization

2. **Tables**:

   &nbsp; &nbsp; Data tables containing several columns of statistics about related topics

3. **Graphs**:

   &nbsp; &nbsp; A table of datapoints containing several times and values per pod that can be displayed with a graph containing several colored lines (one line per pod). Graphs can be displayed by running graph_visualization.py

#### Header:

![Header](extras/readme_photos/example_header.png)

#### Table:

![Tables](extras/readme_photos/example_table.png)

#### Graph:

![Graphs](extras/readme_photos/example_graph.png)

---

### Inputs

There are 4 main inputs (defined in `inputs.py`) that specify what information will be returned:

1. `DEFAULT_DURATION`
2. `DEFAULT_GRAPH_STEP`
3. `DEFAULT_GRAPH_TIME_OFFSET`
4. `DEFAULT_FINAL_GRAPH_TIME`

You can also specify specific inputs in the parameters when initializing a class in main_functions.py. Changing these inputs will affect how much data will be returned and from how long ago.
To see how inputs affect a specific query, mess with the settings for that query [here](https://thanos.nrp-nautilus.io/).
Note: There are a couple other inputs used which can be found in [inputs.py](inputs.py)

For more information on the code base, inputs, and all data collected, look at [extras/data_retrieval.pdf](extras/data_retrieval.pdf)

For information on how the queries were originally found, look at these older notes: [extras/queries_notes.md](extras/queries_notes.md)
