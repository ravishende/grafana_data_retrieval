#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
from tqdm import tqdm
from helpers.querying import query_data
from helpers.filtering import filter_df_for_workers
from inputs import NAMESPACE, DEFAULT_DURATION


class Tables():
    def __init__(self, namespace: str = NAMESPACE, duration: str = DEFAULT_DURATION) -> None:
        self.namespace = namespace
        self.duration = duration
        self.cpu_quota = pd.DataFrame(columns=[
                                      "Node", "Pod", "CPU Usage", "CPU Requests", "CPU Requests %", "CPU Limits", "CPU Limits %"])
        self.mem_quota = pd.DataFrame(columns=["Node", "Pod", "Memory Usage", "Memory Requests",  "Memory Requests %",
                                      "Memory Limits", "Memory Limits %", "Memory Usage (RSS)", "Memory Usage (Cache)"])
        self.network_usage = pd.DataFrame(columns=["Node", "Pod", "Current Receive Bandwidth", "Current Transmit Bandwidth", "Rate of Received Packets",
                                          "Rate of Transmitted Packets", "Rate of Received Packets Dropped", "Rate of Transmitted Packets Dropped"])
        self.storage_io = pd.DataFrame(columns=["Node", "Pod", "IOPS(Reads)", "IOPS(Writes)",
                                       "IOPS(Reads + Writes)", "Throughput(Read)", "Throughput(Write)", "Throughput(Read + Write)"])
        self.queries = {
            # CPU Quota
            'CPU Usage': 'sum by(node, pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="' + self.namespace + '"})',
            'CPU Requests': 'sum by(node, pod) (cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests{cluster="", namespace="' + self.namespace + '"})',
            'CPU Limits': 'sum by(node, pod) (cluster:namespace:pod_cpu:active:kube_pod_container_resource_limits{cluster="", namespace="' + self.namespace + '"})',
            # Memory Quota
            'Memory Usage': 'sum by(node, pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '", container!="", image!=""})',
            'Memory Requests': 'sum by(node, pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{cluster="", namespace="' + self.namespace + '"})',
            'Memory Limits': 'sum by(node, pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_limits{cluster="", namespace="' + self.namespace + '"})',
            'Memory Usage (RSS)': 'sum by(node, pod) (container_memory_rss{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '",container!=""})',
            'Memory Usage (Cache)': 'sum by(node, pod) (container_memory_cache{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '",container!=""})',
            # Network Usage
            'Current Receive Bandwidth': 'sum by(node, pod) (irate(container_network_receive_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Current Transmit Bandwidth': 'sum by(node, pod) (irate(container_network_transmit_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Rate of Received Packets': 'sum by(node, pod) (irate(container_network_receive_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Rate of Transmitted Packets': 'sum by(node, pod) (irate(container_network_transmit_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Rate of Received Packets Dropped': 'sum by(node, pod) (irate(container_network_receive_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Rate of Transmitted Packets Dropped':  'sum by(node, pod) (irate(container_network_transmit_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '"}[' + self.duration + ']))'
        }
        self.partial_queries = {
            # Current Storage IO
            'IOPS(Reads)': 'sum by(node, pod) (rate(container_fs_reads_total{job="kubelet", metrics_path="/metrics/cadvisor", container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'IOPS(Writes)': 'sum by(node, pod) (rate(container_fs_writes_total{job="kubelet", metrics_path="/metrics/cadvisor", container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Throughput(Read)': 'sum by(node, pod) (rate(container_fs_reads_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Throughput(Write)': 'sum by(node, pod) (rate(container_fs_writes_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))'
        }

    # get a dictionary of all the tables
    def get_tables_dict(self, only_include_worker_pods: bool = False, queries: dict[str, str] = None, partial_queries: dict[str, str] = None) -> dict[str, pd.DataFrame]:
        # Note: queries and partial_queries can be passed in as None and will be updated in
        # _fill_df_by_queries() for the first 3 and _get_storage_io() for 'Current Storage IO'
        tables_dict = {
            'CPU Quota': self._get_cpu_quota(queries=queries),
            'Memory Quota': self._get_mem_quota(queries=queries),
            'Current Network Usage': self._get_network_usage(queries=queries),
            'Current Storage IO': self._get_storage_io(partial_queries=partial_queries)
        }

        # filter by worker pods if requested
        if only_include_worker_pods:
            for title, table in tables_dict.items():
                tables_dict[title] = filter_df_for_workers(table)

        return tables_dict

    # Given a dictionary of queries, generate a table based on those queries
    # Note: if the queries do not start with "sum by(node, pod)", then you must set sum_by
    #        to be what the query has in "sum by(_____)""
    #        If queries do not have "sum by(...)", then set sum_by to None
    # Return a dictionary of a table_name: table_df if table_name is specified, otherwise returns
    # a dataframe that is the table
    def get_table_from_queries(self, queries_dict: dict[str, str], sum_by: list[str] | str | None = "_", table_name: str = "") -> pd.DataFrame | dict[str, pd.DataFrame]:
        # set ['node', 'pod'] as default for sum_by without putting dangerous default list in definition
        if sum_by == "_":
            sum_by = ["node", "pod"]
        # handle if sum_by is a single input (put into list format)
        if isinstance(sum_by, str):
            sum_by = [sum_by]
        # get rid of title case for sum_by metrics
        if sum_by is not None:
            for i, metric in enumerate(sum_by):
                sum_by[i] = metric[0].lower() + metric[1:]

        # generate tables
        table_df = pd.DataFrame()
        for title, query in tqdm(queries_dict.items()):
            single_query_table = self._generate_table_df(
                title, query, sum_by=sum_by)
            # add column to table
            if single_query_table is not None:
                # initialize table if it isn't already
                if len(table_df) == 0:
                    table_df = single_query_table
                else:
                    table_df[title] = single_query_table[title]
            else:
                table_df[title] = None

        # if table_name is specified, return the table_df in a dict of table_name:table_df
        if table_name != "":
            return {table_name: table_df}
        # otherwise, return the table_df
        return table_df

    # combines all table dataframes into one large dataframe.
    # Each table is represented as a few columns.
    # this works because all tables are queried for the same time frame and they have the same pods
    def get_tables_as_one_df(self, tables_dict: dict[str, pd.DataFrame] | None = None, only_include_worker_pods: bool = False, queries: dict[str, str] = None, partial_queries: dict[str, str] = None) -> pd.DataFrame:
        # initialize total df
        total_df = pd.DataFrame(columns=['Node', 'Pod'])

        # Generate tables if none given
        if tables_dict is None:
            tables_dict = self.get_tables_dict(
                only_include_worker_pods=only_include_worker_pods,
                queries=queries, partial_queries=partial_queries)

        # get first table_df that isn't empty and use its Node and Pod columns
        for table_df in tables_dict.values():
            if not table_df.empty:
                total_df = table_df[['Node', 'Pod']].copy()
                break

        # Fill in tables columns
        for table_df in tables_dict.values():
            for column in table_df.columns:
                # Node and Pod columns are all the same for each table_df
                if column in ["Node", "Pod"]:
                    continue
                # Add unique columns to total_df
                total_df[column] = table_df[column].copy()

        return total_df

    # return a dataframe of pods, nodes, and values for a given result_list for a
    # column in a table (e.g. CPUQuota: CPU usage)
    def _generate_df(self, col_title: str, res_list: list[dict]) -> pd.DataFrame:
        # initialize dataframe and filter json data
        df = pd.DataFrame(columns=['Node', 'Pod', col_title])
        # df = pd.DataFrame(columns=['Time', 'Node', 'Pod', col_title])  # for timestamp
        # fill in dataframe
        for datapoint in res_list:
            # each datapoint in parsed_data is a dictionary with node (str), pod (str), (int)
            node = datapoint['metric']['node']
            pod = datapoint['metric']['pod']
            value = datapoint['value'][1]
            # timestamp = data_point['value'][0]  # for timestamp
            # add a row to the end of the dataframe containing a node, pod, and value
            df.loc[len(df.index)] = [node, pod, value]
            # df.loc[len(df.index)] = [timestamp, node, pod, value]  # for timestamp
        return df

    # returns an updated dataframe by filling in data queried from the
    # columns in a passed in dataframe
    def _fill_df_by_queries(
            self, table_df: pd.DataFrame, queries: dict[str, str] | None = None) -> pd.DataFrame:
        # check if they passed in a dict of queries.
        if queries is None:
            queries = self.queries

        # update the columns of the database with the data from querying
        for col_title in tqdm(table_df.columns):
            # get the corresponding query for each column
            query = queries.get(col_title)
            if query is None:
                continue

            # update the table with the new column information
            result_list = query_data(query)
            new_df = self._generate_df(col_title, result_list)

            # if table_df is not empty
            if len(table_df.index) > 0:
                # add column to the rightmost end of the dataframe
                final_col = new_df.columns[-1]
                table_df[final_col] = new_df[final_col]
            else:  # table_df is empty.
                # set table_df equal to the new df to fill in node and pod too
                for column in new_df.columns:
                    table_df[column] = new_df[column]
        return table_df

    def _generate_table_df(self, query_title: str, query: str, sum_by: list[str] | str | None = "_") -> pd.DataFrame:
        # set ['node', 'pod'] as default for sum_by without putting dangerous default list in definition
        if sum_by == "_":
            sum_by = ["node", "pod"]
        # query for data
        result_list = query_data(query)
        if len(result_list) == 0:
            return None

        # prepare columns for table. columns will be appended to one pod at a time
        values_column = []

        # initialize sum_by columns to eventually be put into the table
        if sum_by is not None:  # if sum_by is None, don't add a sum_by column
            sum_by_columns = {metric: [] for metric in sum_by}

        # loop through the data for each pod. The data has: node, pod, values, timestamps
        for datapoint in result_list:
            # prepare data to be extracted
            values_list = []

            # fill in lists
            time_value_pair = datapoint['value']
            values_list.append(float(time_value_pair[1]))

            # add pod's lists to the table's columns
            values_column.extend(values_list)
            # add sum_by column (e.g. pod name)
            if sum_by is not None:
                for item in sum_by:
                    item_list = [datapoint['metric'][item]]*len(values_list)
                    sum_by_columns[item].extend(item_list)

        # create and populate table dataframe
        table_df = pd.DataFrame()
        # add in sum_by columns
        if sum_by is not None:  # if sum_by is None, don't add a sum_by column
            for item in sum_by:
                # make sure item cols in table are title case (Capitalized First Letters Of Words)
                table_df[item.title()] = sum_by_columns[item]
        # add in values column
        table_df[query_title] = values_column

        return table_df

    def _calc_percent(self, numerator_col: pd.Series, divisor_col: pd.Series) -> pd.Series:
        # divide the two columns, then multiply by 100 to get the percentage
        result = numerator_col.astype(float).div(divisor_col.astype(float))
        return result.multiply(100)

    # get the cpu_quota dataframe. If it is empty, generate it
    def _get_cpu_quota(self, queries: dict[str, str] | None = None) -> pd.DataFrame:
        # check if the table has been filled in. If it has, return it
        if len(self.cpu_quota.index) > 0:
            return self.cpu_quota

        if queries is None:
            queries = self.queries

        # if not, fill in the table then return it
        self.cpu_quota = self._fill_df_by_queries(
            table_df=self.cpu_quota, queries=queries)
        # calculate each percent column by dividing the two columns
        # responsible for it then multiplying by 100
        self.cpu_quota['CPU Requests %'] = \
            self._calc_percent(
                self.cpu_quota['CPU Usage'], self.cpu_quota['CPU Requests'])
        self.cpu_quota['CPU Limits %'] = \
            self._calc_percent(
                self.cpu_quota['CPU Usage'], self.cpu_quota['CPU Limits'])
        return self.cpu_quota

    # get the mem_quota dataframe. If it is empty, generate it
    def _get_mem_quota(self, queries: dict[str, str] | None = None) -> pd.DataFrame:
        # check if the table has been filled in. If it has, return it
        if len(self.mem_quota.index) > 0:
            return self.mem_quota

        if queries is None:
            queries = self.queries

        # if not, fill in the table then return it
        self.mem_quota = self._fill_df_by_queries(
            table_df=self.mem_quota, queries=queries)
        # calculate each percent column by dividing the two columns
        # responsible for it
        self.mem_quota['Memory Requests %'] = \
            self._calc_percent(
                self.mem_quota['Memory Usage'], self.mem_quota['Memory Requests'])
        self.mem_quota['Memory Limits %'] = \
            self._calc_percent(
                self.mem_quota['Memory Usage'], self.mem_quota['Memory Limits'])
        return self.mem_quota

    # get the network_usage dataframe. If it is empty, generate it
    def _get_network_usage(self, queries: dict[str, str] | None = None) -> pd.DataFrame:
        # check if the table has been filled in. If it has, return it
        if len(self.network_usage.index) > 0:
            return self.network_usage

        if queries is None:
            queries = self.queries

        # if not, fill in the table then return it
        self.network_usage = self._fill_df_by_queries(
            table_df=self.network_usage, queries=queries)
        return self.network_usage

    # get the storage_io dataframe. If it is empty, generate it
    def _get_storage_io(self, partial_queries: dict[str, str] | None = None) -> pd.DataFrame:
        # check if the table has been filled in. If it has, return it
        if len(self.storage_io.index) > 0:
            return self.storage_io

        if partial_queries is None:
            partial_queries = self.partial_queries

        # if not, fill in the table then return it
        self.storage_io = self._fill_df_by_queries(
            table_df=self.storage_io, queries=partial_queries)
        # calculate each sum column by adding the two columns responsible for it
        self.storage_io['IOPS(Reads + Writes)'] = \
            self.storage_io['IOPS(Reads)'].astype(
                float) + self.storage_io['IOPS(Writes)'].astype(float)
        self.storage_io['Throughput(Read + Write)'] = \
            self.storage_io['Throughput(Read)'].astype(
                float) + self.storage_io['Throughput(Write)'].astype(float)
        return self.storage_io
