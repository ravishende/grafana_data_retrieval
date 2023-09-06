#!/usr/bin/python3
# -*- coding: utf-8 -*-

# namespace
NAMESPACE = 'wifire-quicfire'
# NAMESPACE = 'alto'  # used for testing when no data in wifire-quicfire

# duration for all 3 data types: Header, tables, graphs
DEFAULT_DURATION = '10m'

# graphs
DEFAULT_GRAPH_STEP = '1m'
DEFAULT_GRAPH_TIME_OFFSET = '30m'
# requerying
REQUERY_GRAPH_STEP_DIVISOR = 10  # a value of 10 means there will be 10x the number of datapoints for a given timeframe
