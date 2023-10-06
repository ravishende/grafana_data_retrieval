#!/usr/bin/python3
# -*- coding: utf-8 -*-

# namespace
# NAMESPACE = 'wifire-quicfire'  # main namespace to be using
NAMESPACE = 'alto'  # used for testing when no data in wifire-quicfire

# duration for data types: tables, graphs
DEFAULT_DURATION = '10m'

# graphs
DEFAULT_GRAPH_STEP = '1m'
DEFAULT_GRAPH_TIME_OFFSET = '1h'
# requerying
REQUERY_GRAPH_STEP_DIVISOR = 10  # a value of 10 means there will be 10x the number of datapoints for a given timeframe
