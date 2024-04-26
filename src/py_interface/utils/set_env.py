#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script use to parse R code and convert R functions to python funcitons.
Also helps to set up the proper running environments.
"""

import rpy2.robjects as robjects
from pathlib import Path
from py_interface import parse_user_input
import argparse


def create_parser():
    synopsis = 'Set program environment'
    name = __name__.split('.')[-1]
    parser = argparse.ArgumentParser(
        name, description=synopsis)
    parser.add_argument("-p", "--path", help="Path to the CONFIG file")
    return parser


def cmd_line_parse(args=None):
    parser = create_parser()
    inps = parser.parse_args(args=args)
    if inps.path:
        inps.path = Path(inps.path).resolve()
    else:
        print('Path {} is not exist'.format(inps.path))
    return inps


def create_project_structure(path):
    user_input = parse_user_input(path)
    output_dir = Path(user_input['output_dir'])
    output_dir.mkdir(exist_ok=True)
    temp_dir = output_dir.joinpath('point_cloud_processing_temp')
    temp_dir.mkdir(exist_ok=True)
    delivery_dir = output_dir.joinpath('point_cloud_processing_delivery')
    delivery_dir.mkdir(exist_ok=True)
    temp_folders = ["00_grid",]