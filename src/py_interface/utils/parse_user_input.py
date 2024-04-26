#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script read CONFIG file of given path to return a dictionary, 
and generate one with default settings if no CONFIG file was found. 

"""
# Version 04.25.2024
# Author: Jiawei Li 

import argparse
import pathlib
import shutil
import sys
from pathlib import Path


def create_parser(subparsers=None):
    synopsis = 'Parse CONFIG file for user inputs'
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

def type_rules(arg):
    arg = arg.strip()
    if arg.isdigit():
        return int(arg)
    if arg.replace('.', '', 1).isdigit():
        return float(arg)
    if arg == 'T' or arg == 'True':
        return True
    if arg == 'F' or arg == 'False':
        return False
    if arg == 'NA' or arg =='NAN':
        return float('nan')
    if arg.startswith('"') and arg.endswith('"'):
        return arg.replace('"', '')
    if arg.replace('e', '', 1).isdigit():
        return int(float(arg))
    if arg.replace('e-', '', 1).isdigit():
        return float(arg)
    return arg

def parse_config(path):
    param_dict = {}
    with open(path, 'r') as file:
        lines = [line.strip() for line in file if not line.startswith(("#", " "))]
    param_list = [param for param in lines if param != '']  #remove all empty strs
    for p in param_list:
        key, value = p.split("=", 1)
        key = key.strip()
        value = type_rules(value)
        param_dict[key] = value
    return param_dict


def main(args=None) -> dict:
    """
    CLI usage: python parse_user_input.py -p <CONFIG path>
    To import use: from py_interface import parse_user_input
    :param args: path-like objective
    :return: dict
    """
    default_config_path = Path(__file__).parents[1].joinpath('defaults/CONFIG')
    if isinstance(args, (str, pathlib.Path)):
        path = Path(args)
    else:
        inps = cmd_line_parse(args)
        path = inps.path
    if path.is_dir():
        path = path.joinpath('CONFIG')
    if path.is_file():
        print("reading CONFIG in {}".format(path))
    else:
        shutil.copyfile(default_config_path, path)
        print('Generating config file under {}'.format(path))
    param_dict = parse_config(path)
    return param_dict


if __name__ == '__main__':
    if len(sys.argv[1:]) == 0:
        raise SyntaxError('Use python parse_user_input.py -p <path_to_CONFIG>')
    output_dict = main(sys.argv[1:])
    for key, value in output_dict.items():
        print("{}: {}".format(key, value))
