#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script use to parse R code and convert R functions to python funcitons.
Also helps to set up the proper running environments.
"""
import pathlib
import shutil
import rpy2.robjects.packages as rpackages
from pathlib import Path
import argparse
from types import SimpleNamespace
import subprocess


def create_parser():
    synopsis = 'Set program environment'
    name = __name__.split('.')[-1]
    parser = argparse.ArgumentParser(
        name, description=synopsis)
    parser.add_argument("-p", "--path", help="Path to the CONFIG file")
    parser.add_argument("-f","--force_overlap", help="To force remove all files for new program",
                        action="store_true")
    return parser


def cmd_line_parse(iargs=None):
    parser = create_parser()
    inps = parser.parse_args(args=iargs)
    if inps.path:
        inps.path = Path(inps.path).resolve()
    else:
        print('Path {} is not exist'.format(inps.path))
    if inps.force_overlap:
        print('All temp results will be purged')
    return inps


def type_convert(config_str: str) -> (float, str, int, bool):
    config_str = config_str.strip()
    if config_str.isdigit():
        return int(config_str)
    if config_str.replace('.', '', 1).isdigit():
        return float(config_str)
    if config_str == 'T' or config_str == 'True':
        return True
    if config_str == 'F' or config_str == 'False':
        return False
    if config_str == 'NA' or config_str == 'NAN':
        return float('nan')
    if config_str.startswith('"') and config_str.endswith('"'):
        return config_str.replace('"', '')
    if config_str.replace('e', '', 1).isdigit():
        return int(float(config_str))
    if config_str.replace('e-', '', 1).isdigit():
        return float(config_str)
    return config_str


def parse_config(config_path:pathlib.PurePath) -> dict:
    config = {}
    with open(config_path, 'r') as file:
        lines = [line.strip() for line in file if not line.startswith(("#", " "))]
    # remove all empty strs
    param_list = [param for param in lines if param != '']
    for p in param_list:
        key, value = p.split("=", 1)
        key = key.strip()
        value = type_convert(value)
        config[key] = value
    return config


def create_project_structure(iargs):
    user_input = parse_config(iargs.path)
    output_dir = Path(user_input['output_dir'])
    output_dir.mkdir(exist_ok=True)
    temp_dir = output_dir.joinpath('point_cloud_processing_temp')
    temp_dir.mkdir(exist_ok=True)
    delivery_dir = output_dir.joinpath('point_cloud_processing_delivery')
    delivery_dir.mkdir(exist_ok=True)
    concat_dict = {'temp_dir':temp_dir,'delivery_dir':delivery_dir}
    temp_folders = ["00_grid", "01_classify", "02_normalize", "03_dtm", "04_chm",
                    "05_stem_norm", "06_las_stem", "07_stem_poly_tile"]
    temp_folders_tag = ['las_grid_dir','las_classify_dir','las_normalize_dir','dtm_dir',
                        'chm_dir','las_stem_norm_dir','las_stem_dir','stem_poly_tile_dir']
    for tag, folder in zip(temp_folders_tag, temp_folders):
        temp_path = temp_dir.joinpath(folder)
        if iargs.force_overlap:
            shutil.rmtree(temp_path)
        temp_path.mkdir(exist_ok=True)
        concat_dict[tag] = temp_path
    config = SimpleNamespace(**concat_dict)
    return config


def install_r_packages():
    sys_pkg_install_path = Path(__file__).parent.joinpath('set_env_debain.sh')
    rc = subprocess.call(str(sys_pkg_install_path))
    utils = rpackages.importr('utils')
    utils.chooseCRANmirror(ind=1)
    utils.install_packages('pak', dependencies=True)
    return

