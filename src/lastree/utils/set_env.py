#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script use to parse R code and convert R functions to python funcitons.
Also helps to set up the proper running environments.
"""
# Version 04.25.2024
# Author: Jiawei Li
import pathlib
import shutil
import rpy2
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
import rpy2.robjects.packages as rpackages
from pathlib import Path
from types import SimpleNamespace
import pandas as pd


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
        return ro.r('NA')
    if config_str.startswith('"') and config_str.endswith('"'):
        return config_str.replace('"', '')
    if config_str.replace('e', '', 1).isdigit():
        return int(float(config_str))
    if config_str.replace('e-', '', 1).isdigit():
        return float(config_str)
    return config_str


def parse_config(config_path: (str, pathlib.PurePath)) -> dict:
    """
    This function takes a pathlik object point to CONFIG file and parse the config file into a dictionary
    :param config_path: pathlike object, the path to the CONFIG file
    :return: dictoionary, the parsed config file in a dictionary format
    """

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


def create_project_structure(config_path: (str, pathlib.PurePath),
                             force_overlap=False) -> rpy2.robjects.vectors.DataFrame:
    """
    This program is translated from R function create_project_structure, this program adds path of the temp folders to \
    the config dictionary taken from the parse_config function.
    :param config_path: pathlib.PurePath, the path to the CONFIG file
    :param force_overlap: bool, if True, all temp files will be removed
    :return: SimpleNamespace, the namespace contains all necessary parameters for the program
    """
    user_input = parse_config(config_path)
    output_dir = Path(user_input['output_dir'])
    output_dir.mkdir(exist_ok=True)
    temp_dir = output_dir.joinpath('point_cloud_processing_temp')
    temp_dir.mkdir(exist_ok=True)
    delivery_dir = output_dir.joinpath('point_cloud_processing_delivery')
    if force_overlap:
        shutil.rmtree(delivery_dir, ignore_errors=True)
    delivery_dir.mkdir(exist_ok=True)
    concat_dict = {**user_input,'temp_dir':str(temp_dir),'delivery_dir':str(delivery_dir)}
    temp_folders = ["00_grid", "01_classify", "02_normalize", "03_dtm", "04_chm",
                    "05_stem_norm", "06_las_stem", "07_stem_poly_tile"]
    temp_folders_tag = ['las_grid_dir','las_classify_dir','las_normalize_dir','dtm_dir',
                        'chm_dir','las_stem_norm_dir','las_stem_dir','stem_poly_tile_dir']
    for tag, folder in zip(temp_folders_tag, temp_folders):
        temp_path = temp_dir.joinpath(folder)
        if force_overlap:
            shutil.rmtree(temp_path, ignore_errors=True)
        temp_path.mkdir(exist_ok=True)
        concat_dict[tag] = str(temp_path)
    config_df = pd.DataFrame(concat_dict, index=[0])
    with (ro.default_converter + pandas2ri.converter).context():
        config = ro.conversion.get_conversion().py2rpy(config_df)
    ro.globalenv['config'] = config
    ro.globalenv['accuracy_level'] = ro.r('config$accuracy_level')
    ro.globalenv['max_pts_m2'] = ro.r('{}'.format(ro.r('config$max_pts_m2')[0]))
    ro.r('config$max_pts_m2[1]=max_pts_m2')
    ro.r('config <- as.list(config)')
    config = ro.r['config']
    print("Add variable 'config' to the R environment")
    return config


def load_r_packages() -> SimpleNamespace:
    """
    This function load all necessary R packages for the program and save into a namespace.
    :return: rpaks: SimpleNamespace
    """
    concat_dict = {}
    packnames = ('lasR','tidyverse', 'viridis', 'scales', 'latex2exp', 'tools', 'terra',
                 'sf','sfarrow','lidR', 'ForestTools', 'rlas', 'lasR', 'TreeLS', 'randomForest',
                 'RCSF', 'RMCC', 'brms', 'parallel', 'doParallel', 'foreach', 'kableExtra')
    for tag in packnames:
        name = rpackages.importr(tag)
        concat_dict[tag] = name
    rpaks = SimpleNamespace(**concat_dict)
    return rpaks


if __name__ == '__main__':
    rpaks = load_r_packages()
    create_project_structure('/las_tree/defaults/CONFIG')
