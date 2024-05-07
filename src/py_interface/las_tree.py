#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script is the main CLI interface for the program
"""
# Version 05.06.2024
# Author: Jiawei Li
from py_interface import r_script_2_py,set_env, tools
import argparse
import sys
from pathlib import Path
import pathlib
import shutil
import subprocess
import rpy2.robjects as ro
def create_parser():
    synopsis = 'This is a python interface for point_cloud_tree_detection_ex program'
    name = __name__.split('.')[-1]
    parser = argparse.ArgumentParser(
        name, description=synopsis)
    parser.add_argument("-p", "--path", help="Path to the CONFIG file")
    parser.add_argument("--generate_config", metavar='PATH',help = "Copy default CONFIG file")
    parser.add_argument("-f","--force_overlap", help="To force remove all files for new program",
                        action="store_true")
    parser.add_argument("--example_data", help="To download example data",
                        action="store_true")
    return parser


def cmd_line_parse(iargs=None):
    default_config_path = Path(__file__).parent.joinpath('defaults/CONFIG')
    parser = create_parser()
    inps = parser.parse_args(args=iargs)
    if inps.path:
        inps.path = Path(inps.path).resolve()
    if inps.force_overlap:
        print('All result will be purged')
    if inps.generate_config:
        user_given_path = Path(inps.generate_config).resolve()
        if user_given_path.is_dir():
            user_given_path = user_given_path.joinpath('CONFIG')
        shutil.copyfile(default_config_path, user_given_path)
        sys.exit(0)
    if inps.example_data:
        tools.download_example()
        sys.exit(0)
    return inps

def main(iargs=None):
    default_config_path = Path(__file__).parent.joinpath('defaults/CONFIG')
    inps = cmd_line_parse(iargs)
    if isinstance(inps.path, (str, pathlib.PurePath)):
        path = Path(inps.path).resolve()
    else:
        raise()
    if path.is_dir():
        path = path.joinpath('CONFIG')
        print('Try to parse config in {}'.format(path))
    if path.is_file():
        print("Parsing config in {}".format(path))
        config_path = path
        print('config_path:',config_path)
    else:
        print('No config file found in {}!'.format(path))
        sys.exit(0)
    print('Setting environment...this will take a while...')
    set_env.load_r_packages()
    config = set_env.create_project_structure(config_path, force_overlap=inps.force_overlap)
    #create_lax_file for las file
    config = r_script_2_py.create_lax_for_tiles(config)
    #read_las_ctg
    config, las_ctg = r_script_2_py.readlascatalog(config)
    #reproj_crs
    config, las_ctg = r_script_2_py.reproject_las_fn(config, las_ctg)
    #handle_missing_crs
    config, las_ctg = r_script_2_py.handle_missing_crs(config, las_ctg)
    #check_ctg_size
    config = r_script_2_py.check_ctg_size(config, las_ctg)
    #write_las_coverage
    r_script_2_py.write_las_coverage(config, las_ctg)
    #create_chunk_ctg
    config, ctg_chunk_data = r_script_2_py.create_chunk_ctg(config, las_ctg)
    #determine_chunking
    config, process_data = r_script_2_py.determine_chunking(config, ctg_chunk_data)
    tools.clean_mem()
    #classify_las
    config = r_script_2_py.lasr_classify_csf(config)
    #denoise_pipeline
    config = r_script_2_py.denoise(config)
    #dtm+normalize_pipeline
    config = r_script_2_py.dtm_with_normalize_pipe(config)
    #chm_pipeline
    config = r_script_2_py.chm_pipe(config)
    #build_lasR_pipeline
    config = r_script_2_py.lasR_pipe(config)
    #execute_lasR_pipeline
    config = r_script_2_py.lasR_pipe_execute(config)
    #write_dtm
    config = r_script_2_py.write_dtm(config)
    #write_chm
    config = r_script_2_py.write_chm(config)
    #detect_tree_stems
    config = r_script_2_py.detect_tree_stems(config)
    #tree_map_func
    config = r_script_2_py.tree_map_func(config)
    #write_stem_las_fn
    config = r_script_2_py.write_stem_las_fn(config)
    #call_stem_las_fn
    config = r_script_2_py.call_stem_las_fn(config)
    tools.clean_mem()
    #combine_stem_vector
    config = r_script_2_py.combine_stem_vector(config)
    tools.clean_mem()
    #set CHM individual tree detection
    config = r_script_2_py.chm_individual_tree_detec(config)
    tools.clean_mem()
    #model_missing_dbh
    config = r_script_2_py.model_missing_dbh(config)
    #Calculate Silviculture Metrics
    config = r_script_2_py.calculate_silviculture_metrics(config)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        subprocess.run(['python','las_tree.py','-h'])
    else:
        main(sys.argv[1:])
