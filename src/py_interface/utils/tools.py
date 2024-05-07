import pathlib
from pathlib import Path
import rpy2.robjects as ro
import sys
import time
import urllib.request
import zipfile
import shutil
import os

def list_files_r(path: (str, pathlib.PurePath), pattern: (str, list)):
    pattern = list(pattern)
    path = Path(path)
    filelist = []
    for p in pattern:
        tmp_list = list(path.glob(p))
        filelist.extend(tmp_list)
    filelist_str = [str(f) for f in filelist]
    return filelist_str

def clean_mem():
    ro.r("""
    remove(list = ls()[grep("_temp",ls())])
    # clean up from setup to free some memory
    # check_ls_size_fn(ls())
    gc()
    """)
    return

def progress_reporthook(count, block_size, total_size):
    global start_time
    if count == 0:
        start_time = time.time()
        return
    duration = time.time() - start_time
    progress_size = int(count * block_size)
    speed = int(progress_size / (1024 * duration))
    percent = int(count * block_size * 100 / total_size)
    sys.stdout.write("\r...%d%%, %d MB, %d KB/s, %d seconds passed" %
                    (percent, progress_size / (1024 * 1024), speed, duration))
    sys.stdout.flush()


def extract(zip_path, target_path):
    with zipfile.ZipFile(zip_path) as zf:
        filesList = zf.namelist()
        for idx, file in enumerate(filesList):
            percent = round((idx / len(filesList)) * 100)
            sys.stdout.write("\rUnzipping...%d%% %s" %(percent, file))
            sys.stdout.flush()
            zf.extract(file, target_path)
        zf.close()
        print('\n')
def download_example():
    print('Downloading example data, the size of data will be around 6GB')
    print('Downloading Treemap data')
    url = 'https://s3-us-west-2.amazonaws.com/fs.usda.rds/RDS-2021-0074/RDS-2021-0074_Data.zip'
    treemap_file_dir = Path.cwd().parent.joinpath('data/treemap')
    treemap_file_dir.mkdir(parents=True, exist_ok=True)
    treemap_file = treemap_file_dir.joinpath('RDS-2021-0074_Data.zip')
    if any(treemap_file_dir.iterdir()):
        force_clean = input('The directory {} is not empty, do you want to purge it? (y/n)'.format(treemap_file_dir))
        if force_clean.lower() == 'y':
            shutil.rmtree(treemap_file_dir)
        else:
            print('Skip cleanning')
    if treemap_file.exists():
        print('Treemap data already exists')
    else:
        urllib.request.urlretrieve(url, treemap_file, reporthook=progress_reporthook)
    print('Unziping Treemap data')
    extract(treemap_file, treemap_file_dir)
    files = treemap_file_dir.joinpath('Data').glob('*')
    for f in files:
        shutil.move(f, treemap_file_dir)
    shutil.rmtree(treemap_file_dir.joinpath('Data'))
    las_file_dir = Path.cwd().parent.joinpath('data/las_data')
    las_file_dir.mkdir(parents=True, exist_ok=True)
    las_file = las_file_dir.joinpath('las.zip')
    if any(las_file_dir.iterdir()):
        force_clean = input('The directory {} is not empty, do you want to purge it? (y/n)'.format(treemap_file_dir))
        if force_clean.lower() == 'y':
            shutil.rmtree(las_file_dir)
        else:
            print('Skip cleanning')

    if las_file.exists():
        print("Example LAS data already exist in {}".format(las_file))
    else:
        print('Downloading LAS data')
        url_las = ('https://drive.usercontent.google.com/download?id=1-k6Os9E59GcFyGT9WoY3V6hNX2SmQsMV&export='
                    'download&authuser=0&confirm=t&uuid=cf8aa26d-3936-4fa5-a512-f6f19309df3b&at='
                    'APZUnTXkAH669BbhRiNWjGpaHymS%3A1715036657732')
        urllib.request.urlretrieve(url_las, las_file, reporthook=progress_reporthook)
    print('Unziping LAS data')
    extract(las_file, las_file_dir)
