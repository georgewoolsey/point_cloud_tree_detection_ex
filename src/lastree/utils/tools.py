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
            sys.stdout.write("\rUnzipping...%d%% %s" % (percent, file))
            sys.stdout.flush()
            if Path(target_path).joinpath(file).is_file():
                Path(target_path).joinpath(file).unlink()
            zf.extract(file, target_path)
        zf.close()
        print('\n')


def download_example():
    print('Downloading example data, the size of data will be around 6GB')
    treemap_file_dir = Path.cwd().joinpath('data/treemap')
    treemap_file_dir.mkdir(parents=True, exist_ok=True)
    treemap_file = treemap_file_dir.joinpath('RDS-2021-0074_Data.zip')
    treemap_url = 'https://s3-us-west-2.amazonaws.com/fs.usda.rds/RDS-2021-0074/RDS-2021-0074_Data.zip'
    if treemap_file.is_file():
        redownload = input('Treemap data already exists, do you want to download it as {}_1.{}? (y/n)'
                           .format(treemap_file.stem, treemap_file.suffix))
        if redownload.lower() == 'y':
            treemap_file = treemap_file_dir.joinpath(treemap_file.name.join('_1.zip'))
            urllib.request.urlretrieve(treemap_url, treemap_file, reporthook=progress_reporthook)
            extract(treemap_file, treemap_file_dir)
        else:
            extract(treemap_file, treemap_file_dir)
    else:
        urllib.request.urlretrieve(treemap_url, treemap_file, reporthook=progress_reporthook)
        print('Unzipping Treemap data')
        extract(treemap_file, treemap_file_dir)
    files = treemap_file_dir.joinpath('Data').glob('*')
    for f in files:
        if treemap_file_dir.joinpath(f.name).is_file():
            treemap_file_dir.joinpath(f.name).unlink()
        shutil.move(f, treemap_file_dir)

    shutil.rmtree(treemap_file_dir.joinpath('Data'))

    las_file_dir = Path.cwd().joinpath('data/las_data')
    las_file_dir.mkdir(parents=True, exist_ok=True)
    las_file = las_file_dir.joinpath('las.zip')
    url_las = ('https://drive.usercontent.google.com/download?'
               'id=1-k6Os9E59GcFyGT9WoY3V6hNX2SmQsMV&export=download&confirm'
               '=t&uuid=c78e06ca-0c61-4ef8-b2ba-de1ab83c6812')
    if las_file.is_file():
        redownload = input("Example LAS data already exist, do you want to download it as {}_1.{}? (y/n)"
                           .format(las_file.stem, las_file.suffix))
        if redownload.lower() == 'y':
            print('Downloading LAS data')
            las_file = las_file_dir.joinpath(las_file.name.join('_1.zip'))
            urllib.request.urlretrieve(url_las, las_file, reporthook=progress_reporthook)
            extract(las_file, las_file_dir)
        else:
            extract(las_file, las_file_dir)
    else:
        urllib.request.urlretrieve(url_las, las_file, reporthook=progress_reporthook)
        print('Unziping LAS data')
        extract(las_file, las_file_dir)

