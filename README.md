# Point Cloud Tree Detection Example

This code was adapted from work by [Neal Swayze and Wade Tinkham (2022)](https://scholar.google.com/scholar?oi=bibs&hl=en&cluster=10655866445299954513) to ingest raw point cloud data for processing and analysis of forest structure metrics.

[*Project site*](https://georgewoolsey.github.io/point_cloud_tree_detection_ex/)

## Point Cloud Tree Detection Python Interface 

This R-Python interface allows program to run under CLI, and convert R code to Python environment for further ML development. 
The environment was packed into a container by [apptainer](https://apptainer.org/docs/user/main/introduction.html) 
The container used for this program can be found [HERE](https://drive.usercontent.google.com/download?id=1WWhHFDZQIiv2lr2rtfAXd-BCcJlUhrCc&confirm=t&uuid=56606aaf-79fc-483d-ad11-a860775133c9)  

### usage 
```
#install Apptainer (for ubuntu)
sudo add-apt-repository -y ppa:apptainer/ppa
sudo apt update
sudo apt install -y apptainer

#clone this repository 
git clone https://github.com/jldz9/point_cloud_tree_detection_ex.git
cd point_cloud_tree_detection_ex
#Put the container image you downloaded from google drive "las_tree_container.sif" in current folder

#Get example data
apptainer exec ./las_tree_container.sif ./las_tree.py --example_data

#Init CONFIG file
apptainer exec ./las_tree_container.sif ./las_tree.py -p

#Run the point cloud tree detection
apptainer exec ./las_tree_container.sif ./las_tree.py -p ./CONFIG
```

