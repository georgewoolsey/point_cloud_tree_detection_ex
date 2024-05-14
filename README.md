# Point Cloud Tree Detection Example

This code was adapted from work by [Neal Swayze and Wade Tinkham (2022)](https://scholar.google.com/scholar?oi=bibs&hl=en&cluster=10655866445299954513) to ingest raw point cloud data for processing and analysis of forest structure metrics.

[*Project site*](https://georgewoolsey.github.io/point_cloud_tree_detection_ex/)

## Point Cloud Tree Detection Python Interface 

This R-Python interface allows program to run under CLI, and convert R code to Python environment for further ML development. 
The environment was packed into a container by [apptainer](https://apptainer.org/docs/user/main/introduction.html) 

### Use the container image (recommend) 
```
#install Apptainer (for ubuntu)
sudo add-apt-repository -y ppa:apptainer/ppa
sudo apt update
sudo apt install -y apptainer

#Download container image
apptainer remote add --no-login SylabsCloud cloud.sycloud.io
apptainer remote use SylabsCloud
apptainer pull lastree.sif library://jldz9/lastree/image:1.0

#Execute the lastree
apptainer exec lastree.tif lastree -h
```
### Install use pip 
In this method, no R environment was installed, you will need to config R environment by yourself (Which is painful, trust me) 
```
#install lastree
pip install lastree

#run lastree
lastree -h
```

### Using the source code 
```
#clone this repository 
git clone https://github.com/jldz9/point_cloud_tree_detection_ex.git
```

### lastree usage 
```
#Get example data
apptainer exec lastree.sif las_tree --example_data

#Generate CONFIG file
apptainer exec las_tree_container.sif las_tree.py --generate_config

#Run the point cloud tree detection
apptainer exec las_tree_container.sif las_tree.py -p ./CONFIG
```

