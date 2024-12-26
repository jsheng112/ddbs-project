# Distributed Database System Project

This project is part of the Distributed Database Systems course and aims to implement a distributed data center with efficient data management and querying capabilities. The system supports structured and unstructured data processing, bulk data loading, and advanced functionalities such as fault tolerance and data migration.

## Features
- Bulk loading of User, Article, and Read tables.
- Efficient querying and updating of data.
- Monitoring DBMS server status and workloads.
- Optional advanced features for fault tolerance and data migration.

## Prerequisites
- Python 3.x
- MongoDB
- Required Python libraries (install using `pip install -r requirements.txt`)

## Data Creation
To create the data, run the script `genTable_mongoDB10G.py` in the `data` folder

## Build Instructions
1. Open the `Makefile` to configure your environment.
2. **Important**: Update the IP addresses in the `Makefile` and in `docker-compose-files/mongo-docker-compose.yaml` to match the current machine's IP (you can find it by running `ipconfig getifaddr en0` on MacOS).
3. Run the following commands depending on the desired functionality:
   - To execute all steps except HDFS setup, run:
     ```bash
     make all_except_hdfs
     ```
     This will execute the following tasks: `config`, `dbms1`, `dbms2`, `mongo`, `redis`, `movefiles`, and `sharddata`.
   - To set up HDFS and move files to HDFS, run:
     ```bash
     make hdfs_files
     ```
     This will execute the following tasks: `hdfs` and `movefilestohdfs`.

## Running the GUI
To interact with the distributed database system using the graphical user interface (GUI):
1. Navigate to the directory containing the `mongo_app.py` script.
2. Start the GUI by running:
   ```bash
   python mongo_app.py
