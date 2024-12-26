docker compose -f docker-compose-files/config-server-docker-compose.yaml down
docker compose -f docker-compose-files/dbms1-docker-compose.yaml down
docker compose -f docker-compose-files/dbms2-docker-compose.yaml down
# docker compose -f docker-compose-files/hdfs-docker-compose.yaml down
docker compose -f docker-compose-files/mongo-docker-compose.yaml down
docker compose -f docker-compose-files/redis-docker-compose.yaml down
docker volume rm docker-compose-files_cfg1
docker volume rm docker-compose-files_cfg2
docker volume rm docker-compose-files_cfg3
docker volume rm docker-compose-files_dbms1_svr
docker volume rm docker-compose-files_dbms2_svr
docker volume rm docker-compose-files_redis-data
docker volume prune -f
# Note that this reset does not reset the hdfs, because it takes several hours to reload the files in, so we suggest to not delete the hdfs related volumes when debugging