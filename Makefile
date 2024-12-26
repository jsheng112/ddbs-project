HOSTIP :=192.168.3.33
TIME_FILE := time.txt
all_except_hdfs: config dbms1 dbms2 mongo redis movefiles sharddata

hdfs_files: hdfs movefilestohdfs



config:
	@start_time=$$(date +%s%N | cut -c1-10); \
	docker compose -f docker-compose-files/config-server-docker-compose.yaml up -d; \
	docker exec -it cfg1 mongosh "mongodb://${HOSTIP}:30001" --eval "rs.initiate({_id: 'cfg', configsvr: true, members: [{ _id : 0, host : '${HOSTIP}:30001' }, { _id : 1, host : '${HOSTIP}:30002' }, { _id : 2, host : '${HOSTIP}:30003' }]})"; \
	end_time=$$(date +%s%N | cut -c1-10); \
	echo "Config setup time: $$((end_time-start_time)) seconds" >> $(TIME_FILE)

dbms1:
	@start_time=$$(date +%s%N | cut -c1-10); \
	docker compose -f docker-compose-files/dbms1-docker-compose.yaml up -d; \
	docker exec -it dbms1_svr mongosh "mongodb://${HOSTIP}:30004" --eval "rs.initiate({_id: \"dbms1\", members: [{ _id : 0, host : \"${HOSTIP}:30004\" }]})"; \
	end_time=$$(date +%s%N | cut -c1-10); \
	echo "DBMS1 setup time: $$((end_time-start_time)) seconds" >> $(TIME_FILE)

dbms2:
	@start_time=$$(date +%s%N | cut -c1-10); \
	docker compose -f docker-compose-files/dbms2-docker-compose.yaml up -d; \
	docker exec -it dbms2_svr mongosh "mongodb://${HOSTIP}:30005" --eval  "rs.initiate({_id: \"dbms2\", members: [{ _id : 0, host : \"${HOSTIP}:30005\" }]})"; \
	end_time=$$(date +%s%N | cut -c1-10); \
	echo "DBMS2 setup time: $$((end_time-start_time)) seconds" >> $(TIME_FILE)

hdfs:
	@start_time=$$(date +%s%N | cut -c1-10); \
	docker compose -f docker-compose-files/hdfs-docker-compose.yaml up -d; \
	docker run -d -p 80:80 --name myserver nginx
 	end_time=$$(date +%s%N | cut -c1-10); \
	echo "HDFS setup time: $$((end_time-start_time)) seconds" >> $(TIME_FILE)

mongo:
	@start_time=$$(date +%s%N | cut -c1-10); \
	env HOSTIP=${HOSTIP} docker compose -f docker-compose-files/mongo-docker-compose.yaml up -d; \
	docker exec -it mongo_svr mongosh "mongodb://${HOSTIP}:30000" --eval "sh.addShard(\"dbms1/${HOSTIP}:30004\")"; \
	docker exec -it mongo_svr mongosh "mongodb://${HOSTIP}:30000" --eval "sh.addShard(\"dbms2/${HOSTIP}:30005\")"; \
	end_time=$$(date +%s%N | cut -c1-10); \
	echo "Mongo setup time: $$((end_time-start_time)) seconds" >> $(TIME_FILE)

redis:
	@start_time=$$(date +%s%N | cut -c1-10); \
	docker compose -f docker-compose-files/redis-docker-compose.yaml up -d; \
	end_time=$$(date +%s%N | cut -c1-10); \
	echo "Redis setup time: $$((end_time-start_time)) seconds" >> $(TIME_FILE)

movefiles:
	@start_time=$$(date +%s%N | cut -c1-10); \
	docker cp data/user.dat mongo_svr:/user.dat; \
	docker cp data/article.dat mongo_svr:/article.dat; \
	docker cp data/read.dat mongo_svr:/read.dat; \
	docker exec -it mongo_svr bash -c "mongoimport --db ddbs --collection user --file user.dat"; \
	docker exec -it mongo_svr bash -c "mongoimport --db ddbs --collection article --file article.dat"; \
	docker exec -it mongo_svr bash -c "mongoimport --db ddbs --collection read --file read.dat"; \
	end_time=$$(date +%s%N | cut -c1-10); \
	echo "Move files time: $$((end_time-start_time)) seconds" >> $(TIME_FILE)

sharddata:
	@start_time=$$(date +%s%N | cut -c1-10); \
	docker cp shard_data.js mongo_svr:/shard_data.js; \
	docker exec -it mongo_svr mongosh "mongodb://${HOSTIP}:30000" --eval "load('shard_data.js')"; \
	end_time=$$(date +%s%N | cut -c1-10); \
	echo "Shard data time: $$((end_time-start_time)) seconds" >> $(TIME_FILE)

movefilestohdfs:
	@start_time=$$(date +%s%N | cut -c1-10); \
	docker cp data namenode:/; \
	docker exec -it namenode hdfs dfs -mkdir -p /data; \
	docker exec -it namenode hdfs dfs -put /data/articles /data; \
	end_time=$$(date +%s%N | cut -c1-10); \
	echo "Move files to HDFS time: $$((end_time-start_time)) seconds" >> $(TIME_FILE)

systemtesting:
	docker cp testing/test_user.js mongo_svr:/test_user.js;
	docker cp testing/test_user.js mongo_svr:/test_article.js;
	docker cp testing/test_user.js mongo_svr:/test_read.js;
	docker cp testing/test_user.js mongo_svr:/test_beread.js;
	docker cp testing/test_user.js mongo_svr:/test_poprank.js;
	docker exec -it mongo_svr mongosh "mongodb://${HOSTIP}:30000" --eval "load('test_user.js')";
	docker exec -it mongo_svr mongosh "mongodb://${HOSTIP}:30000" --eval "load('test_article.js')";
	docker exec -it mongo_svr mongosh "mongodb://${HOSTIP}:30000" --eval "load('test_read.js')";
	docker exec -it mongo_svr mongosh "mongodb://${HOSTIP}:30000" --eval "load('test_beread.js')";
	docker exec -it mongo_svr mongosh "mongodb://${HOSTIP}:30000" --eval "load('test_poprank.js')";


clean:
	@bash reset.sh
