//Query test
let startTime = performance.now();
db.ddbs.user.find({ "region": "Beijing" }).toArray();  // Query for users in Beijing
let endTime = performance.now();
let queryTime = endTime - startTime;  // Latency in milliseconds
print("Query Latency: " + queryTime + " ms");

// Insert test
startTime = performance.now();
db.ddbs.user.insertMany([
    { "region": "Beijing", "uid": 1, "name": "User1" },  // Replace with actual user data
    { "region": "Hong Kong", "uid": 2, "name": "User2" }  // Replace with actual user data
]);  
endTime = performance.now();
let insertTime = endTime - startTime;  // Latency in milliseconds
print("Insert Latency: " + insertTime + " ms");

//Update test
startTime = performance.now();
db.ddbs.user.updateMany(
    { "uid": { $in: [1, 2] } },  // Update only the inserted users
    { $set: { "name": "UpdatedUser" } }  // Update name for users in Beijing
);  
endTime = performance.now();
let updateTime = endTime - startTime;  // Latency in milliseconds
print("Update Latency: " + updateTime + " ms");

//Delete test
startTime = performance.now();
db.ddbs.user.deleteMany({ "uid": { $in: [1, 2] } });  // Delete only the inserted users
endTime = performance.now();
let deleteTime = endTime - startTime;  // Latency in milliseconds
print("Delete Latency: " + deleteTime + " ms");

//Throughput test for Query
let queryOperationsCount = 0;
let queryDuration = 1000;  // Duration in milliseconds
let queryStartTime = performance.now();

while (performance.now() - queryStartTime < queryDuration) {
    db.ddbs.user.find({ "region": "Beijing" }).toArray();  // Query for users in Beijing
    queryOperationsCount++;
}

let queryEndTime = performance.now();
let queryThroughput = queryOperationsCount / (queryDuration / 1000);  // Operations per second
print("Query Throughput: " + queryThroughput + " ops/sec");

//Throughput test for Insert
let insertOperationsCount = 0;
let insertDuration = 1000;  // Duration in milliseconds
let insertStartTime = performance.now();

while (performance.now() - insertStartTime < insertDuration) {
    db.ddbs.user.insertOne({ "region": "Beijing", "uid": insertOperationsCount + 3, "name": "ThroughputUser" });  // Insert users
    insertOperationsCount++;
}

let insertEndTime = performance.now();
let insertThroughput = insertOperationsCount / (insertDuration / 1000);  // Operations per second
print("Insert Throughput: " + insertThroughput + " ops/sec");

//Throughput test for Update
let updateOperationsCount = 0;
let updateDuration = 1000;  // Duration in milliseconds
let updateStartTime = performance.now();

while (performance.now() - updateStartTime < updateDuration) {
    db.ddbs.user.updateMany(
        { "uid": { $in: [1, 2] } },  // Update only the inserted users
        { $set: { "name": "UpdatedUser" } }  // Update name for users in Beijing
    );
    updateOperationsCount++;
}

let updateEndTime = performance.now();
let updateThroughput = updateOperationsCount / (updateDuration / 1000);  // Operations per second
print("Update Throughput: " + updateThroughput + " ops/sec");

//Throughput test for Delete
let deleteOperationsCount = 0;
let deleteDuration = 1000;  // Duration in milliseconds
let deleteStartTime = performance.now();

while (performance.now() - deleteStartTime < deleteDuration) {
    db.ddbs.user.deleteMany({ "uid": { $in: [1, 2] } });  // Delete only the inserted users
    deleteOperationsCount++;
}

let deleteEndTime = performance.now();
let deleteThroughput = deleteOperationsCount / (deleteDuration / 1000);  // Operations per second
print("Delete Throughput: " + deleteThroughput + " ops/sec");