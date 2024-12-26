// Performance Testing Script for ddbs.read Collection

// Query Latency Test
let startTime = performance.now();
db.ddbs.read.find({ "region": "Beijing" }).toArray();  // Query for reads in Beijing
let endTime = performance.now();
let queryLatency = endTime - startTime;  // Latency in milliseconds
print("Query Latency: " + queryLatency + " ms");

// Insert Latency Test
startTime = performance.now();
db.ddbs.read.insertMany([
    { "region": "Beijing", "id": 1, "uid": 101, "aid": 201, "timestamp": new Date() },
    { "region": "Hong Kong", "id": 2, "uid": 102, "aid": 202, "timestamp": new Date() }
]);  
endTime = performance.now();
let insertLatency = endTime - startTime;  // Latency in milliseconds
print("Insert Latency: " + insertLatency + " ms");

// Update Latency Test
startTime = performance.now();
db.ddbs.read.updateMany(
    { "id": { $in: [1, 2] } },  // Update only the inserted reads
    { $set: { "timestamp": new Date() } }  // Update timestamp
);  
endTime = performance.now();
let updateLatency = endTime - startTime;  // Latency in milliseconds
print("Update Latency: " + updateLatency + " ms");

// Delete Latency Test
startTime = performance.now();
db.ddbs.read.deleteMany({ "id": { $in: [1, 2] } });  // Delete only the inserted reads
endTime = performance.now();
let deleteLatency = endTime - startTime;  // Latency in milliseconds
print("Delete Latency: " + deleteLatency + " ms");

// Query Throughput Test
let queryOpsCount = 0;
let queryDuration = 1000;  // Duration in milliseconds
let queryStartTime = performance.now();

while (performance.now() - queryStartTime < queryDuration) {
    db.ddbs.read.find({ "region": "Beijing" }).toArray();  // Query for reads in Beijing
    queryOpsCount++;
}

let queryEndTime = performance.now();
let queryThroughput = queryOpsCount / (queryDuration / 1000);  // Operations per second
print("Query Throughput: " + queryThroughput + " ops/sec");

// Insert Throughput Test
let insertOpsCount = 0;
let insertDuration = 1000;  // Duration in milliseconds
let insertStartTime = performance.now();

while (performance.now() - insertStartTime < insertDuration) {
    db.ddbs.read.insertOne({
        "region": "Beijing",
        "id": insertOpsCount + 3,
        "uid": 103 + insertOpsCount,
        "aid": 203 + insertOpsCount,
        "timestamp": new Date()
    });  // Insert reads
    insertOpsCount++;
}

let insertEndTime = performance.now();
let insertThroughput = insertOpsCount / (insertDuration / 1000);  // Operations per second
print("Insert Throughput: " + insertThroughput + " ops/sec");

// Update Throughput Test
let updateOpsCount = 0;
let updateDuration = 1000;  // Duration in milliseconds
let updateStartTime = performance.now();

while (performance.now() - updateStartTime < updateDuration) {
    db.ddbs.read.updateMany(
        { "id": { $in: [1, 2] } },  // Update only the inserted reads
        { $set: { "timestamp": new Date() } }  // Update timestamp
    );
    updateOpsCount++;
}

let updateEndTime = performance.now();
let updateThroughput = updateOpsCount / (updateDuration / 1000);  // Operations per second
print("Update Throughput: " + updateThroughput + " ops/sec");

// Delete Throughput Test
let deleteOpsCount = 0;
let deleteDuration = 1000;  // Duration in milliseconds
let deleteStartTime = performance.now();

while (performance.now() - deleteStartTime < deleteDuration) {
    db.ddbs.read.deleteMany({ "id": { $in: [1, 2] } });  // Delete only the inserted reads
    deleteOpsCount++;
}

let deleteEndTime = performance.now();
let deleteThroughput = deleteOpsCount / (deleteDuration / 1000);  // Operations per second
print("Delete Throughput: " + deleteThroughput + " ops/sec");
