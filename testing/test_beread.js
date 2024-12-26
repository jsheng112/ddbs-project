// Performance Testing Script for ddbs.beread Collection

// Query Latency Test
let startTime = performance.now();
db.ddbs.beread.find({ "category": "science" }).toArray();  // Query for science category articles
let endTime = performance.now();
let queryLatency = endTime - startTime;  // Latency in milliseconds
print("Query Latency: " + queryLatency + " ms");

// Insert Latency Test
startTime = performance.now();
db.ddbs.beread.insertMany([
    { 
        "category": "science", 
        "aid": "a101", 
        "timestamp": new Date(), 
        "readNum": 5, 
        "readUidList": [1, 2, 3, 4, 5],
        "commentNum": 2, 
        "commentUidList": [1, 3],
        "agreeNum": 3, 
        "agreeUidList": [1, 2, 3],
        "shareNum": 1, 
        "shareUidList": [2]
    },
    { 
        "category": "technology", 
        "aid": "a102", 
        "timestamp": new Date(), 
        "readNum": 3, 
        "readUidList": [6, 7, 8],
        "commentNum": 1, 
        "commentUidList": [7],
        "agreeNum": 2, 
        "agreeUidList": [6, 8],
        "shareNum": 0, 
        "shareUidList": []
    }
]);  
endTime = performance.now();
let insertLatency = endTime - startTime;  // Latency in milliseconds
print("Insert Latency: " + insertLatency + " ms");

// Update Latency Test
startTime = performance.now();
db.ddbs.beread.updateMany(
    { "aid": { $in: ["a101", "a102"] } },  // Update only the inserted articles
    { $set: { "timestamp": new Date() } }  // Update timestamp
);  
endTime = performance.now();
let updateLatency = endTime - startTime;  // Latency in milliseconds
print("Update Latency: " + updateLatency + " ms");

// Delete Latency Test
startTime = performance.now();
db.ddbs.beread.deleteMany({ "aid": { $in: ["a101", "a102"] } });  // Delete only the inserted articles
endTime = performance.now();
let deleteLatency = endTime - startTime;  // Latency in milliseconds
print("Delete Latency: " + deleteLatency + " ms");

// Query Throughput Test
let queryOpsCount = 0;
let queryDuration = 1000;  // Duration in milliseconds
let queryStartTime = performance.now();

while (performance.now() - queryStartTime < queryDuration) {
    db.ddbs.beread.find({ "category": "science" }).toArray();  // Query for science category articles
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
    db.ddbs.beread.insertOne({
        "category": "science",
        "aid": `a${insertOpsCount + 103}`,
        "timestamp": new Date(),
        "readNum": 1,
        "readUidList": [insertOpsCount],
        "commentNum": 0,
        "commentUidList": [],
        "agreeNum": 1,
        "agreeUidList": [insertOpsCount],
        "shareNum": 0,
        "shareUidList": []
    });  // Insert articles
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
    db.ddbs.beread.updateMany(
        { "aid": { $in: ["a101", "a102"] } },
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
    db.ddbs.beread.deleteMany({ "aid": { $in: ["a101", "a102"] } });  // Delete only the inserted articles
    deleteOpsCount++;
}

let deleteEndTime = performance.now();
let deleteThroughput = deleteOpsCount / (deleteDuration / 1000);  // Operations per second
print("Delete Throughput: " + deleteThroughput + " ops/sec");
