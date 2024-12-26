// Performance Testing Script for ddbs.article Collection

// Query Latency Test
let startTime = performance.now();
db.ddbs.article.find({ "category": "science" }).toArray();  // Query for science articles
let endTime = performance.now();
let queryLatency = endTime - startTime;  // Latency in milliseconds
print("Query Latency: " + queryLatency + " ms");

// Insert Latency Test
startTime = performance.now();
db.ddbs.article.insertMany([
    { "category": "science", "aid": 101, "title": "Science Article 1" },
    { "category": "technology", "aid": 102, "title": "Technology Article 2" }
]);  
endTime = performance.now();
let insertLatency = endTime - startTime;  // Latency in milliseconds
print("Insert Latency: " + insertLatency + " ms");

// Update Latency Test
startTime = performance.now();
db.ddbs.article.updateMany(
    { "aid": { $in: [101, 102] } },  // Update only the inserted articles
    { $set: { "title": "Updated Article" } }
);  
endTime = performance.now();
let updateLatency = endTime - startTime;  // Latency in milliseconds
print("Update Latency: " + updateLatency + " ms");

// Delete Latency Test
startTime = performance.now();
db.ddbs.article.deleteMany({ "aid": { $in: [101, 102] } });  // Delete only the inserted articles
endTime = performance.now();
let deleteLatency = endTime - startTime;  // Latency in milliseconds
print("Delete Latency: " + deleteLatency + " ms");

// Query Throughput Test
let queryOpsCount = 0;
let queryDuration = 1000;  // Test duration in milliseconds
let queryStartTime = performance.now();

while (performance.now() - queryStartTime < queryDuration) {
    db.ddbs.article.find({ "category": "science" }).toArray();  // Query for science articles
    queryOpsCount++;
}

let queryEndTime = performance.now();
let queryThroughput = queryOpsCount / (queryDuration / 1000);  // Operations per second
print("Query Throughput: " + queryThroughput + " ops/sec");

// Insert Throughput Test
let insertOpsCount = 0;
let insertDuration = 1000;  // Test duration in milliseconds
let insertStartTime = performance.now();

while (performance.now() - insertStartTime < insertDuration) {
    db.ddbs.article.insertOne({ "category": "science", "aid": insertOpsCount + 103, "title": "Throughput Article" });
    insertOpsCount++;
}

let insertEndTime = performance.now();
let insertThroughput = insertOpsCount / (insertDuration / 1000);  // Operations per second
print("Insert Throughput: " + insertThroughput + " ops/sec");

// Update Throughput Test
let updateOpsCount = 0;
let updateDuration = 1000;  // Test duration in milliseconds
let updateStartTime = performance.now();

while (performance.now() - updateStartTime < updateDuration) {
    db.ddbs.article.updateMany(
        { "aid": { $in: [101, 102] } },
        { $set: { "title": "Updated Throughput Article" } }
    );
    updateOpsCount++;
}

let updateEndTime = performance.now();
let updateThroughput = updateOpsCount / (updateDuration / 1000);  // Operations per second
print("Update Throughput: " + updateThroughput + " ops/sec");

// Delete Throughput Test
let deleteOpsCount = 0;
let deleteDuration = 1000;  // Test duration in milliseconds
let deleteStartTime = performance.now();

while (performance.now() - deleteStartTime < deleteDuration) {
    db.ddbs.article.deleteMany({ "aid": { $in: [101, 102] } });
    deleteOpsCount++;
}

let deleteEndTime = performance.now();
let deleteThroughput = deleteOpsCount / (deleteDuration / 1000);  // Operations per second
print("Delete Throughput: " + deleteThroughput + " ops/sec");
