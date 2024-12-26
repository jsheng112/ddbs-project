// Performance Testing Script for ddbs.poprank Collection

// Query Latency Test
let startTime = performance.now();
db.ddbs.poprank.find({ "temporalGranularity": "daily" }).toArray();  // Query daily granularity rankings
let endTime = performance.now();
let queryLatency = endTime - startTime;  // Latency in milliseconds
print("Query Latency: " + queryLatency + " ms");

// Insert Latency Test
startTime = performance.now();
db.ddbs.poprank.insertMany([
    {
        _id: "d1", 
        timestamp: new Date("2024-12-01T00:00:00Z"), 
        articleAidList: ["a101", "a102", "a103", "a104", "a105"], 
        temporalGranularity: "daily"
    },
    {
        _id: "w1", 
        timestamp: new Date("2024-12-03T00:00:00Z"), 
        articleAidList: ["a201", "a202", "a203", "a204", "a205"], 
        temporalGranularity: "weekly"
    }
]);  
endTime = performance.now();
let insertLatency = endTime - startTime;  // Latency in milliseconds
print("Insert Latency: " + insertLatency + " ms");

// Update Latency Test
startTime = performance.now();
db.ddbs.poprank.updateMany(
    { "temporalGranularity": "daily" },  // Update daily granularity entries
    { $set: { "articleAidList": ["a301", "a302", "a303", "a304", "a305"] } }  // Update articles
);  
endTime = performance.now();
let updateLatency = endTime - startTime;  // Latency in milliseconds
print("Update Latency: " + updateLatency + " ms");

// Delete Latency Test
startTime = performance.now();
db.ddbs.poprank.deleteMany({ "temporalGranularity": "daily" });  // Delete daily granularity entries
endTime = performance.now();
let deleteLatency = endTime - startTime;  // Latency in milliseconds
print("Delete Latency: " + deleteLatency + " ms");

// Query Throughput Test
let queryOpsCount = 0;
let queryDuration = 1000;  // Duration in milliseconds
let queryStartTime = performance.now();

while (performance.now() - queryStartTime < queryDuration) {
    db.ddbs.poprank.find({ "temporalGranularity": "daily" }).toArray();  // Query daily granularity rankings
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
    db.ddbs.poprank.insertOne({
        _id: `d${insertOpsCount + 2}`,
        timestamp: new Date(),
        articleAidList: ["a401", "a402", "a403", "a404", "a405"],
        temporalGranularity: "daily"
    });  // Insert daily granularity rankings
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
    db.ddbs.poprank.updateMany(
        { "temporalGranularity": "daily" },  // Update daily granularity entries
        { $set: { "articleAidList": ["a501", "a502", "a503", "a504", "a505"] } }  // Update articles
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
    db.ddbs.poprank.deleteMany({ "temporalGranularity": "daily" });  // Delete daily granularity entries
    deleteOpsCount++;
}

let deleteEndTime = performance.now();
let deleteThroughput = deleteOpsCount / (deleteDuration / 1000);  // Operations per second
print("Delete Throughput: " + deleteThroughput + " ops/sec");
