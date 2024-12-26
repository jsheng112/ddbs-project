// this script contins all mongo shell commands required to populate and shard collections for this project
const ddbs = db.getSiblingDB("ddbs");

// Enable sharding for the database
sh.enableSharding("ddbs");

// shard user
try {
    // Ensure the collection has the correct index for the shard key
    ddbs.user.createIndex({"region": 1, "uid": 1});

    // Shard the collection
    sh.shardCollection("ddbs.user", {"region": 1, "uid": 1});

    // Disable balancing temporarily
    sh.disableBalancing("ddbs.user");

    // Add shard tags and tag ranges
    sh.addShardTag("dbms1", "BJ");
    sh.addTagRange(
        "ddbs.user",
        {"region": "Beijing", "uid": MinKey},
        {"region": "Beijing", "uid": MaxKey},
        "BJ"
    );
    sh.addShardTag("dbms2", "HK");
    sh.addTagRange(
        "ddbs.user",
        {"region": "Hong Kong", "uid": MinKey},
        {"region": "Hong Kong", "uid": MaxKey},
        "HK"
    );

    // Re-enable balancing
    sh.enableBalancing("ddbs.user");
    print("Sharded collection 'ddbs.user' successfully.");
} catch (e) {
    print("Error sharding collection 'ddbs.user': " + e.message);
}

// shard article
try {
    const ddbs = db.getSiblingDB("ddbs");

    // Ensure the collection has the correct index for the shard key
    ddbs.article.createIndex({"category": 1, "aid": 1});

    // Shard the collection
    sh.shardCollection("ddbs.article", {"category": 1, "aid": 1});

    // Disable balancing temporarily
    sh.disableBalancing("ddbs.article");

    // Add shard tags and tag ranges
    sh.addShardTag("dbms1", "SCI");
    sh.addTagRange(
        "ddbs.article",
        {"category": "science", "aid": MinKey},
        {"category": "science", "aid": MaxKey},
        "SCI"
    );

    sh.addShardTag("dbms2", "TECH");
    sh.addTagRange(
        "ddbs.article",
        {"category": "technology", "aid": MinKey},
        {"category": "technology", "aid": MaxKey},
        "TECH"
    );

    // Re-enable balancing
    sh.enableBalancing("ddbs.article");

    print("Sharded collection 'ddbs.article' successfully.");
} catch (e) {
    print("Error while sharding collection 'ddbs.article': " + e.message);
}


try {
    const ddbs = db.getSiblingDB("ddbs");

    // Create 'science_articles' and populate it with science articles
    ddbs.article.aggregate([
        { $match: { category: "science" } },
        { $merge: { into: "science_articles", whenMatched: "replace" } }
    ]);

    // Create the necessary index for sharding
    ddbs.science_articles.createIndex({ "category": 1, "aid": 1 });

    // Shard the 'science_articles' collection
    sh.shardCollection("ddbs.science_articles", { "category": 1, "aid": 1 });

    // Disable balancing temporarily
    sh.disableBalancing("ddbs.science_articles");

    // Add shard tags and ranges
    sh.addShardTag("dbms2", "SCI2");
    sh.addTagRange(
        "ddbs.science_articles",
        { "category": "science", "aid": MinKey },
        { "category": "science", "aid": MaxKey },
        "SCI2"
    );

    // Re-enable balancing
    sh.enableBalancing("ddbs.science_articles");

    print("Sharded collection 'ddbs.science_articles' successfully.");
} catch (e) {
    print("Error while sharding collection 'ddbs.science_articles': " + e.message);
}


try {
    const ddbs = db.getSiblingDB("ddbs");

    // Add region and category columns to read
    try {
        // Create uid_reg collection
        ddbs.user.aggregate([
            { $project: { uid: 1, region: 1 } },
            { $out: "uid_reg" }
        ]);
        print("Created 'uid_reg' collection successfully.");
    } catch (e) {
        print("Error creating 'uid_reg' collection: " + e.message);
    }

    try {
        // Update 'read' with region from 'uid_reg'
        ddbs.read.aggregate([
            { $lookup: { from: "uid_reg", localField: "uid", foreignField: "uid", as: "someField" } },
            { $addFields: { region: "$someField.region" } },
            { $unwind: "$region" },
            { $project: { someField: 0 } },
            { $out: "read" }
        ], { allowDiskUse: true });
        print("Updated 'read' collection with region successfully.");
    } catch (e) {
        print("Error updating 'read' with region: " + e.message);
    }

    try {
        // Create article_metadata collection
        ddbs.article.aggregate([
            { $project: { aid: 1, category: 1, timestamp: 1 } },
            { $out: "article_metadata" }
        ]);
        print("Created 'article_metadata' collection successfully.");
    } catch (e) {
        print("Error creating 'article_metadata' collection: " + e.message);
    }

    try {
        // Update 'read' with category and timestamp from 'article_metadata'
        ddbs.read.aggregate([
            { $lookup: { from: "article_metadata", localField: "aid", foreignField: "aid", as: "someField" } },
            { $addFields: { category: "$someField.category", article_ts: "$someField.timestamp" } },
            { $unwind: "$category" },
            { $unwind: "$article_ts" },
            { $project: { someField: 0 } },
            { $out: "read" }
        ], { allowDiskUse: true });
        print("Updated 'read' collection with category and timestamp successfully.");
    } catch (e) {
        print("Error updating 'read' with category and timestamp: " + e.message);
    }

    // Add readOrNot field and set it to 1 for all documents
    try {
        ddbs.read.updateMany({}, { $set: { readOrNot: "1" } });
        print("Added 'readOrNot' field and set it to 1 for all documents successfully.");
    } catch (e) {
        print("Error adding 'readOrNot' field: " + e.message);
    }

    // Shard read
    try {
        // Create index for the shard key
        ddbs.read.createIndex({ "region": 1, "id": 1 });

        // Shard the collection
        sh.shardCollection("ddbs.read", { "region": 1, "id": 1 });

        // Disable balancing temporarily
        sh.disableBalancing("ddbs.read");

        // Add shard tags and ranges
        sh.addShardTag("dbms1", "BJ");
        sh.addTagRange(
            "ddbs.read",
            { "region": "Beijing", "id": MinKey },
            { "region": "Beijing", "id": MaxKey },
            "BJ"
        );

        sh.addShardTag("dbms2", "HK");
        sh.addTagRange(
            "ddbs.read",
            { "region": "Hong Kong", "id": MinKey },
            { "region": "Hong Kong", "id": MaxKey },
            "HK"
        );

        // Re-enable balancing
        sh.enableBalancing("ddbs.read");
        print("Sharded collection 'ddbs.read' successfully.");
    } catch (e) {
        print("Error while sharding collection 'ddbs.read': " + e.message);
    }
} catch (e) {
    print("Unexpected error: " + e.message);
}


try {
    const ddbs = db.getSiblingDB("ddbs");

    // Populate beread
    try {
        ddbs.read.aggregate(
            [
                // Group by aid and create new fields with aggregated counts and arrays
                {
                    $group: {
                        _id: "$aid",
                        category: { $first: "$category" },
                        timestamp: { $first: "$article_ts" },
                        readNum: { $sum: { $toInt: "$readOrNot" } },
                        readUidList: { $addToSet: { $cond: { if: { $eq: ["$readOrNot", "1"] }, then: "$uid", else: "$$REMOVE" } } },
                        commentNum: { $sum: { $toInt: "$commentOrNot" } },
                        commentUidList: { $addToSet: { $cond: { if: { $eq: ["$commentOrNot", "1"] }, then: "$uid", else: "$$REMOVE" } } },
                        agreeNum: { $sum: { $toInt: "$agreeOrNot" } },
                        agreeUidList: { $addToSet: { $cond: { if: { $eq: ["$agreeOrNot", "1"] }, then: "$uid", else: "$$REMOVE" } } },
                        shareNum: { $sum: { $toInt: "$shareOrNot" } },
                        shareUidList: { $addToSet: { $cond: { if: { $eq: ["$shareOrNot", "1"] }, then: "$uid", else: "$$REMOVE" } } },
                    }
                },

                // Modify aid from integer to string
                { $addFields: { "aid": { $concat: ["a", "$_id"] } } },

                { $out: "beread" }
            ],
            { allowDiskUse: true }
        );
        print("Populated 'beread' collection successfully.");
    } catch (e) {
        print("Error populating 'beread' collection: " + e.message);
    }

    // Shard beread
    try {
        // Create index for the shard key
        ddbs.beread.createIndex({ "category": 1, "aid": 1 });

        // Shard the collection
        sh.shardCollection("ddbs.beread", { "category": 1, "aid": 1 });

        // Disable balancing temporarily
        sh.disableBalancing("ddbs.beread");

        // Add shard tags and ranges
        sh.addShardTag("dbms1", "SCI");
        sh.addTagRange(
            "ddbs.beread",
            { "category": "science", "aid": MinKey },
            { "category": "science", "aid": MaxKey },
            "SCI"
        );

        sh.addShardTag("dbms2", "TECH");
        sh.addTagRange(
            "ddbs.beread",
            { "category": "technology", "aid": MinKey },
            { "category": "technology", "aid": MaxKey },
            "TECH"
        );

        // Re-enable balancing
        sh.enableBalancing("ddbs.beread");
        print("Sharded 'ddbs.beread' collection successfully.");
    } catch (e) {
        print("Error sharding 'ddbs.beread': " + e.message);
    }

    // Populate beread_science
    try {
        ddbs.beread.aggregate([
            { $match: { category: "science" } },
            { $merge: { into: "beread_science", whenMatched: "replace" } }
        ]);
        print("Populated 'beread_science' collection successfully.");
    } catch (e) {
        print("Error populating 'beread_science' collection: " + e.message);
    }

    // Shard beread_science
    try {
        // Create index for the shard key
        ddbs.beread_science.createIndex({ "category": 1, "aid": 1 });

        // Shard the collection
        sh.shardCollection("ddbs.beread_science", { "category": 1, "aid": 1 });

        // Disable balancing temporarily
        sh.disableBalancing("ddbs.beread_science");

        // Add shard tags and ranges
        sh.addShardTag("dbms2", "SCI2");
        sh.addTagRange(
            "ddbs.beread_science",
            { "category": "science", "aid": MinKey },
            { "category": "science", "aid": MaxKey },
            "SCI2"
        );

        // Re-enable balancing
        sh.enableBalancing("ddbs.beread_science");
        print("Sharded 'ddbs.beread_science' collection successfully.");
    } catch (e) {
        print("Error sharding 'ddbs.beread_science': " + e.message);
    }
} catch (e) {
    print("Unexpected error: " + e.message);
}

function processPopularity(ddbs, granularity, outputCollection) {
    try {
        const dateFields = {
            monthly: { year: { $year: "$date" }, month: { $month: "$date" } },
            weekly: { year: { $isoWeekYear: "$date" }, week: { $isoWeek: "$date" } }, // Using isoWeekYear and isoWeek for weeks
            daily: { year: { $year: "$date" }, month: { $month: "$date" }, day: { $dayOfYear: "$date" } }
        };

        const granularitySuffix = {
            monthly: "m",
            weekly: "w",
            daily: "d"
        };

        ddbs.read.aggregate([
            // Project relevant fields
            { 
                $project: { 
                    date: { "$toDate": { "$toLong": "$timestamp" } },
                    aid: 1, readOrNot: 1, agreeOrNot: 1, commentOrNot: 1, shareOrNot: 1 
                }
            },

            // Add year, month (and week/day for granularity) and calculate popScore
            {
                $addFields: {
                    ...dateFields[granularity],
                    popScore: { 
                        $sum: [
                            { $toInt: "$readOrNot" }, 
                            { $toInt: "$agreeOrNot" }, 
                            { $toInt: "$commentOrNot" }, 
                            { $toInt: "$shareOrNot" }
                        ]
                    }
                }
            },

            // Add timestamp based on year, month, (week or day)
            {
                $addFields: { 
                    timestamp: {
                        $toString: {
                            $cond: {
                                if: { $eq: [granularity, "weekly"] }, 
                                then: {
                                    $subtract: [
                                        { $dateFromParts: {
                                            'isoWeekYear': "$year", 
                                            'isoWeek': "$week"
                                        }},
                                        new Date("1970-01-01")
                                    ]
                                },
                                else: {
                                    $subtract: [
                                        { $dateFromParts: {
                                            'year': "$year",
                                            'month': "$month",
                                            ...(granularity === "daily" ? { 'day': "$day" } : {})
                                        }},
                                        new Date("1970-01-01")
                                    ]
                                }
                            }
                        }
                    }
                }
            },

            // Group by timestamp and aid and compute popularity score
            {
                $group: {
                    _id: { "timestamp": "$timestamp", "aid": "$aid" },
                    popScoreAgg: { $sum: "$popScore" }
                }
            },

            // Sort by popScore and timestamp
            { $sort: { "_id.timestamp": 1, "popScoreAgg": -1 } },

            // Store articles in sorted order in array
            {
                $group: {
                    _id: "$_id.timestamp",
                    articleAidList: { $push: "$_id.aid" }
                }
            },

            // Keep only top 5 articles
            {
                $project: {
                    _id: { $concat: [granularitySuffix[granularity], { $toString: "$_id" }] },
                    timestamp: "$_id",
                    articleAidList: { $slice: ["$articleAidList", 5] },
                    temporalGranularity: granularity
                }
            },

            // Output the result to the specified collection
            { "$out": outputCollection }
        ], { allowDiskUse: true });

        // Print when processing for this granularity completes
        console.log(`Processing completed for poprank ${granularity} granularity successfully`);

    } catch (error) {
        console.error(`Error processing ${granularity}:`, error);
    }
}

// Process each granularity
processPopularity(ddbs, "monthly", "poprank_month");
processPopularity(ddbs, "weekly", "poprank_week");
processPopularity(ddbs, "daily", "poprank_day");

// Combine all into poprank
try {
    ddbs.poprank_month.find().forEach(doc => ddbs.poprank.insert(doc));
    ddbs.poprank_week.find().forEach(doc => ddbs.poprank.insert(doc));
    ddbs.poprank_day.find().forEach(doc => ddbs.poprank.insert(doc));

    ddbs.poprank.aggregate([{ $sort: { timestamp: 1 } }, { $out: "poprank" }]);

    // Print when the combination process is completed
    console.log("Combination into poprank completed successfully.");

} catch (error) {
    console.error("Error combining into poprank:", error);
}


function processPopularityByCategory(ddbs, category, granularity, outputCollection) {
    try {
        const dateFields = {
            monthly: { year: { $year: "$date" }, month: { $month: "$date" } },
            weekly: { year: { $isoWeekYear: "$date" }, week: { $isoWeek: "$date" } }, // Using isoWeekYear and isoWeek for weeks
            daily: { year: { $year: "$date" }, month: { $month: "$date" }, day: { $dayOfYear: "$date" } }
        };

        const granularitySuffix = {
            monthly: "m",
            weekly: "w",
            daily: "d"
        };

        ddbs.read.aggregate([
            // Match only the science articles
            { $match: { category: category } },

            // Project relevant fields
            { 
                $project: { 
                    date: { "$toDate": { "$toLong": "$timestamp" } },
                    aid: 1, readOrNot: 1, agreeOrNot: 1, commentOrNot: 1, shareOrNot: 1 
                }
            },

            // Add year, month, (and week/day for granularity) and calculate popScore
            {
                $addFields: {
                    ...dateFields[granularity],
                    popScore: { 
                        $sum: [
                            { $toInt: "$readOrNot" }, 
                            { $toInt: "$agreeOrNot" }, 
                            { $toInt: "$commentOrNot" }, 
                            { $toInt: "$shareOrNot" }
                        ]
                    }
                }
            },

            // Add timestamp based on year, month, (week or day)
            {
                $addFields: { 
                    timestamp: {
                        $toString: {
                            $cond: {
                                if: { $eq: [granularity, "weekly"] }, 
                                then: {
                                    $subtract: [
                                        { $dateFromParts: {
                                            'isoWeekYear': "$year", 
                                            'isoWeek': "$week"
                                        }},
                                        new Date("1970-01-01")
                                    ]
                                },
                                else: {
                                    $subtract: [
                                        { $dateFromParts: {
                                            'year': "$year",
                                            'month': "$month",
                                            ...(granularity === "daily" ? { 'day': "$day" } : {})
                                        }},
                                        new Date("1970-01-01")
                                    ]
                                }
                            }
                        }
                    }
                }
            },

            // Group by timestamp and aid and compute popularity score
            {
                $group: {
                    _id: { "timestamp": "$timestamp", "aid": "$aid" },
                    popScoreAgg: { $sum: "$popScore" }
                }
            },

            // Sort by popScore and timestamp
            { $sort: { "_id.timestamp": 1, "popScoreAgg": -1 } },

            // Store articles in sorted order in array
            {
                $group: {
                    _id: "$_id.timestamp",
                    articleAidList: { $push: "$_id.aid" }
                }
            },

            // Keep only top 5 articles
            {
                $project: {
                    _id: { $concat: [granularitySuffix[granularity], { $toString: "$_id" }] },
                    timestamp: "$_id",
                    articleAidList: { $slice: ["$articleAidList", 5] },
                    temporalGranularity: granularity
                }
            },

            // Output the result to the specified collection
            { "$out": outputCollection }
        ], { allowDiskUse: true });

        // Print when processing for this granularity completes
        console.log(`Processing for poprank ${category} ${granularity} granularity completed successfully.`);

    } catch (error) {
        console.error(`Error processing ${category} - ${granularity}:`, error);
    }
}

// Process each granularity for science category
processPopularityByCategory(ddbs, "science", "monthly", "poprank_science_month");
processPopularityByCategory(ddbs, "science", "weekly", "poprank_science_week");
processPopularityByCategory(ddbs, "science", "daily", "poprank_science_day");

// Combine all into poprank_science
try {
    ddbs.poprank_science_month.find().forEach(function (doc) {
        ddbs.poprank_science.insert(doc);
    });

    ddbs.poprank_science_week.find().forEach(function (doc) {
        ddbs.poprank_science.insert(doc);
    });

    ddbs.poprank_science_day.find().forEach(function (doc) {
        ddbs.poprank_science.insert(doc);
    });

    ddbs.poprank_science.aggregate([{ $sort: { timestamp: 1 } }, { $out: "poprank_science" }]);

    // Print when the combination process is completed
    console.log("Combination into poprank_science completed sucessfully.");

} catch (error) {
    console.error("Error combining into poprank_science:", error);
}

// Process each granularity for technology category
processPopularityByCategory(ddbs, "technology", "monthly", "poprank_tech_month");
processPopularityByCategory(ddbs, "technology", "weekly", "poprank_tech_week");
processPopularityByCategory(ddbs, "technology", "daily", "poprank_tech_day");

// Combine all into poprank_tech
try {
    ddbs.poprank_tech_month.find().forEach(function (doc) {
        ddbs.poprank_tech.insert(doc);
    });

    ddbs.poprank_tech_week.find().forEach(function (doc) {
        ddbs.poprank_tech.insert(doc);
    });

    ddbs.poprank_tech_day.find().forEach(function (doc) {
        ddbs.poprank_tech.insert(doc);
    });

    ddbs.poprank_tech.aggregate([{ $sort: { timestamp: 1 } }, { $out: "poprank_tech" }]);

    // Print when the combination process is completed
    console.log("Combination into poprank_tech completed successfully.");

} catch (error) {
    console.error("Error combining into poprank_tech:", error);
}

// Function to perform sharding operations
function shardCollection(ddbs, collectionName, shardTag, shardKey = { "_id": 1 }) {
    try {
        // Create index on the shard key
        ddbs[collectionName].createIndex(shardKey);

        // Shard the collection
        sh.shardCollection(`ddbs.${collectionName}`, shardKey);

        // Disable balancing during the sharding process
        sh.disableBalancing(`ddbs.${collectionName}`);

        // Add shard tag and range
        sh.addShardTag("dbms2", shardTag);
        sh.addTagRange(
            `ddbs.${collectionName}`,
            { "_id": MinKey },
            { "_id": MaxKey },
            shardTag
        );

        // Enable balancing after the sharding process
        sh.enableBalancing(`ddbs.${collectionName}`);

        console.log(`${collectionName} sharding completed successfully.`);
    } catch (error) {
        console.error(`Error sharding ${collectionName}:`, error);
    }
}

// Shard poprank
shardCollection(ddbs, "poprank", "POPALL");

// Shard poprank_tech
shardCollection(ddbs, "poprank_tech", "POPTECH");

// Handle poprank_science and poprank_science2 separately since they have different insert logic
try {
    // Copy documents from poprank_science to poprank_science2
    ddbs.poprank_science.find().forEach(function (doc) {
        ddbs.poprank_science2.insert(doc);
    });

    // Shard poprank_science
    shardCollection(ddbs, "poprank_science", "POPSCI");

    // Shard poprank_science2
    shardCollection(ddbs, "poprank_science2", "POPSCI2");

} catch (error) {
    console.error("Error processing poprank_science and poprank_science2:", error);
}
