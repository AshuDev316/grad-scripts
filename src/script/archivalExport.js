const mongoose = require("mongoose");

const { MongoClient } = require('mongodb');
const fs = require('fs');
const { Transform } = require('stream');
const path = require('path');

const collectionName = 'Recommendations'; // Replace with your collection name
const outputFile = path.join(__dirname, 'grad-archival-db.Recommendations.json');

const dbUri = "mongodb+srv://grad-app-prod:N5pxJ8aART9FHcHW@gradright-cluster0-ab5bz.mongodb.net/grad-prod?retryWrites=true&w=majority";
// Configuration for batch processing
const BATCH_SIZE = 1000; // Number of documents to process per batch

// Custom Transform stream to format JSON
class JsonFormatter extends Transform {
    constructor() {
        super({ objectMode: true });
        this.isFirst = true;
    }

    _transform(doc, encoding, callback) {
        try {
            const jsonString = JSON.stringify(doc);
            if (this.isFirst) {
                this.push('[' + jsonString);
                this.isFirst = false;
            } else {
                this.push(',' + jsonString);
            }
            callback();
        } catch (err) {
            callback(err);
        }
    }

    _flush(callback) {
        this.push(']');
        callback();
    }
}

// Function to export collection
async function exportCollection() {
    let clusterConnection;
    console.log("filepath", outputFile);
    try {
        clusterConnection = await mongoose.createConnection(dbUri, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
            maxPoolSize: 10
        }).asPromise();

        const gradStudentsDb = clusterConnection.useDb("grad-archival-db");
        const collection = gradStudentsDb.collection(collectionName);

        // Get total document count for progress tracking
        const totalDocs = await collection.countDocuments();
        console.log(`Total documents to export: ${totalDocs}`);

        let processedDocs = 0;

        // Create a readable stream from the collection
        const cursor = collection.find().batchSize(BATCH_SIZE).stream();

        // Create write stream for output file
        const writeStream = fs.createWriteStream(outputFile, { encoding: 'utf8' });

        // Create JSON formatter transform stream
        const jsonFormatter = new JsonFormatter();

        // Pipe the cursor stream through JSON formatter to file
        cursor
            .pipe(jsonFormatter)
            .pipe(writeStream)
            .on('finish', () => {
                console.log(`Export completed. Total documents exported: ${processedDocs}`);
                clusterConnection.close();
            })
            .on('error', (err) => {
                console.error('Error during export:', err);
                clusterConnection.close();
            });

        // Track progress
        cursor.on('data', () => {
            processedDocs++;
            if (processedDocs % 100000 === 0) { // Log every 100,000 documents
                console.log(`Processed ${processedDocs} of ${totalDocs} documents (${((processedDocs / totalDocs) * 100).toFixed(2)}%)`);
            }
        });

    } catch (err) {
        console.error('Error in export process:', err);
        if (clusterConnection) await clusterConnection.close();
    }
}

// Run the export
exportCollection().catch(console.error);