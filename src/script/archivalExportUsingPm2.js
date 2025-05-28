const mongoose = require('mongoose');
const fs = require('fs');
const { Transform } = require('stream');
const path = require('path');
const { promisify } = require('util');

const collectionName = 'Recommendations';
const outputFile = path.join(__dirname, 'grad-archival-db.Recommendations.json');
const dbUri = 'mongodb+srv://grad-app-prod:N5pxJ8aART9FHcHW@gradright-cluster0-ab5bz.mongodb.net/grad-prod?retryWrites=true&w=majority';
const BATCH_SIZE = 1000;

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

// Function to check disk space
const stat = promisify(fs.statfs);
async function checkDiskSpace() {
    const stats = await stat(__dirname);
    const freeSpaceGB = (stats.bavail * stats.bsize) / (1024 * 1024 * 1024);
    console.log(`Available disk space: ${freeSpaceGB.toFixed(2)} GB`);
    if (freeSpaceGB < 50) {
        throw new Error('Insufficient disk space for export (minimum 50 GB required)');
    }
}

// Function to export collection
async function exportCollection() {
    let clusterConnection;
    console.log('File path:', outputFile);

    try {
        // Check disk space
        await checkDiskSpace();

        // Connect to MongoDB with retry logic
        const maxRetries = 3;
        let retries = 0;
        while (retries < maxRetries) {
            try {
                clusterConnection = await mongoose.createConnection(dbUri, {
                    useNewUrlParser: true,
                    useUnifiedTopology: true,
                    maxPoolSize: 20,
                    connectTimeoutMS: 60000,
                    socketTimeoutMS: 60000,
                }).asPromise();
                console.log('Connected to MongoDB');
                break;
            } catch (err) {
                retries++;
                console.error(`Connection attempt ${retries} failed: ${err.message}`);
                if (retries === maxRetries) throw new Error('Max retries reached for MongoDB connection');
                await new Promise(resolve => setTimeout(resolve, 5000));
            }
        }

        const gradStudentsDb = clusterConnection.useDb('grad-archival-db');
        const collection = gradStudentsDb.collection(collectionName);

        // Get total document count
        const totalDocs = await collection.countDocuments();
        console.log(`Total documents to export: ${totalDocs}`);

        let processedDocs = 0;

        // Create a readable stream
        const cursor = collection.find().batchSize(BATCH_SIZE).stream();

        // Create write stream
        const writeStream = fs.createWriteStream(outputFile, { encoding: 'utf8' });

        // Create JSON formatter
        const jsonFormatter = new JsonFormatter();

        // Pipe the streams
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
            if (processedDocs % 100000 === 0) {
                console.log(`Processed ${processedDocs} of ${totalDocs} documents (${((processedDocs / totalDocs) * 100).toFixed(2)}%)`);
            }
        });

    } catch (err) {
        console.error('Error in export process:', err);
        if (clusterConnection) await clusterConnection.close();
        process.exit(1); // Exit with error code for PM2 to detect failure
    }
}

// Run the export
exportCollection().catch(console.error);