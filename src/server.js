const express = require('express');
const mongoose = require('mongoose');
require('dotenv').config();
//routes

const parentRoute = require('./routes');

const app = express();

// Middleware
app.use(express.json());


console.log(process.env.MONGO_URI, "URI MongoDB!");
// Connect to MongoDB
mongoose.connect(process.env.MONGO_URI, {
    useNewUrlParser: true, useUnifiedTopology: true, useUnifiedTopology: true,
    serverSelectionTimeoutMS: 30000,
    socketTimeoutMS: 45000
})
    .then(() => console.log('MongoDB connected'))
    .catch((err) => console.log('MongoDB connection error:', err));

// Start the server
const PORT = process.env.PORT || 4002;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
