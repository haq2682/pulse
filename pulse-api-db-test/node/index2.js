const express = require("express");
const mongoose = require("mongoose");
const cors = require("cors");

const app  = express();
const PORT = process.env.PORT || 3000;

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json());

// ── MongoDB connection ────────────────────────────────────────────────────────
const MONGO_URI = process.env.MONGO_URI || "mongodb://root:rootpassword@mongodb:27017/pulse-test?authSource=admin&directConnection=true";


mongoose
  .connect(MONGO_URI)
  .then(() => {
    console.log("Connected to MongoDB");

    app.listen(PORT, () =>
      console.log(`API listening on http://0.0.0.0:${PORT}`)
    );
  })
  .catch((err) => {
    console.error("MongoDB connection error:", err);
    process.exit(1);
  });

// ── Routes ────────────────────────────────────────────────────────────────────

// GET /data — returns ALL collections, each as a table_name + data array
app.get("/data", async (req, res) => {
  try {
    const db          = mongoose.connection.db;
    const collections = await db.listCollections().toArray();

    const tables = await Promise.all(
      collections.map(async ({ name }) => {
        const data = await db.collection(name).find({}, { projection: { _id: 0 } }).toArray();
        return { table_name: name, data };
      })
    );

    res.json({ tables });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /data/:collection — returns a single collection in the same shape
app.get("/data/:collection", async (req, res) => {
  try {
    const { collection } = req.params;
    const limit  = parseInt(req.query.limit) || 100;
    const skip   = parseInt(req.query.skip)  || 0;

    const db   = mongoose.connection.db;
    const data = await db
      .collection(collection)
      .find({}, { projection: { _id: 0 } })
      .skip(skip)
      .limit(limit)
      .toArray();

    res.json({ tables: [{ table_name: collection, data }] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /collections — utility: list collection names only
app.get("/collections", async (req, res) => {
  try {
    const collections = await mongoose.connection.db.listCollections().toArray();
    res.json(collections.map((c) => c.name));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});
