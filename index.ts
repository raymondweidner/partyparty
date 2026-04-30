import express = require('express');
import { Pool } from 'pg';
import cors = require('cors');
import * as admin from 'firebase-admin';
import * as fs from 'fs';
import * as path from 'path';
import { setupEndpoints } from './lib/service';
const validateFirebaseIdToken = require('./authMiddleware');

const app = express();
const port = process.env.PORT || 3000;

const serviceAccountPath = path.resolve('service-account.json');

if (fs.existsSync(serviceAccountPath)) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccountPath),
  });
} else {
  admin.initializeApp(); // Fallback to default credentials (e.g. env vars or Cloud environment)
}

// Enable CORS for all routes and allow Express to parse JSON request bodies
app.use(cors());
app.use(express.json());
app.use(validateFirebaseIdToken);

const pool = new Pool({
  // Default to the emulator's port (5432) if DATABASE_URL is not set
  connectionString: process.env.DATABASE_URL || 'postgresql://postgres:@127.0.0.1:5432/postgres',
});

app.get('/', (req: express.Request, res: express.Response) => {
  res.send('Hello World!');
});

app.get('/now', async (req: express.Request, res: express.Response) => {
  try {
    const result = await pool.query('SELECT NOW()');
    res.send(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).send('Error connecting to database');
  }
});

async function startServer() {
  let retries = 10;
  while (retries > 0) {
    try {
      // Attempt to connect/query to verify DB is up
      await pool.query('SELECT NOW()');
      console.log('Connected to database');
      break;
    } catch (err: any) {
      console.error(`Database connection failed (retries left: ${retries}):`, err.message);
      retries--;
      if (retries === 0) {
        console.error('Could not connect to database. Exiting.');
        process.exit(1);
      }
      await new Promise((res) => setTimeout(res, 2000));
    }
  }

  try {
    await setupEndpoints(app, pool);

    app.listen(port, () => {
      console.log(`Example app listening on port ${port}`);
    });
  } catch (err) {
    console.error('Failed to start server:', err);
    process.exit(1);
  }
}

startServer();