const GET_ENTITIES = ["host", "guest", "party", "invite", "channel", "channel_membership", "channel_message", "user_device"];
const POST_ENTITIES = ["host", "guest", "party", "invite", "channel", "channel_membership", "channel_message", "user_device"];
const PUT_ENTITIES = ["host", "guest", "party", "invite", "channel", "channel_membership", "user_device"];
const DELETE_ENTITIES = ["host", "guest", "party", "invite", "channel", "channel_membership", "user_device"];

const express = require('express');
const { Pool } = require('pg');
const cors = require('cors'); // Import the cors package
const app = express();
const port = process.env.PORT || 3000;
const admin = require('firebase-admin');
const validateFirebaseIdToken = require('./authMiddleware');

admin.initializeApp(); // Ensure Firebase Admin is initialized

// Enable CORS for all routes and allow Express to parse JSON request bodies
app.use(cors());
app.use(express.json());
app.use(validateFirebaseIdToken);

const pool = new Pool({
  // Default to the emulator's port (5432) if DATABASE_URL is not set
  connectionString: process.env.DATABASE_URL || 'postgresql://postgres:@127.0.0.1:5432/postgres',
});

app.get('/', (req, res) => {
  res.send('Hello World!');
});

app.get('/now', async (req, res) => {
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
    } catch (err) {
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
    // Query the schema for a list of public tables
    const schemaRes = await pool.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE'
      AND table_name != 'pgmigrations'
    `);

    // Dynamically register routes for each table
    schemaRes.rows.forEach((row) => {
      const tableName = row.table_name;
      const route = `/${tableName}`;
      console.log(`Generating REST endpoints for table: ${tableName}`);

      // GET: List all records
      if (GET_ENTITIES.includes(tableName.toLowerCase())) {
        console.log(`Generating GET endpoint for table: ${tableName}`);
        
        app.get(route, async (req, res) => {
          try {
            const queryParams = req.query;
            const keys = Object.keys(queryParams);
            const values = Object.values(queryParams);

            let query = `SELECT * FROM "${tableName}"`;
            if (keys.length > 0) {
              const whereClause = keys.map((key, index) => `"${key}" = $${index + 1}`).join(' AND ');
              query += ` WHERE ${whereClause}`;
            }

            console.log(`Executing query: ${query} with values: ${values}`);

            const result = await pool.query(query, values);
            res.send(result.rows);
          } catch (err) {
            res.status(500).send(err.message);
          }
        });

        app.get(`${route}/:id`, async (req, res) => {
          try {
            const { id } = req.params;
            const query = `SELECT * FROM "${tableName}" WHERE "id" = $1`;
            const result = await pool.query(query, [id]);

            if (result.rows.length === 0) return res.status(404).send('Record not found');
            res.send(result.rows[0]);
          } catch (err) {
            res.status(500).send(err.message);
          }
        });
      }

      // POST: Create a new record
      if (POST_ENTITIES.includes(tableName.toLowerCase())) {
        console.log(`Generating POST endpoint for table: ${tableName}`);

        app.post(route, async (req, res) => {
          try {
            const keys = Object.keys(req.body);
            const values = Object.values(req.body);
            if (keys.length === 0) return res.status(400).send('No data provided');

            const columns = keys.map((k) => `"${k}"`).join(', ');
            const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
            const query = `INSERT INTO "${tableName}" (${columns}) VALUES (${placeholders}) RETURNING *`;

            const result = await pool.query(query, values);
            res.status(201).send(result.rows[0]);
          } catch (err) {
            res.status(500).send(err.message);
          }
        });
      }

      // PUT: Update a record
      if (PUT_ENTITIES.includes(tableName.toLowerCase())) {
        console.log(`Generating PUT endpoint for table: ${tableName}`);

        app.put(`${route}/:id`, async (req, res) => {
          try {
            const { id } = req.params;
            const keys = Object.keys(req.body);
            const values = Object.values(req.body);
            if (keys.length === 0) return res.status(400).send('No data provided');

            const setClause = keys.map((k, i) => `"${k}" = $${i + 1}`).join(', ');
            const query = `UPDATE "${tableName}" SET ${setClause} WHERE "id" = $${keys.length + 1} RETURNING *`;

            const result = await pool.query(query, [...values, id]);
            if (result.rows.length === 0) return res.status(404).send('Record not found');
            res.send(result.rows[0]);
          } catch (err) {
            res.status(500).send(err.message);
          }
        });
      }

      // DELETE: Delete a record
      if (DELETE_ENTITIES.includes(tableName.toLowerCase())) {
        console.log(`Generating DELETE endpoint for table: ${tableName}`);

        app.delete(`${route}/:id`, async (req, res) => {
          try {
            const { id } = req.params;
            const result = await pool.query(`DELETE FROM "${tableName}" WHERE "id" = $1 RETURNING *`, [id]);
            if (result.rows.length === 0) return res.status(404).send('Record not found');
            res.send(result.rows[0]);
          } catch (err) {
            res.status(500).send(err.message);
          }
        });
      }
    });

    app.listen(port, () => {
      console.log(`Example app listening on port ${port}`);
    });
  } catch (err) {
    console.error('Failed to start server:', err);
    process.exit(1);
  }
}

startServer();
