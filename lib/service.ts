import { Express, Request, Response } from 'express';
import { Pool } from 'pg';

const GET_ENTITIES = ["fam", "user_device", "account", "tribe", "tribe_fam", "event", "proposal", "availability"];
const POST_ENTITIES = ["fam", "user_device", "account", "tribe", "tribe_fam", "event", "proposal", "availability"];
const PUT_ENTITIES = ["fam", "user_device", "account", "tribe", "tribe_fam", "event", "proposal", "availability"];
const DELETE_ENTITIES = ["fam", "user_device", "account", "tribe", "tribe_fam", "event", "proposal", "availability"];

/**
 * Sets up REST endpoints for the application based on the database schema.
 * @param app 
 * @param pool 
 */
export const setupEndpoints = async (app: Express, pool: Pool) => {
  // Query the schema for a list of public tables
  const schemaRes = await pool.query(`
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_type = 'BASE TABLE'
    AND table_name != 'pgmigrations'
  `);

  // Dynamically register routes for each table
  schemaRes.rows.forEach((row: { table_name: string }) => {
    const tableName = row.table_name;
    const route = `/${tableName}`;
    console.log(`Generating REST endpoints for table: ${tableName}`);

    // GET: List all records
    if (GET_ENTITIES.includes(tableName.toLowerCase())) {
      console.log(`Generating GET endpoint for table: ${tableName}`);
      
      app.get(route, async (req: Request, res: Response) => {
        try {
          const queryParams = req.query;
          const keys = Object.keys(queryParams);
          const values = Object.values(queryParams);

          let query = `SELECT * FROM "${tableName}"`;
          if (keys.length > 0) {
            const whereClause = keys.map((key, index) => `"${key}" = $${index + 1}`).join(' AND ');
            query += ` WHERE ${whereClause}`;
          }

          console.log(`Executing SELECT: ${query} with values:`, values);

          const result = await pool.query(query, values);
          res.send(result.rows);
        } catch (err: any) {
          res.status(500).send(err.message);
        }
      });

      app.get(`${route}/:id`, async (req: Request, res: Response) => {
        try {
          const { id } = req.params;
          const query = `SELECT * FROM "${tableName}" WHERE "id" = $1`;
          console.log(`Executing SELECT: ${query} for ID:`, id);

          const result = await pool.query(query, [id]);

          if (result.rows.length === 0) return res.status(404).send('Record not found');
          res.send(result.rows[0]);
        } catch (err: any) {
          res.status(500).send(err.message);
        }
      });
    }

    // POST: Create a new record
    if (POST_ENTITIES.includes(tableName.toLowerCase())) {
      console.log(`Generating POST endpoint for table: ${tableName}`);

      app.post(route, async (req: Request, res: Response) => {
        console.log(`POST request to ${route} with body:`, req.body);
        try {
          const keys = Object.keys(req.body);
          const values = Object.values(req.body);
          if (keys.length === 0) return res.status(400).send('No data provided');

          const columns = keys.map((k) => `"${k}"`).join(', ');
          const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
          const query = `INSERT INTO "${tableName}" (${columns}) VALUES (${placeholders}) RETURNING *`;
          console.log(`Executing INSERT: ${query} with values:`, values);

          const result = await pool.query(query, values);
          res.status(201).send(result.rows[0]);
        } catch (err: any) {
          res.status(500).send(err.message);
        }
      });
    }

    // PUT: Update a record
    if (PUT_ENTITIES.includes(tableName.toLowerCase())) {
      console.log(`Generating PUT endpoint for table: ${tableName}`);

      app.put(`${route}/:id`, async (req: Request, res: Response) => {
        try {
          const { id } = req.params;
          const keys = Object.keys(req.body);
          const values = Object.values(req.body);
          if (keys.length === 0) return res.status(400).send('No data provided');

          const setClause = keys.map((k, i) => `"${k}" = $${i + 1}`).join(', ');
          const query = `UPDATE "${tableName}" SET ${setClause} WHERE "id" = $${keys.length + 1} RETURNING *`;
          console.log(`Executing UPDATE: ${query} with values:`, values);

          const result = await pool.query(query, [...values, id]);
          if (result.rows.length === 0) return res.status(404).send('Record not found');
          res.send(result.rows[0]);
        } catch (err: any) {
          res.status(500).send(err.message);
        }
      });
    }

    // DELETE: Delete a record
    if (DELETE_ENTITIES.includes(tableName.toLowerCase())) {
      console.log(`Generating DELETE endpoint for table: ${tableName}`);

      app.delete(`${route}/:id`, async (req: Request, res: Response) => {
        try {
          const { id } = req.params;
          const query = `DELETE FROM "${tableName}" WHERE "id" = $1 RETURNING *`;
          console.log(`Executing DELETE: ${query} for ID:`, id);

          const result = await pool.query(query, [id]);
          if (result.rows.length === 0) return res.status(404).send('Record not found');
          res.send(result.rows[0]);
        } catch (err: any) {
          res.status(500).send(err.message);
        }
      });
    }
  });
};