import { Express, Request, Response } from 'express';
import { Pool } from 'pg';
import * as admin from 'firebase-admin';

const GET_ENTITIES = ["host", "guest", "party", "invite", "channel", "channel_membership", "channel_message", "user_device"];
const POST_ENTITIES = ["host", "guest", "party", "invite", "channel", "channel_membership", "channel_message", "user_device"];
const PUT_ENTITIES = ["host", "guest", "party", "invite", "channel", "channel_membership", "user_device"];
const DELETE_ENTITIES = ["host", "guest", "party", "invite", "channel", "channel_membership", "user_device"];

/**
 * Sets up REST endpoints for the application based on the database schema.
 * @param app 
 * @param pool 
 */
export const setupEndpoints = async (app: Express, pool: Pool) => {
  let defaultChannelId: string | null = null;

  // Seed default channel
  try {
    const tableCheck = await pool.query(`
      SELECT table_name FROM information_schema.tables 
      WHERE table_schema = 'public' AND table_name = 'channel'
    `);
    if (tableCheck.rows.length > 0) {
      const res = await pool.query(`SELECT id FROM "channel" WHERE "name" = $1`, ['ALL']);
      if (res.rows.length === 0) {
        console.log('Seeding default channel: ALL');
        const insertRes = await pool.query(`INSERT INTO "channel" ("name", "description", "type", "created_on") VALUES ($1, $2, $3, NOW()) RETURNING id`, ['ALL', '', 'chat']);
        defaultChannelId = insertRes.rows[0].id;
      } else {
        defaultChannelId = res.rows[0].id;
      }
    }
  } catch (err: any) {
    console.warn('Error seeding default channel:', err.message);
  }

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

          console.log(`Executing query: ${query} with values: ${values}`);

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
          console.log(`Executing query: ${query} with values: ${values}`);

          const result = await pool.query(query, values);

          if (tableName === 'guest' && defaultChannelId) {
            try {
              await pool.query(`INSERT INTO "channel_membership" ("channel_id", "guest_id") VALUES ($1, $2)`, [defaultChannelId, result.rows[0].id]);
              console.log(`Added guest ${result.rows[0].id} to default channel`);
            } catch (err: any) {
              console.error(`Failed to add guest to default channel: ${err.message}`);
            }
          }

          if (tableName === 'channel_message') {
            console.log('New channel message posted, sending FCM notifications to channel members...');
            try {
              const message = result.rows[0];
              const tokenRes = await pool.query(`
                SELECT DISTINCT ud.token 
                FROM "channel_membership" cm
                JOIN "guest" g ON cm.guest_id = g.id
                JOIN "user_device" ud ON g.user_id = ud.user_id
                WHERE cm.channel_id = $1 AND g.id != $2
              `, [message.channel_id, message.sender_id]);

              const tokens = tokenRes.rows.map((r: any) => r.token);
              console.log(`Found ${tokens.length} device tokens to notify`, tokens);
              if (tokens.length > 0) {
                const messageResponse = await admin.messaging().sendEachForMulticast({
                  tokens,
                  notification: {
                    title: 'New Message',
                    body: message.content
                  },
                  webpush: {
                    fcmOptions: {
                      link: '/'
                    }
                  }
                });
                console.log(`FCM notifications sent, response:`, messageResponse);
                if (messageResponse.failureCount > 0) {
                  messageResponse.responses.forEach((resp, idx) => {
                    if (!resp.success) {
                      console.error(`Failed to send to ${tokens[idx]}: ${resp.error}`);
                    }
                  });
                }
              }
            } catch (err: any) {
              console.error(`Failed to send FCM notifications: ${err.message}`);
            }
          }

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
          const result = await pool.query(`DELETE FROM "${tableName}" WHERE "id" = $1 RETURNING *`, [id]);
          if (result.rows.length === 0) return res.status(404).send('Record not found');
          res.send(result.rows[0]);
        } catch (err: any) {
          res.status(500).send(err.message);
        }
      });
    }
  });
};