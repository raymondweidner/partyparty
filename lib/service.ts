import { Express, Request, Response } from 'express';
import { Pool } from 'pg';
import * as nodemailer from 'nodemailer';
import { config } from './config';

const GET_ENTITIES = ["member", "member_contact", "chat", "chat_member", "user_device", "tribe", "tribe_member", "proposal", "availability", "meetup"];
const POST_ENTITIES = ["member", "member_contact", "chat", "chat_member", "user_device", "tribe", "tribe_member", "proposal", "availability", "meetup"];
const PUT_ENTITIES = ["member", "member_contact", "chat", "chat_member", "user_device", "tribe", "tribe_member", "proposal", "availability", "meetup"];
const DELETE_ENTITIES = ["member", "member_contact", "chat", "chat_member", "user_device", "tribe", "tribe_member", "proposal", "availability", "meetup"];

export const setupPublicEndpoints = (app: Express, pool: Pool) => {
  app.get('/member/invite', async (req: Request, res: Response) => {
    try {
      const { email } = req.query;
      if (!email || typeof email !== 'string') {
        return res.status(400).send('Valid email query parameter is required');
      }

      const query = `SELECT "name" FROM "member" WHERE "email" = $1 AND "status" = 'invited'`;
      console.log(`Executing SELECT: ${query} with email:`, email);

      const result = await pool.query(query, [email]);
      console.log('Query Result:', result.rows);

      if (result.rows.length === 0) return res.status(404).send('Invited member not found');
      
      res.send({ name: result.rows[0].name, email: email });
    } catch (err: any) {
      res.status(500).send(err.message);
    }
  });
};

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
  console.log('Query Result:', schemaRes.rows);

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
          console.log('Query Result:', result.rows);
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
          console.log('Query Result:', result.rows);

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
          console.log('Query Result:', result.rows);
          const newRecord = result.rows[0];

          if (tableName.toLowerCase() === 'member' && newRecord.status === 'invited') {
            const email = newRecord.email;
            // Configure transporter
            const transporter = nodemailer.createTransport(config.email);
            
            // Down in your POST endpoint:
            const inviteLink = `${config.app.url}/login?invite=${encodeURIComponent(email)}`;
            
            await transporter.sendMail({
              from: '"PartyParty" <noreply@partyparty.com>',
              to: email,
              subject: "You're invited to PartyParty!",
              text: `You have been invited! Click to join!`,
              html: `<p>You have been invited! <a href="${inviteLink}">Click to join!</a></p>`,
            });
          }

          if (tableName.toLowerCase() === 'chat_member' && newRecord.status === 'invited') {
            const { member_id, chat_id } = newRecord;

            // Get member's email
            const memberRes = await pool.query('SELECT "email" FROM "member" WHERE "id" = $1', [member_id]);
            
            if (memberRes.rows.length > 0) {
              const email = memberRes.rows[0].email;

              // Get chat title and URL
              const chatRes = await pool.query('SELECT "title", "url" FROM "chat" WHERE "id" = $1', [chat_id]);
              const chatTitle = chatRes.rows.length > 0 ? chatRes.rows[0].title : 'a group chat';
              const chatLink = chatRes.rows.length > 0 && chatRes.rows[0].url ? chatRes.rows[0].url : `${config.app.url}/chat/${chat_id}`;

              const transporter = nodemailer.createTransport(config.email);

              await transporter.sendMail({
                from: '"PartyParty" <noreply@partyparty.com>',
                to: email,
                subject: `You've been invited to a chat on PartyParty!`,
                text: `You have been invited to join "${chatTitle}". Click here to see the chat: ${chatLink}`,
                html: `<p>You have been invited to join "<strong>${chatTitle}</strong>". <a href="${chatLink}">Click here to see the chat.</a></p>`,
              });
            } else {
              console.error(`Could not find member with id ${member_id} to send chat invite email.`);
            }
          }

          res.status(201).send(newRecord);
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
          
          const updateData = { ...req.body };
          delete updateData.id;

          const keys = Object.keys(updateData);
          const values = Object.values(updateData);
          if (keys.length === 0) return res.status(400).send('No data provided');

          const setClause = keys.map((k, i) => `"${k}" = $${i + 1}`).join(', ');
          const query = `UPDATE "${tableName}" SET ${setClause} WHERE "id" = $${keys.length + 1} RETURNING *`;
          console.log(`Executing UPDATE: ${query} with values:`, values);

          const result = await pool.query(query, [...values, id]);
          console.log('Query Result:', result.rows);
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
          console.log('Query Result:', result.rows);
          if (result.rows.length === 0) return res.status(404).send('Record not found');
          res.send(result.rows[0]);
        } catch (err: any) {
          res.status(500).send(err.message);
        }
      });
    }
  });
};