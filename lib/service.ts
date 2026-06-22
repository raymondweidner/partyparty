import { Express, Request, Response } from 'express';
import { Pool } from 'pg';
import { getRecords, getRecordById, createRecord, updateRecord, deleteRecord, createAndSendNotification } from './data';
import { handleSqlErrorForRest } from './errorHandler';
import { logger } from './logger';

const GET_ENTITIES = ["member", "member_contact", "chat", "chat_member", "user_device", "tribe", "tribe_member", "proposal", "availability", "meetup", "notification"];
const POST_ENTITIES = ["member", "member_contact", "chat", "chat_member", "user_device", "tribe", "tribe_member", "proposal", "availability", "meetup", "notification"];
const PUT_ENTITIES = ["member", "member_contact", "chat", "chat_member", "user_device", "tribe", "tribe_member", "proposal", "availability", "meetup", "notification"];
const DELETE_ENTITIES = ["member", "member_contact", "chat", "chat_member", "user_device", "tribe", "tribe_member", "proposal", "availability", "meetup", "notification"];



export const setupPublicEndpoints = (app: Express, pool: Pool) => {
  app.get('/member/invite', async (req: Request, res: Response) => {
    try {
      const { email } = req.query;
      if (!email || typeof email !== 'string') {
        return res.status(400).send('Valid email query parameter is required');
      }

      const query = `SELECT * FROM "member" WHERE "email" = $1 AND "status" = 'invited'`;
      logger.info({ query, email }, 'Executing SELECT for member invite');

      const result = await pool.query(query, [email]);
      logger.info({ rows: result.rows }, 'Query Result');

      if (result.rows.length === 0) return res.status(404).send('Invited member not found');

      res.send({ name: result.rows[0].name, email: email, phone: result.rows[0].phone });
    } catch (err: any) {
      handleSqlErrorForRest(err, res);
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
  logger.info({ rows: schemaRes.rows }, 'Schema Query Result');

  // Dynamically register routes for each table
  schemaRes.rows.forEach((row: { table_name: string }) => {
    const tableName = row.table_name;
    const route = `/${tableName}`;
    logger.info({ table: tableName }, 'Generating REST endpoints');

    // GET: List all records
    if (GET_ENTITIES.includes(tableName.toLowerCase())) {
      logger.info({ table: tableName, method: 'GET' }, 'Generating endpoint');

      app.get(route, async (req: Request, res: Response) => {
        try {
          const queryParams = req.query;
          logger.info({ table: tableName, filters: queryParams }, 'Executing getRecords');

          const records = await getRecords(pool, tableName, queryParams);
          logger.info({ recordsCount: records.length }, 'Query Result');
          res.send(records);
        } catch (err: any) {
          handleSqlErrorForRest(err, res);
        }
      });

      app.get(`${route}/:id`, async (req: Request, res: Response) => {
        try {
          const { id } = req.params;
          logger.info({ table: tableName, id }, 'Executing getRecordById');

          const record = await getRecordById(pool, tableName, id);
          logger.info({ found: !!record }, 'Query Result');

          if (!record) return res.status(404).send('Record not found');
          res.send(record);
        } catch (err: any) {
          handleSqlErrorForRest(err, res);
        }
      });
    }

    // POST: Create a new record
    if (POST_ENTITIES.includes(tableName.toLowerCase())) {
      logger.info({ table: tableName, method: 'POST' }, 'Generating endpoint');

      app.post(route, async (req: Request, res: Response) => {
        logger.info({ route, body: req.body }, 'POST request received');
        try {
          const keys = Object.keys(req.body);
          if (keys.length === 0) return res.status(400).send('No data provided');

          let newRecord;
          if (tableName.toLowerCase() === 'notification') {
            logger.info({ table: tableName, values: req.body }, 'Executing createAndSendNotification');

            const memberId = req.body.member_id || req.body.user_id;
            if (!memberId) return res.status(400).send({ error: 'Bad Request: Missing member_id' });
            if (!req.body.title || !req.body.body) return res.status(400).send({ error: 'Bad Request: Missing title or body' });

            newRecord = await createAndSendNotification(
              pool,
              memberId,
              req.body.title,
              req.body.body,
              req.body.htmlBody,
              req.body.resourceType,
              req.body.resourceId,
              req.body.actionMode
            );
            logger.info({ notificationId: newRecord?.id }, 'Notification processed via POST');
          } else {
            logger.info({ table: tableName, values: req.body }, 'Executing createRecord');
            newRecord = await createRecord(pool, tableName, req.body);
            logger.info({ newRecordId: newRecord?.id }, 'Query Result');
          }

          res.status(201).send(newRecord);
        } catch (err: any) {
          handleSqlErrorForRest(err, res);
        }
      });
    }

    // PUT: Update a record
    if (PUT_ENTITIES.includes(tableName.toLowerCase())) {
      logger.info({ table: tableName, method: 'PUT' }, 'Generating endpoint');

      app.put(`${route}/:id`, async (req: Request, res: Response) => {
        try {
          const { id } = req.params;

          const updateData = { ...req.body };
          delete updateData.id;

          const keys = Object.keys(updateData);
          if (keys.length === 0) return res.status(400).send('No data provided');

          logger.info({ table: tableName, id, values: updateData }, 'Executing updateRecord');

          const newRecord = await updateRecord(pool, tableName, id, updateData);
          logger.info({ updatedRecordId: newRecord?.id }, 'Query Result');

          if (tableName.toLowerCase() === 'notification') {
            logger.info({ notificationId: newRecord?.id, updateData }, 'Notification processed via PUT');
          }

          if (!newRecord) return res.status(404).send('Record not found');

          res.send(newRecord);
        } catch (err: any) {
          handleSqlErrorForRest(err, res);
        }
      });
    }

    // DELETE: Delete a record
    if (DELETE_ENTITIES.includes(tableName.toLowerCase())) {
      logger.info({ table: tableName, method: 'DELETE' }, 'Generating endpoint');

      app.delete(`${route}/:id`, async (req: Request, res: Response) => {
        try {
          const { id } = req.params;
          logger.info({ table: tableName, id }, 'Executing deleteRecord');

          const record = await deleteRecord(pool, tableName, id);
          logger.info({ deleted: !!record }, 'Query Result');
          if (!record) return res.status(404).send('Record not found');
          res.send(record);
        } catch (err: any) {
          handleSqlErrorForRest(err, res);
        }
      });
    }
  });
};