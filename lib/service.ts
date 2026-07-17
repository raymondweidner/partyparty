import { Express, Request, Response } from 'express';
import { Pool } from 'pg';
import { getRecords, getRecordById, createRecord, updateRecord, deleteRecord, createAndSendNotification, handleDriveAuthError } from './data';
import { handleSqlErrorForRest } from './errorHandler';
import { logger } from './logger';
import Busboy from 'busboy';
import { getDriveClient, createFolder, uploadFileStream, deleteFile } from './googleDriveService';

const GET_ENTITIES = ["member", "member_contact", "chat", "chat_member", "user_device", "tribe", "tribe_member", "proposal", "availability", "meetup", "meetup_event", "notification", "member_alert_preference", "poll", "poll_entry", "poll_vote", "poll_winner", "help_registry", "registry_item", "tribal_council", "event_check_in"];
const POST_ENTITIES = ["member", "member_contact", "chat", "chat_member", "user_device", "tribe", "tribe_member", "proposal", "availability", "meetup", "meetup_event", "notification", "member_alert_preference", "poll", "poll_entry", "poll_vote", "poll_winner", "help_registry", "registry_item", "tribal_council", "event_check_in"];
const PUT_ENTITIES = ["member", "member_contact", "chat", "chat_member", "user_device", "tribe", "tribe_member", "proposal", "availability", "meetup", "meetup_event", "notification", "member_alert_preference", "poll", "poll_entry", "poll_vote", "poll_winner", "help_registry", "registry_item", "tribal_council", "event_check_in"];
const DELETE_ENTITIES = ["member", "member_contact", "chat", "chat_member", "user_device", "tribe", "tribe_member", "proposal", "availability", "meetup", "meetup_event", "notification", "member_alert_preference", "poll", "poll_entry", "poll_vote", "poll_winner", "help_registry", "registry_item", "tribal_council", "event_check_in"];



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

  function getDistanceFromLatLonInM(lat1: number, lon1: number, lat2: number, lon2: number) {
    var R = 6371000; // Radius of the earth in m
    var dLat = (lat2 - lat1) * Math.PI / 180;
    var dLon = (lon2 - lon1) * Math.PI / 180;
    var a = 
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * 
      Math.sin(dLon / 2) * Math.sin(dLon / 2)
      ; 
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)); 
    var d = R * c; // Distance in m
    return d;
  }

  app.post('/meetup_event/:id/checkin', async (req: Request, res: Response) => {
    try {
      const eventId = req.params.id;
      const { latitude, longitude } = req.body;
      const firebaseUid = (req as any).user?.uid;
      
      if (!firebaseUid) return res.status(401).send('Unauthorized');
      if (latitude === undefined || longitude === undefined) return res.status(400).send('Missing latitude or longitude');

      const memberRes = await pool.query('SELECT id FROM "member" WHERE user_id = $1', [firebaseUid]);
      if (memberRes.rows.length === 0) return res.status(404).send('Member not found');
      const memberId = memberRes.rows[0].id;

      const eventRes = await pool.query('SELECT latitude, longitude, geofence_radius_meters FROM "meetup_event" WHERE id = $1', [eventId]);
      if (eventRes.rows.length === 0) return res.status(404).send('Event not found');
      const event = eventRes.rows[0];

      if (event.latitude != null && event.longitude != null && event.geofence_radius_meters != null) {
        const distance = getDistanceFromLatLonInM(latitude, longitude, event.latitude, event.longitude);
        if (distance > event.geofence_radius_meters) {
          return res.status(403).send({ error: 'You are too far from the event to check in', distance });
        }
      }

      // Upsert check-in
      const existing = await pool.query('SELECT id FROM "event_check_in" WHERE meetup_event_id = $1 AND member_id = $2', [eventId, memberId]);
      let checkinRecord;
      if (existing.rows.length > 0) {
         checkinRecord = await updateRecord(pool, 'event_check_in', existing.rows[0].id, { status: 'checked_in', check_in_time: new Date() });
      } else {
         checkinRecord = await createRecord(pool, 'event_check_in', { meetup_event_id: eventId, member_id: memberId, status: 'checked_in', check_in_time: new Date() });
      }

      res.status(200).send(checkinRecord);
    } catch (err: any) {
      handleSqlErrorForRest(err, res);
    }
  });

  app.post('/meetup_event/:id/checkout', async (req: Request, res: Response) => {
    try {
      const eventId = req.params.id;
      const firebaseUid = (req as any).user?.uid;
      if (!firebaseUid) return res.status(401).send('Unauthorized');

      const memberRes = await pool.query('SELECT id FROM "member" WHERE user_id = $1', [firebaseUid]);
      if (memberRes.rows.length === 0) return res.status(404).send('Member not found');
      const memberId = memberRes.rows[0].id;

      const existing = await pool.query('SELECT id FROM "event_check_in" WHERE meetup_event_id = $1 AND member_id = $2', [eventId, memberId]);
      if (existing.rows.length > 0) {
         await updateRecord(pool, 'event_check_in', existing.rows[0].id, { status: 'checked_out', check_out_time: new Date() });
      }

      // Clear from firestore
      const admin = require('firebase-admin');
      if (admin.apps.length > 0) {
         await admin.firestore().collection('events').doc(eventId).collection('locations').doc(memberId).delete().catch(() => {});
      }

      res.status(200).send({ success: true });
    } catch (err: any) {
      handleSqlErrorForRest(err, res);
    }
  });

  app.post('/media/:filename', (req: Request, res: Response) => {
    const filename = req.params.filename;
    const meetupId = req.query.meetup_id as string;
    const pollId = req.query.poll_id as string;


    if (!meetupId) {
      return res.status(400).send('meetup_id query parameter is required');
    }

    const firebaseUid = (req as any).user?.uid;
    if (!firebaseUid) return res.status(401).send('Unauthorized');

    let bb: any;
    try {
      bb = Busboy({ headers: req.headers, limits: { fileSize: 10 * 1024 * 1024 } });
    } catch (err) {
      return res.status(400).send('Invalid request headers');
    }

    let fileUploaded = false;

    // We do all async db/folder checks in a promise so busboy can start consuming the stream immediately
    const resolveContext = async () => {
      const memberRes = await pool.query('SELECT id FROM "member" WHERE user_id = $1', [firebaseUid]);
      if (memberRes.rows.length === 0) throw new Error('Member not found');
      const uploaderId = memberRes.rows[0].id;

      const meetupRes = await pool.query('SELECT creator_id, root_folder_id, title FROM "meetup" WHERE id = $1', [meetupId]);
      if (meetupRes.rows.length === 0) throw new Error('Meetup not found');
      let meetupRootFolderId = meetupRes.rows[0].root_folder_id;
      const creatorId = meetupRes.rows[0].creator_id;
      const meetupTitle = meetupRes.rows[0].title || 'Untitled Meetup';

      const eventRes = await pool.query('SELECT host_id, root_folder_id FROM "meetup_event" WHERE meetup_id = $1 ORDER BY start_at DESC LIMIT 1', [meetupId]);
      let tokenHolderId = creatorId;

      if (eventRes.rows.length > 0 && eventRes.rows[0].root_folder_id) {
         meetupRootFolderId = eventRes.rows[0].root_folder_id;
         tokenHolderId = eventRes.rows[0].host_id;
      }

      const tokenRes = await pool.query('SELECT google_refresh_token, root_folder_id FROM "member" WHERE id = $1', [tokenHolderId]);
      const token = tokenRes.rows[0].google_refresh_token;
      const memberRootFolderId = tokenRes.rows[0].root_folder_id;

      if (!token) throw new Error('Meetup organizer has not integrated Google Drive');

      const driveClient = getDriveClient(token);

      if (!meetupRootFolderId) {
         if (!memberRootFolderId) {
            throw new Error('Meetup organizer has no root Drive folder configured');
         }
         meetupRootFolderId = await createFolder(driveClient, meetupTitle, memberRootFolderId);
         if (!meetupRootFolderId) {
             throw new Error('Failed to create Meetup Drive folder');
         }
         if (tokenHolderId === creatorId) {
            await pool.query('UPDATE "meetup" SET root_folder_id = $1 WHERE id = $2', [meetupRootFolderId, meetupId]);
         } else {
            await pool.query('UPDATE "meetup_event" SET root_folder_id = $1 WHERE meetup_id = $2 AND host_id = $3', [meetupRootFolderId, meetupId, tokenHolderId]);
         }
      }

      let uploadFolderId = meetupRootFolderId;

      if (pollId) {
        const pollRes = await pool.query('SELECT title, root_folder_id FROM "poll" WHERE id = $1', [pollId]);
        if (pollRes.rows.length === 0) throw new Error('Poll not found');

        const pollTitle = pollRes.rows[0].title;
        let pollFolderId = pollRes.rows[0].root_folder_id;

        if (!pollFolderId) {
          const pollsRootFolderId = await createFolder(driveClient, "Polls", meetupRootFolderId);
          pollFolderId = await createFolder(driveClient, pollTitle, pollsRootFolderId || meetupRootFolderId);
          if (pollFolderId) {
            await updateRecord(pool, 'poll', pollId, { root_folder_id: pollFolderId });
          }
        }
        uploadFolderId = pollFolderId || meetupRootFolderId;
      }

      return { uploaderId, tokenHolderId, driveClient, uploadFolderId };
    };

    const contextPromise = resolveContext();

    bb.on('file', async (name: any, file: any, info: any) => {
      logger.info({ name, filename }, 'Busboy file event');
      if (fileUploaded) {
        file.resume();
        return;
      }
      fileUploaded = true;

      const chunks: Buffer[] = [];
      file.on('data', (data: Buffer) => chunks.push(data));

      file.on('end', async () => {
        let currentTokenHolderId: string | undefined;

        try {
          const { uploaderId, tokenHolderId, driveClient, uploadFolderId } = await contextPromise;
          currentTokenHolderId = tokenHolderId;
          
          const buffer = Buffer.concat(chunks);
          const { Readable } = require('stream');
          const bufferStream = Readable.from(buffer);
          
          const fileId = await uploadFileStream(driveClient, filename, info.mimeType, bufferStream, uploadFolderId);

        if (fileId && pollId) {
          logger.info({ pollId, fileId }, 'File uploaded to Drive, updating database');
          const caption = req.query.caption as string | undefined;

          let thumbnail: string | null = null;
          if (info.mimeType.startsWith('image/')) {
            try {
              const sharp = require('sharp');
              const resized = await sharp(buffer)
                .resize(256, 256, { fit: 'inside' })
                .jpeg({ quality: 80 })
                .toBuffer();
              thumbnail = `data:image/jpeg;base64,${resized.toString('base64')}`;
            } catch (err) {
              logger.warn({ err }, 'Failed to generate thumbnail');
            }
          }

          const existingEntry = await pool.query(
            'SELECT id, file_id FROM "poll_entry" WHERE poll_id = $1 AND creator_id = $2',
            [pollId, uploaderId]
          );

          if (existingEntry.rows.length > 0) {
            const entryId = existingEntry.rows[0].id;
            const oldFileId = existingEntry.rows[0].file_id;
            if (oldFileId) await deleteFile(driveClient, oldFileId);
            await updateRecord(pool, 'poll_entry', entryId, { file_id: fileId, caption: caption || null, thumbnail });
          } else {
            await createRecord(pool, 'poll_entry', {
              poll_id: pollId,
              file_id: fileId,
              creator_id: uploaderId,
              caption: caption || null,
              thumbnail
            });
          }
        }
        res.status(200).send({ success: true, fileId });
      } catch (err: any) {
        if (currentTokenHolderId) {
          await handleDriveAuthError(pool, err, currentTokenHolderId);
        }
        logger.error({ err, filename }, 'Error streaming file');
        if (err.message && err.message.includes('not found')) {
          res.status(404).send(err.message);
        } else if (err.message && err.message.includes('configured')) {
           res.status(400).send(err.message);
        } else {
           res.status(500).send('Error uploading file');
        }
        }
      });
    });

    bb.on('finish', () => {
      logger.info('Busboy finish event triggered');
      if (!fileUploaded) {
        logger.warn('Busboy finished but no file was uploaded');
        res.status(400).send('No file found in request');
      }
    });

    bb.on('error', (err: any) => {
      logger.error({ err }, 'Busboy error');
      if (!res.headersSent) res.status(500).send('Upload parsing error');
    });

    logger.info('Piping request to Busboy');
    req.pipe(bb);
  });

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