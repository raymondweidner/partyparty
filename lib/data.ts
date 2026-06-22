import * as nodemailer from 'nodemailer';
import { Pool } from 'pg';
import { config } from './config';
import { logger } from './logger';
import { sendMessagesToDevices } from './messaging';

async function notifyMeetupStateChange(pool: Pool, meetupId: string, newState: string) {
  try {
    const membersRes = await pool.query(`
      SELECT m.id, m.email 
      FROM member m 
      JOIN tribe_member tm ON m.id = tm.member_id 
      JOIN meetup mt ON mt.tribe_id = tm.tribe_id
      WHERE mt.id = $1
    `, [meetupId]);
    const meetupRes = await pool.query('SELECT "title" FROM "meetup" WHERE "id" = $1', [meetupId]);
    if (meetupRes.rows.length === 0) return;
    const meetupTitle = meetupRes.rows[0].title;
    for (const row of membersRes.rows) {
      await createAndSendNotification(
        pool,
        row.id,
        `Meetup ${meetupTitle} is now ${newState}!`,
        `The meetup "${meetupTitle}" has changed its status to ${newState}.`,
        `<p>The meetup "<strong>${meetupTitle}</strong>" has changed its status to <strong>${newState}</strong>.</p>`,
        "meetup",
        meetupId,
        "GET"
      );
    }
  } catch (err) {
    logger.error({ err, meetupId, newState }, 'Error sending meetup state change emails');
  }
}

async function notifyProposalSelected(pool: Pool, meetupId: string) {
  try {
    const membersRes = await pool.query(`
      SELECT m.id, m.email 
      FROM member m 
      JOIN tribe_member tm ON m.id = tm.member_id 
      JOIN meetup mt ON mt.tribe_id = tm.tribe_id
      WHERE mt.id = $1
    `, [meetupId]);
    const meetupRes = await pool.query('SELECT "title" FROM "meetup" WHERE "id" = $1', [meetupId]);
    if (meetupRes.rows.length === 0) return;
    const meetupTitle = meetupRes.rows[0].title;
    for (const row of membersRes.rows) {
      await createAndSendNotification(
        pool,
        row.id,
        `Proposal selected for ${meetupTitle}!`,
        `A proposal has been selected for the meetup "${meetupTitle}".`,
        `<p>A proposal has been selected for the meetup "<strong>${meetupTitle}</strong>".</p>`,
        "meetup",
        meetupId,
        "GET"
      );
    }
  } catch (err) {
    logger.error({ err, meetupId }, 'Error sending proposal selected emails');
  }
}

export async function createAndSendNotification(pool: Pool, memberId: string, title: string, body: string, htmlBody?: string, resourceType?: string, resourceId?: string, actionMode?: string) {
  try {
    logger.info({ memberId, title, resourceType, resourceId, actionMode }, 'Processing new notification creation');

    // 1. Insert into Notification table
    const newRecord = await createRecord(pool, 'notification', {
      member_id: memberId,
      title,
      body,
      html_body: htmlBody || null,
      resource_type: resourceType || null,
      resource_id: resourceId || null,
      action_mode: actionMode || null,
    });
    logger.info({ notificationId: newRecord.id, memberId, title }, 'Database notification record created successfully');

    // 2. Query user_device tokens
    // Note: user_device stores the Firebase Auth UID, whereas memberId is the Postgres UUID. 
    // We must first fetch the Firebase UID from the member record!
    let tokens: string[] = [];
    const memRes = await pool.query(`SELECT * FROM "member" WHERE "id" = $1`, [memberId]);
    if (memRes.rows.length > 0) {
      const firebaseUid = memRes.rows[0].user_id;
      if (firebaseUid) {
        const tokensRes = await pool.query(`SELECT "token" FROM "user_device" WHERE "user_id" = $1`, [firebaseUid]);
        tokens = tokensRes.rows.map((r: any) => r.token);
      }
    }
    logger.info({ memberId, tokensFound: tokens.length }, 'Queried user_device for FCM tokens');

    // 3. Send Push Notification via FCM
    if (tokens.length > 0) {
      await sendMessagesToDevices({ notificationId: newRecord.id, title, body, htmlBody, resourceType, resourceId, actionMode }, 'notification', tokens);
      logger.info({ memberId, tokensCount: tokens.length }, 'Notification payload handed off to FCM');
    } else {
      logger.warn({ memberId }, 'No FCM tokens found for member, skipping push notification');
    }

    // 4. Send Email Fallback
    const memberRes = await pool.query('SELECT "email" FROM "member" WHERE "id" = $1', [memberId]);
    if (memberRes.rows.length > 0 && memberRes.rows[0].email) {
      const email = memberRes.rows[0].email;
      const transporter = nodemailer.createTransport(config.email);
      await transporter.sendMail({
        from: '"PartyParty" <noreply@partyparty.com>',
        to: email,
        subject: title,
        text: body,
        html: htmlBody || `<p>${body}</p>`,
      });
      logger.info({ email }, 'Notification email fallback sent');
    }

    return newRecord;
  } catch (err) {
    logger.error({ err, memberId, title }, 'Error in createAndSendNotification');
  }
}


/**
 * Retrieves records from a table, optionally filtering by specific key-value pairs.
 */
export async function getRecords(pool: Pool, tableName: string, filters: Record<string, any> = {}) {
  const keys = Object.keys(filters);
  const values = Object.values(filters);

  let query = `SELECT * FROM "${tableName}"`;
  if (keys.length > 0) {
    const whereClause = keys.map((key, index) => `"${key}" = $${index + 1}`).join(' AND ');
    query += ` WHERE ${whereClause}`;
  }

  const result = await pool.query(query, values);
  return result.rows;
}

/**
 * Retrieves a single record by its ID.
 */
export async function getRecordById(pool: Pool, tableName: string, id: string | number) {
  const query = `SELECT * FROM "${tableName}" WHERE "id" = $1`;
  const result = await pool.query(query, [id]);
  return result.rows.length > 0 ? result.rows[0] : null;
}

/**
 * Creates a new record in the specified table.
 */
export async function createRecord(pool: Pool, tableName: string, data: Record<string, any>) {
  const keys = Object.keys(data);
  const values = Object.values(data);

  if (keys.length === 0) {
    throw new Error('No data provided to create record');
  }

  const columns = keys.map((k) => `"${k}"`).join(', ');
  const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
  const query = `INSERT INTO "${tableName}" (${columns}) VALUES (${placeholders}) RETURNING *`;

  const result = await pool.query(query, values);
  const newRecord = result.rows[0];

  if (tableName.toLowerCase() === 'member' && newRecord.status === 'invited') {
    const email = newRecord.email;
    const transporter = nodemailer.createTransport(config.email);
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
    const memberRes = await pool.query('SELECT "email" FROM "member" WHERE "id" = $1', [member_id]);

    if (memberRes.rows.length > 0) {
      const chatRes = await pool.query('SELECT "title", "url" FROM "chat" WHERE "id" = $1', [chat_id]);
      const chatTitle = chatRes.rows.length > 0 ? chatRes.rows[0].title : 'a group chat';
      const chatLink = chatRes.rows.length > 0 && chatRes.rows[0].url ? chatRes.rows[0].url : `${config.app.url}/chat/${chat_id}`;

      await createAndSendNotification(
        pool,
        member_id,
        `You've been invited to a chat on PartyParty!`,
        `You have been invited to join "${chatTitle}". Click here to see the chat: ${chatLink}`,
        `<p>You have been invited to join "<strong>${chatTitle}</strong>". <a href="${chatLink}">Click here to see the chat.</a></p>`,
        "chat",
        chat_id,
        "GET"
      );
    } else {
      logger.error({ member_id }, 'Could not find member to send chat invite email');
    }
  }

  if (tableName.toLowerCase() === 'member_contact' && newRecord.status === 'invited') {
    const { source_id, subject_id } = newRecord;
    const sourceRes = await pool.query('SELECT "name" FROM "member" WHERE "id" = $1', [source_id]);
    const sourceName = sourceRes.rows.length > 0 ? sourceRes.rows[0].name : 'Someone';

    await createAndSendNotification(
      pool,
      subject_id,
      `New Contact Request!`,
      `${sourceName} wants to connect with you.`,
      `<p><strong>${sourceName}</strong> wants to connect with you.</p>`
    );
  }

  if (tableName.toLowerCase() === 'tribe_member') {
    const { member_id, tribe_id } = newRecord;
    const memberRes = await pool.query('SELECT "name" FROM "member" WHERE "id" = $1', [member_id]);
    const memberName = memberRes.rows.length > 0 ? memberRes.rows[0].name : 'A new member';
    const tribeRes = await pool.query('SELECT "name" FROM "tribe" WHERE "id" = $1', [tribe_id]);
    const tribeName = tribeRes.rows.length > 0 ? tribeRes.rows[0].name : 'a tribe';

    const tribeMembersRes = await pool.query(`SELECT "member_id" FROM "tribe_member" WHERE "tribe_id" = $1`, [tribe_id]);
    
    if (tribeMembersRes.rows.length > 1) {
      for (const row of tribeMembersRes.rows) {
        if (row.member_id === member_id) {
          await createAndSendNotification(
            pool,
            row.member_id,
            `Welcome to the tribe!`,
            `You have been added to the tribe "${tribeName}".`,
            `<p>You have been added to the tribe "<strong>${tribeName}</strong>".</p>`,
            "tribe",
            tribe_id,
            "GET"
          );
        } else {
          await createAndSendNotification(
            pool,
            row.member_id,
            `${memberName} joined the tribe!`,
            `${memberName} has been added to "${tribeName}".`,
            `<p><strong>${memberName}</strong> has been added to "<strong>${tribeName}</strong>".</p>`,
            "tribe",
            tribe_id,
            "GET"
          );
        }
      }
    }
  }

  if (tableName.toLowerCase() === 'meetup') {
    const { tribe_id, creator_id, title, id: meetupId } = newRecord;
    const tribeMembersRes = await pool.query(`SELECT "member_id" FROM "tribe_member" WHERE "tribe_id" = $1`, [tribe_id]);
    for (const row of tribeMembersRes.rows) {
      if (row.member_id === creator_id) continue;
      await createAndSendNotification(
        pool,
        row.member_id,
        `New Meetup Created!`,
        `A new meetup "${title}" has been created in your tribe.`,
        `<p>A new meetup "<strong>${title}</strong>" has been created in your tribe.</p>`,
        "meetup",
        meetupId,
        "GET"
      );
    }
  }

  if (tableName.toLowerCase() === 'proposal') {
    const { meetup_id, host_id } = newRecord;
    const meetupRes = await pool.query(`SELECT "tribe_id", "title" FROM "meetup" WHERE "id" = $1`, [meetup_id]);
    if (meetupRes.rows.length > 0) {
      const { tribe_id, title: meetupTitle } = meetupRes.rows[0];
      const tribeMembersRes = await pool.query(`SELECT "member_id" FROM "tribe_member" WHERE "tribe_id" = $1`, [tribe_id]);
      for (const row of tribeMembersRes.rows) {
        if (row.member_id === host_id) continue;
        await createAndSendNotification(
          pool,
          row.member_id,
          `New Proposal for Meetup!`,
          `A new proposal has been added to the meetup "${meetupTitle}".`,
          `<p>A new proposal has been added to the meetup "<strong>${meetupTitle}</strong>".</p>`,
          "meetup",
          meetup_id,
          "GET"
        );
      }
    }
  }

  return newRecord;
}

/**
 * Updates an existing record by its ID.
 */
export async function updateRecord(pool: Pool, tableName: string, id: string | number, data: Record<string, any>) {
  const keys = Object.keys(data);
  const values = Object.values(data);

  if (keys.length === 0) {
    throw new Error('No data provided to update record');
  }

  const oldRecord = await getRecordById(pool, tableName, id);

  const setClause = keys.map((k, i) => `"${k}" = $${i + 1}`).join(', ');
  const query = `UPDATE "${tableName}" SET ${setClause} WHERE "id" = $${keys.length + 1} RETURNING *`;

  const result = await pool.query(query, [...values, id]);
  const newRecord = result.rows.length > 0 ? result.rows[0] : null;

  if (oldRecord && newRecord) {
    if (tableName.toLowerCase() === 'meetup' && newRecord.status !== oldRecord.status) {
      await notifyMeetupStateChange(pool, newRecord.id, newRecord.status);
    }
    if (tableName.toLowerCase() === 'proposal' && newRecord.status === 'accepted' && oldRecord.status !== 'accepted') {
      await notifyProposalSelected(pool, newRecord.meetup_id);
    }
    if (tableName.toLowerCase() === 'member_contact' && newRecord.status === 'accepted' && oldRecord.status === 'invited') {
      const { source_id, subject_id } = newRecord;
      const subjectRes = await pool.query('SELECT "name" FROM "member" WHERE "id" = $1', [subject_id]);
      const subjectName = subjectRes.rows.length > 0 ? subjectRes.rows[0].name : 'Someone';

      await createAndSendNotification(
        pool,
        source_id,
        `Contact Request Accepted!`,
        `${subjectName} accepted your contact request.`,
        `<p><strong>${subjectName}</strong> accepted your contact request.</p>`
      );
    }
    if (tableName.toLowerCase() === 'member' && newRecord.status === 'active' && oldRecord.status === 'invited') {
      const memberContactRes = await pool.query(`SELECT "source_id" FROM "member_contact" WHERE "subject_id" = $1`, [newRecord.id]);
      if (memberContactRes.rows.length > 0) {
        const inviterId = memberContactRes.rows[0].source_id;
        const memberRes = await pool.query('SELECT "name" FROM "member" WHERE "id" = $1', [newRecord.id]);
        const newMemberName = memberRes.rows.length > 0 ? memberRes.rows[0].name : 'Your friend';
        await createAndSendNotification(
          pool,
          inviterId,
          `${newMemberName} joined PartyParty!`,
          `${newMemberName} has accepted your invite and joined the app.`,
          `<p><strong>${newMemberName}</strong> has accepted your invite and joined the app.</p>`
        );
      }
    }
    if (tableName.toLowerCase() === 'availability' && newRecord.status !== oldRecord.status) {
      const { proposal_id, member_id, status } = newRecord;
      const proposalRes = await pool.query(`SELECT "host_id", "meetup_id" FROM "proposal" WHERE "id" = $1`, [proposal_id]);
      if (proposalRes.rows.length > 0) {
        const { host_id, meetup_id } = proposalRes.rows[0];
        if (host_id !== member_id) {
          const memberRes = await pool.query(`SELECT "name" FROM "member" WHERE "id" = $1`, [member_id]);
          const memberName = memberRes.rows.length > 0 ? memberRes.rows[0].name : 'Someone';
          await createAndSendNotification(
            pool,
            host_id,
            `Availability Updated`,
            `${memberName} updated their availability for your proposal to: ${status}.`,
            `<p><strong>${memberName}</strong> updated their availability for your proposal to: <strong>${status}</strong>.</p>`,
            "meetup",
            meetup_id,
            "GET"
          );
        }
      }
    }
  }

  return newRecord;
}

/**
 * Deletes a record by its ID.
 */
export async function deleteRecord(pool: Pool, tableName: string, id: string | number) {
  const query = `DELETE FROM "${tableName}" WHERE "id" = $1 RETURNING *`;
  const result = await pool.query(query, [id]);
  return result.rows.length > 0 ? result.rows[0] : null;
}
