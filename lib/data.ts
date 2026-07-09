import * as nodemailer from 'nodemailer';
import { Pool } from 'pg';
import { config } from './config';
import { logger } from './logger';
import { sendMessagesToDevices } from './messaging';
import { getDriveClient, createFolder } from './googleDriveService';

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
        "GET",
        "meetup_state_changed"
      );
    }
  } catch (err) {
    logger.error({ err, meetupId, newState }, 'Error sending meetup state change emails');
  }
}

async function notifyMeetupCancelled(pool: Pool, meetupId: string) {
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
        `Meetup Cancelled!`,
        `The meetup "${meetupTitle}" has been cancelled.`,
        `<p>The meetup "<strong>${meetupTitle}</strong>" has been cancelled.</p>`,
        "meetup",
        meetupId,
        "GET",
        "meetup_cancelled"
      );
    }
  } catch (err) {
    logger.error({ err, meetupId }, 'Error sending meetup cancelled emails');
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
        "GET",
        "proposal_selected"
      );
    }
  } catch (err) {
    logger.error({ err, meetupId }, 'Error sending proposal selected emails');
  }
}

export async function notifyPollVotingOpen(pool: Pool, pollId: string) {
  try {
    const membersRes = await pool.query(`
      SELECT m.id 
      FROM member m 
      JOIN tribe_member tm ON m.id = tm.member_id 
      JOIN meetup mt ON mt.tribe_id = tm.tribe_id
      JOIN poll p ON p.meetup_id = mt.id
      WHERE p.id = $1
    `, [pollId]);
    const pollRes = await pool.query('SELECT "title" FROM "poll" WHERE "id" = $1', [pollId]);
    if (pollRes.rows.length === 0) return;
    const pollTitle = pollRes.rows[0].title;
    for (const row of membersRes.rows) {
      await createAndSendNotification(
        pool,
        row.id,
        `Poll Open for Voting!`,
        `The poll "${pollTitle}" is now open for voting.`,
        `<p>The poll "<strong>${pollTitle}</strong>" is now open for voting.</p>`,
        "poll",
        pollId,
        "GET",
        "poll_voting_open"
      );
    }
  } catch (err) {
    logger.error({ err, pollId }, 'Error sending poll voting open notifications');
  }
}

export async function notifyPollCompleted(pool: Pool, pollId: string, winningEntries: any[]) {
  try {
    const membersRes = await pool.query(`
      SELECT m.id 
      FROM member m 
      JOIN tribe_member tm ON m.id = tm.member_id 
      JOIN meetup mt ON mt.tribe_id = tm.tribe_id
      JOIN poll p ON p.meetup_id = mt.id
      WHERE p.id = $1
    `, [pollId]);
    const pollRes = await pool.query('SELECT "title" FROM "poll" WHERE "id" = $1', [pollId]);
    if (pollRes.rows.length === 0) return;
    const pollTitle = pollRes.rows[0].title;

    const winnerNames = winningEntries.map(e => e.creator_name).join(', ');
    const winnerLinks = winningEntries.map(e => `<a href="${config.app.url}/media/${e.file_id}">Entry by ${e.creator_name}</a>`).join(', ');

    for (const row of membersRes.rows) {
      await createAndSendNotification(
        pool,
        row.id,
        `Poll Completed!`,
        `The poll "${pollTitle}" has ended! Winners: ${winnerNames}.`,
        `<p>The poll "<strong>${pollTitle}</strong>" has ended! Winners: ${winnerLinks}.</p>`,
        "poll",
        pollId,
        "GET",
        "poll_completed"
      );
    }
  } catch (err) {
    logger.error({ err, pollId }, 'Error sending poll completed notifications');
  }
}

export async function notifyPollNoEntries(pool: Pool, pollId: string) {
  try {
    const membersRes = await pool.query(`
      SELECT m.id 
      FROM member m 
      JOIN tribe_member tm ON m.id = tm.member_id 
      JOIN meetup mt ON mt.tribe_id = tm.tribe_id
      JOIN poll p ON p.meetup_id = mt.id
      WHERE p.id = $1
    `, [pollId]);
    const pollRes = await pool.query('SELECT "title" FROM "poll" WHERE "id" = $1', [pollId]);
    if (pollRes.rows.length === 0) return;
    const pollTitle = pollRes.rows[0].title;
    for (const row of membersRes.rows) {
      await createAndSendNotification(
        pool,
        row.id,
        `Poll Failed (No Entries)`,
        `The poll "${pollTitle}" received no entries. Come on, participate next time!`,
        `<p>The poll "<strong>${pollTitle}</strong>" received no entries. Come on, participate next time!</p>`,
        "poll",
        pollId,
        "GET",
        "poll_no_entries"
      );
    }
  } catch (err) {
    logger.error({ err, pollId }, 'Error sending poll no entries notifications');
  }
}

export async function notifyPollNoVotes(pool: Pool, pollId: string) {
  try {
    const membersRes = await pool.query(`
      SELECT m.id 
      FROM member m 
      JOIN tribe_member tm ON m.id = tm.member_id 
      JOIN meetup mt ON mt.tribe_id = tm.tribe_id
      JOIN poll p ON p.meetup_id = mt.id
      WHERE p.id = $1
    `, [pollId]);
    const pollRes = await pool.query('SELECT "title" FROM "poll" WHERE "id" = $1', [pollId]);
    if (pollRes.rows.length === 0) return;
    const pollTitle = pollRes.rows[0].title;
    for (const row of membersRes.rows) {
      await createAndSendNotification(
        pool,
        row.id,
        `Poll Failed (No Votes)`,
        `The poll "${pollTitle}" received no votes. Next time, don't forget to vote!`,
        `<p>The poll "<strong>${pollTitle}</strong>" received no votes. Next time, don't forget to vote!</p>`,
        "poll",
        pollId,
        "GET",
        "poll_no_votes"
      );
    }
  } catch (err) {
    logger.error({ err, pollId }, 'Error sending poll no votes notifications');
  }
}

export async function createAndSendNotification(pool: Pool, memberId: string, title: string, body: string, htmlBody?: string, resourceType?: string, resourceId?: string, actionMode?: string, alertType?: string) {
  try {
    logger.info({ memberId, title, resourceType, resourceId, actionMode, alertType }, 'Processing new notification creation');

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

    let emailEnabled = true;
    let pushEnabled = true;

    if (alertType) {
      const prefRes = await pool.query('SELECT email_enabled, push_enabled FROM member_alert_preference WHERE member_id = $1 AND alert_type = $2', [memberId, alertType]);
      if (prefRes.rows.length > 0) {
        emailEnabled = prefRes.rows[0].email_enabled;
        pushEnabled = prefRes.rows[0].push_enabled;
      }
    }

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
    if (tokens.length > 0 && pushEnabled) {
      await sendMessagesToDevices({ notificationId: newRecord.id, title, body, htmlBody, resourceType, resourceId, actionMode }, 'notification', tokens);
      logger.info({ memberId, tokensCount: tokens.length }, 'Notification payload handed off to FCM');
    } else {
      logger.warn({ memberId }, 'No FCM tokens found for member, skipping push notification');
    }

    // 4. Send Email Fallback
    if (emailEnabled) {
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
        logger.info({ memberId, email }, 'Sent email notification fallback');
      }
    }
    
    return newRecord;
  } catch (err) {
    logger.error({ err, memberId, title }, 'Error in createAndSendNotification');
  }
}

export async function handleDriveAuthError(pool: Pool, err: any, hostId: string) {
  if (err?.response?.data?.error === 'invalid_grant' || err?.message?.includes('invalid_grant')) {
    logger.warn({ hostId }, 'Refresh token invalid_grant, disconnecting Drive');
    await updateRecord(pool, 'member', hostId, { google_refresh_token: null, root_folder_id: null });
    await createAndSendNotification(
      pool,
      hostId,
      'Google Drive Disconnected',
      'Your Google Drive connection expired. Please reconnect it in your profile to continue saving media.',
      '<p>Your Google Drive connection expired. Please reconnect it in your profile to continue saving media.</p>',
      'oauth',
      'google_drive'
    );
    return true;
  }
  return false;
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

  if (tableName.toLowerCase() === 'member') {
    const alertTypes = [
      'meetup_state_changed', 'proposal_selected', 'chat_invite', 'tribe_invite', 
      'meetup_created', 'proposal_created', 'tribe_member_added', 'app_invite_accepted', 
      'availability_updated', 'contact_request_received', 'contact_request_accepted', 'meetup_cancelled', 'poll_created',
      'poll_voting_open', 'poll_completed', 'poll_no_entries', 'poll_no_votes', 'registry_item_updated'
    ];
    for (const alertType of alertTypes) {
      await createRecord(pool, 'member_alert_preference', {
        member_id: newRecord.id,
        alert_type: alertType,
        email_enabled: true,
        push_enabled: true,
      });
    }

    if (newRecord.status === 'invited') {
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

    // Google Drive Integration: Create root TribeVibe folder
    if (newRecord.google_refresh_token) {
      try {
        const driveClient = getDriveClient(newRecord.google_refresh_token);
        const rootFolderId = await createFolder(driveClient, 'TribeVibe');
        if (rootFolderId) {
           await updateRecord(pool, 'member', newRecord.id, { root_folder_id: rootFolderId });
        }
      } catch(err) {
        logger.error({ err, memberId: newRecord.id }, 'Error creating TribeVibe folder for new member');
      }
    }
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
        "GET",
        "chat_invite"
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
      `<p><strong>${sourceName}</strong> wants to connect with you.</p>`,
      undefined,
      undefined,
      undefined,
      "contact_request_received"
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
            "GET",
            "tribe_invite"
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
            "GET",
            "tribe_member_added"
          );
        }
      }
    }
  }

  if (tableName.toLowerCase() === 'meetup') {
    const { tribe_id, creator_id, title, id: meetupId } = newRecord;

    // Create Google Drive Folder for Meetup
    try {
      const creatorRes = await pool.query('SELECT google_refresh_token, root_folder_id FROM "member" WHERE id = $1', [creator_id]);
      if (creatorRes.rows.length > 0 && creatorRes.rows[0].google_refresh_token && creatorRes.rows[0].root_folder_id) {
        const driveClient = getDriveClient(creatorRes.rows[0].google_refresh_token);
        const meetupTitle = title || 'Untitled Meetup';
        const meetupFolderId = await createFolder(driveClient, meetupTitle, creatorRes.rows[0].root_folder_id);
        if (meetupFolderId) {
           await pool.query('UPDATE "meetup" SET root_folder_id = $1 WHERE id = $2', [meetupFolderId, meetupId]);
           newRecord.root_folder_id = meetupFolderId;
        }
      }
    } catch (err) {
      logger.error({ err, meetupId }, 'Error creating Drive folder for new Meetup');
    }

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
        "GET",
        "meetup_created"
      );
    }
  }

  if (tableName.toLowerCase() === 'proposal') {
    const { meetup_id, host_id, id: proposalId } = newRecord;
    const meetupRes = await pool.query(`SELECT "tribe_id", "title" FROM "meetup" WHERE "id" = $1`, [meetup_id]);
    if (meetupRes.rows.length > 0) {
      const { tribe_id, title: meetupTitle } = meetupRes.rows[0];

      // Google Drive logic: Create folder for Proposal in host's drive
      try {
        const hostRes = await pool.query('SELECT google_refresh_token, root_folder_id FROM "member" WHERE id = $1', [host_id]);
        if (hostRes.rows.length > 0 && hostRes.rows[0].google_refresh_token && hostRes.rows[0].root_folder_id) {
          const driveClient = getDriveClient(hostRes.rows[0].google_refresh_token);
          const proposalFolderId = await createFolder(driveClient, meetupTitle || 'Untitled Meetup', hostRes.rows[0].root_folder_id);
          if (proposalFolderId) {
             await pool.query('UPDATE "proposal" SET root_folder_id = $1 WHERE id = $2', [proposalFolderId, proposalId]);
             newRecord.root_folder_id = proposalFolderId;
          }
        }
      } catch (err) {
        logger.error({ err, proposalId }, 'Error creating Drive folder for new Proposal');
      }

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
          "GET",
          "proposal_created"
        );
      }
    }
  }

  if (tableName.toLowerCase() === 'poll') {
    const { creator_id, title, details, id: pollId } = newRecord;
    const creatorRes = await pool.query('SELECT "name" FROM "member" WHERE "id" = $1', [creator_id]);
    const creatorName = creatorRes.rows.length > 0 ? creatorRes.rows[0].name : 'A member';
    
    // Notify all distinct members that share a tribe with the creator
    const tribeMembersRes = await pool.query(`
      SELECT DISTINCT tm2.member_id 
      FROM tribe_member tm1
      JOIN tribe_member tm2 ON tm1.tribe_id = tm2.tribe_id
      WHERE tm1.member_id = $1 AND tm2.member_id != $1
    `, [creator_id]);
    
    for (const row of tribeMembersRes.rows) {
      await createAndSendNotification(
        pool,
        row.member_id,
        `New Poll Created!`,
        `${creatorName} created a new poll: "${title}". ${details || ''}`,
        `<p><strong>${creatorName}</strong> created a new poll: "<strong>${title}</strong>".<br/>${details || ''}</p>`,
        "poll",
        pollId,
        "GET",
        "poll_created"
      );
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
      if (newRecord.status === 'Cancelled') {
        await notifyMeetupCancelled(pool, newRecord.id);
      } else {
        await notifyMeetupStateChange(pool, newRecord.id, newRecord.status);
      }
    }
    if (tableName.toLowerCase() === 'proposal' && newRecord.status === 'accepted' && oldRecord.status !== 'accepted') {
      await notifyProposalSelected(pool, newRecord.meetup_id);

      // Create MeetupEvent
      let meetupEvent: any;
      try {
        meetupEvent = await createRecord(pool, 'meetup_event', {
          meetup_id: newRecord.meetup_id,
          host_id: newRecord.host_id,
          start_at: newRecord.start_at,
          end_at: newRecord.end_at,
          location: newRecord.location,
          note: newRecord.note || null,
        });
      } catch (err) {
        logger.error({ err, proposalId: newRecord.id }, 'Error creating MeetupEvent');
      }

      // Transfer registries to the new MeetupEvent
      if (meetupEvent) {
        try {
          await pool.query('UPDATE "help_registry" SET meetup_event_id = $1 WHERE proposal_id = $2', [meetupEvent.id, newRecord.id]);
        } catch (err) {
          logger.error({ err, proposalId: newRecord.id }, 'Error transferring registries to MeetupEvent');
        }
      }

      // Google Drive Logic: Create subfolder for MeetupEvent
      try {
        if (newRecord.root_folder_id) {
          const hostRes = await pool.query('SELECT google_refresh_token FROM "member" WHERE id = $1', [newRecord.host_id]);
          if (hostRes.rows.length > 0 && hostRes.rows[0].google_refresh_token) {
            const driveClient = getDriveClient(hostRes.rows[0].google_refresh_token);
            const dateStr = new Date(newRecord.start_at).toISOString().split('T')[0];
            const dateFolderId = await createFolder(driveClient, dateStr, newRecord.root_folder_id);
            
            if (dateFolderId && meetupEvent) {
              await updateRecord(pool, 'meetup_event', meetupEvent.id, { root_folder_id: dateFolderId });
            }
          }
        }
      } catch (err: any) {
        const handled = await handleDriveAuthError(pool, err, newRecord.host_id);
        if (!handled) {
          logger.error({ err, proposalId: newRecord.id }, 'Error setting up MeetupEvent Drive folder');
        }
      }
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
        `<p><strong>${subjectName}</strong> accepted your contact request.</p>`,
        undefined,
        undefined,
        undefined,
        "contact_request_accepted"
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
          `<p><strong>${newMemberName}</strong> has accepted your invite and joined the app.</p>`,
          undefined,
          undefined,
          undefined,
          "app_invite_accepted"
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
            "GET",
            "availability_updated"
          );
        }
      }
    }
    if (tableName.toLowerCase() === 'registry_item' && newRecord.status !== oldRecord.status) {
      const { help_registry_id, helper_id, status, details } = newRecord;
      const registryRes = await pool.query('SELECT proposal_id, meetup_event_id, name FROM "help_registry" WHERE "id" = $1', [help_registry_id]);
      if (registryRes.rows.length > 0) {
        const { proposal_id, meetup_event_id, name: registryName } = registryRes.rows[0];
        let hostId: string | null = null;
        let meetupId: string | null = null;

        if (proposal_id) {
          const propRes = await pool.query('SELECT host_id, meetup_id FROM "proposal" WHERE "id" = $1', [proposal_id]);
          if (propRes.rows.length > 0) {
            hostId = propRes.rows[0].host_id;
            meetupId = propRes.rows[0].meetup_id;
          }
        } else if (meetup_event_id) {
          const evRes = await pool.query('SELECT host_id, meetup_id FROM "meetup_event" WHERE "id" = $1', [meetup_event_id]);
          if (evRes.rows.length > 0) {
            hostId = evRes.rows[0].host_id;
            meetupId = evRes.rows[0].meetup_id;
          }
        }

        if (hostId) {
          let helperName = 'Someone';
          if (helper_id) {
            const memberRes = await pool.query('SELECT "name" FROM "member" WHERE "id" = $1', [helper_id]);
            if (memberRes.rows.length > 0) {
              helperName = memberRes.rows[0].name;
            }
          }

          await createAndSendNotification(
            pool,
            hostId,
            `Registry Item Updated`,
            `${helperName} updated the status of "${details}" in ${registryName} to: ${status}.`,
            `<p><strong>${helperName}</strong> updated the status of "<strong>${details}</strong>" in <strong>${registryName}</strong> to: <strong>${status}</strong>.</p>`,
            "meetup",
            meetupId || undefined,
            "GET",
            "registry_item_updated"
          );
        }
      }
    }
    if (tableName.toLowerCase() === 'member') {
      if (newRecord.google_refresh_token !== oldRecord.google_refresh_token) {
        if (newRecord.google_refresh_token) {
          // Integration connected/updated
          try {
            const driveClient = getDriveClient(newRecord.google_refresh_token);
            let rootFolderId = newRecord.root_folder_id;
            
            if (!rootFolderId) {
              rootFolderId = await createFolder(driveClient, 'TribeVibe');
              if (rootFolderId) {
                 await pool.query('UPDATE "member" SET root_folder_id = $1 WHERE id = $2', [rootFolderId, newRecord.id]);
              }
            }
            
            if (rootFolderId) {
              // Fetch meetups created by this member
              const createdMeetupsRes = await pool.query(`
                SELECT id, title FROM "meetup" WHERE creator_id = $1
              `, [newRecord.id]);
              
              for (const row of createdMeetupsRes.rows) {
                const meetupTitle = row.title || 'Untitled Meetup';
                const meetupFolderId = await createFolder(driveClient, meetupTitle, rootFolderId);
                
                if (meetupFolderId) {
                  await pool.query('UPDATE "meetup" SET root_folder_id = $1 WHERE id = $2', [meetupFolderId, row.id]);
                  // Backfill polls for this meetup (where meetupEvent is null)
                  const pollsRes = await pool.query(`
                    SELECT id, title FROM "poll" WHERE meetup_id = $1 AND meetup_event_id IS NULL
                  `, [row.id]);
                  
                  let pollsRootFolderId;
                  for (const pollRow of pollsRes.rows) {
                    if (!pollsRootFolderId) {
                      pollsRootFolderId = await createFolder(driveClient, "Polls", meetupFolderId);
                    }
                    const pollFolderId = await createFolder(driveClient, pollRow.title, pollsRootFolderId || meetupFolderId);
                    if (pollFolderId) {
                      await pool.query('UPDATE "poll" SET root_folder_id = $1 WHERE id = $2', [pollFolderId, pollRow.id]);
                    }
                  }
                }
              }
              
              // Fetch proposals hosted by this member
              const hostedProposalsRes = await pool.query(`
                SELECT p.id, p.meetup_id, m.title 
                FROM "proposal" p
                JOIN "meetup" m ON p.meetup_id = m.id
                WHERE p.host_id = $1
              `, [newRecord.id]);
              
              for (const row of hostedProposalsRes.rows) {
                const meetupTitle = row.title || 'Untitled Meetup';
                const proposalFolderId = await createFolder(driveClient, meetupTitle, rootFolderId);
                
                if (proposalFolderId) {
                  await pool.query('UPDATE "proposal" SET root_folder_id = $1 WHERE id = $2', [proposalFolderId, row.id]);

                  // Backfill events for this proposal
                  const eventsRes = await pool.query(`
                    SELECT id, start_at FROM "meetup_event" WHERE meetup_id = $1 AND host_id = $2
                  `, [row.meetup_id, newRecord.id]);
                  
                  for (const eventRow of eventsRes.rows) {
                    const dateStr = new Date(eventRow.start_at).toISOString().split('T')[0];
                    const dateFolderId = await createFolder(driveClient, dateStr, proposalFolderId);
                    if (dateFolderId) {
                      await pool.query('UPDATE "meetup_event" SET root_folder_id = $1 WHERE id = $2', [dateFolderId, eventRow.id]);
                      
                      // Backfill polls for this event
                      const pollsRes = await pool.query(`
                        SELECT id, title FROM "poll" WHERE meetup_event_id = $1
                      `, [eventRow.id]);
                      let pollsRootFolderId;
                      for (const pollRow of pollsRes.rows) {
                        if (!pollsRootFolderId) {
                          pollsRootFolderId = await createFolder(driveClient, "Polls", dateFolderId);
                        }
                        const pollFolderId = await createFolder(driveClient, pollRow.title, pollsRootFolderId || dateFolderId);
                        if (pollFolderId) {
                          await pool.query('UPDATE "poll" SET root_folder_id = $1 WHERE id = $2', [pollFolderId, pollRow.id]);
                        }
                      }
                    }
                  }
                }
              }
            }
          } catch(err) {
            logger.error({ err, memberId: newRecord.id }, 'Error setting up Drive folders during member update');
          }
        } else {
          // Integration removed
          try {
            // Nullify root_folder_id on the member
            await pool.query('UPDATE "member" SET root_folder_id = NULL WHERE id = $1', [newRecord.id]);
            
            // Nullify root_folder_id on all meetups they created and their polls
            const createdMeetupsRes = await pool.query(`
              SELECT id FROM "meetup" WHERE creator_id = $1
            `, [newRecord.id]);
            
            for (const row of createdMeetupsRes.rows) {
              await pool.query('UPDATE "meetup" SET root_folder_id = NULL WHERE id = $1', [row.id]);
              await pool.query('UPDATE "poll" SET root_folder_id = NULL WHERE meetup_id = $1 AND meetup_event_id IS NULL', [row.id]);
            }
            
            // Nullify root_folder_id on all proposals they are hosting and their events/polls
            const hostedProposalsRes = await pool.query(`
              SELECT p.id, p.meetup_id FROM "proposal" p WHERE p.host_id = $1
            `, [newRecord.id]);
            
            for (const row of hostedProposalsRes.rows) {
              await pool.query('UPDATE "proposal" SET root_folder_id = NULL WHERE id = $1', [row.id]);
              const eventsRes = await pool.query('SELECT id FROM "meetup_event" WHERE meetup_id = $1 AND host_id = $2', [row.meetup_id, newRecord.id]);
              for (const eRow of eventsRes.rows) {
                await pool.query('UPDATE "meetup_event" SET root_folder_id = NULL WHERE id = $1', [eRow.id]);
                await pool.query('UPDATE "poll" SET root_folder_id = NULL WHERE meetup_event_id = $1', [eRow.id]);
              }
            }
          } catch(err) {
            logger.error({ err, memberId: newRecord.id }, 'Error removing Drive folders during member update');
          }
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
