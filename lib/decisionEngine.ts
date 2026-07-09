import { Pool } from 'pg';
import { getRecords, updateRecord, notifyPollVotingOpen, notifyPollCompleted, notifyPollNoEntries, notifyPollNoVotes } from './data';
import { logger } from './logger';

/**
 * Dummy function to select a proposal based on the meetup's decision method.
 */
async function executeDecisionMethod(meetup: any, pool: Pool): Promise<string | null> {
  logger.info({ meetupId: meetup.id, method: meetup.decision_method }, '[Decision Engine] Executing dummy decision method');

  const proposals = await getRecords(pool, 'proposal', { meetup_id: meetup.id, status: 'pending' });

  if (proposals.length === 0) {
    logger.info({ meetupId: meetup.id }, '[Decision Engine] No pending proposals found for meetup');
    return null;
  }

  // Dummy logic: Randomly select one proposal
  const selectedIndex = Math.floor(Math.random() * proposals.length);
  return proposals[selectedIndex].id;
}

function getNthDayOfWeek(year: number, month: number, nth: number, dayOfWeek: number): Date {
  const date = new Date(year, month, 1);
  let count = 0;
  while (date.getMonth() === month) {
    if (date.getDay() === dayOfWeek) {
      count++;
      if (count === nth) {
        return new Date(date);
      }
    }
    date.setDate(date.getDate() + 1);
  }
  // Fallback: return the last occurrence if nth is out of bounds
  date.setDate(date.getDate() - 1);
  while (date.getDay() !== dayOfWeek) {
    date.setDate(date.getDate() - 1);
  }
  return date;
}

function calculateNextOccasion(createdAt: Date, type: string, basis: number, monthToRecur: number, weekToRecur: number, dayToRecur: number): Date {
  const now = new Date();
  let k = 0;
  while (true) {
    let occasion: Date;

    if (type === 'yearly') {
      const year = createdAt.getFullYear() + k * basis;
      if (!weekToRecur) {
        occasion = new Date(year, monthToRecur, dayToRecur + 1);
      } else {
        occasion = getNthDayOfWeek(year, monthToRecur, weekToRecur, dayToRecur);
      }
    } else if (type === 'monthly') {
      const totalMonths = createdAt.getMonth() + k * basis;
      const year = createdAt.getFullYear() + Math.floor(totalMonths / 12);
      const month = totalMonths % 12;
      if (!weekToRecur) {
        occasion = new Date(year, month, dayToRecur + 1);
      } else {
        occasion = getNthDayOfWeek(year, month, weekToRecur, dayToRecur);
      }
    } else if (type === 'weekly') {
      const sunday = new Date(createdAt);
      sunday.setDate(sunday.getDate() - sunday.getDay());
      occasion = new Date(sunday);
      occasion.setDate(occasion.getDate() + k * basis * 7 + dayToRecur);
    } else {
      return now; // Fallback
    }

    if (occasion > now) {
      return occasion;
    }
    k++;

    // Safety check
    if (k > 10000) return now;
  }
}

/**
 * Handles deciding on active meetups that have passed their decision deadline.
 */
async function makeMeetupDecisions(pool: Pool) {
  const query = `
    SELECT * FROM "meetup" 
    WHERE "status" = 'Planning' 
      AND "days_to_decide" IS NOT NULL
      AND NOW() >= "created_at" + ("days_to_decide" * INTERVAL '1 day')
  `;
  const { rows: meetups } = await pool.query(query);

  for (const meetup of meetups) {
    logger.info({ meetupId: meetup.id }, '[Decision Engine] Deadline reached for meetup. Selecting proposal...');
    const selectedProposalId = await executeDecisionMethod(meetup, pool);

    if (selectedProposalId) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(`UPDATE "proposal" SET "status" = 'accepted' WHERE "id" = $1`, [selectedProposalId]);
        await client.query(`UPDATE "proposal" SET "status" = 'rejected' WHERE "meetup_id" = $1 AND "id" != $2 AND "status" = 'pending'`, [meetup.id, selectedProposalId]);
        await client.query(`UPDATE "meetup" SET "status" = 'Upcoming' WHERE "id" = $1`, [meetup.id]);
        await client.query('COMMIT');
        logger.info({ meetupId: meetup.id }, '[Decision Engine] Meetup marked as Upcoming');
      } catch (e) {
        await client.query('ROLLBACK');
        throw e;
      } finally {
        client.release();
      }
    } else {
      await updateRecord(pool, 'meetup', meetup.id, { status: 'Cancelled' });
      logger.info({ meetupId: meetup.id }, '[Decision Engine] Meetup marked as Cancelled (no proposals)');
    }
  }
}

/**
 * Marks Upcoming meetups as Ongoing when the current time is between the accepted proposal's startAt and endAt.
 */
async function transitionUpcomingMeetupsToOngoing(pool: Pool) {
  const query = `
    SELECT m.* 
    FROM "meetup" m
    JOIN "proposal" p ON p.meetup_id = m.id
    WHERE m.status = 'Upcoming' 
      AND p.status = 'accepted'
      AND NOW() >= p.start_at
      AND NOW() < p.end_at
  `;
  const { rows: meetups } = await pool.query(query);

  for (const meetup of meetups) {
    logger.info({ meetupId: meetup.id }, '[Decision Engine] Meetup is now Ongoing.');
    await updateRecord(pool, 'meetup', meetup.id, { status: 'Ongoing' });
  }
}

/**
 * Marks Ongoing (or Upcoming) meetups as Complete once their selected proposal has expired.
 */
async function completeExpiredMeetups(pool: Pool) {
  const query = `
    SELECT m.*, p.id as proposal_id 
    FROM "meetup" m
    JOIN "proposal" p ON p.meetup_id = m.id
    WHERE m.status IN ('Upcoming', 'Ongoing')
      AND p.status = 'accepted'
      AND NOW() >= p.end_at
  `;
  const { rows: meetups } = await pool.query(query);

  for (const meetup of meetups) {
    logger.info({ meetupId: meetup.id }, '[Decision Engine] Accepted proposal expired for meetup. Marking as Complete...');
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`UPDATE "meetup" SET "status" = 'Complete' WHERE "id" = $1`, [meetup.id]);

      // Calculate recurs_on if the meetup is recurring
      if (meetup.recurrence_type) {
        const nextOccasion = calculateNextOccasion(
          new Date(meetup.created_at),
          meetup.recurrence_type,
          meetup.recurrence_basis,
          meetup.month_to_recur,
          meetup.week_to_recur,
          meetup.day_to_recur
        );
        await client.query(`UPDATE "meetup" SET "recurs_on" = $1 WHERE "id" = $2`, [nextOccasion, meetup.id]);
      }

      // Delete all proposals (and cascade to availabilities if configured, or delete explicitly)
      await client.query(`DELETE FROM "availability" WHERE "proposal_id" IN (SELECT "id" FROM "proposal" WHERE "meetup_id" = $1)`, [meetup.id]);
      await client.query(`DELETE FROM "proposal" WHERE "meetup_id" = $1`, [meetup.id]);

      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }
}

/**
 * Activates pending recurring meetups when their recurrence interval has passed.
 */
async function activateRecurringMeetups(pool: Pool) {
  const query = `
    SELECT * FROM "meetup" 
    WHERE "status" = 'Complete' 
      AND "recurs_on" IS NOT NULL
      AND NOW() >= "recurs_on"
  `;
  const { rows: meetups } = await pool.query(query);

  for (const meetup of meetups) {
    logger.info({ meetupId: meetup.id }, '[Decision Engine] Recurrence period reached for meetup. Transitioning to Planning...');
    await updateRecord(pool, 'meetup', meetup.id, { status: 'Planning', recurs_on: null });
  }
}

/**
 * Transitions polls from Posting to Voting when entry deadline is reached, or to Complete if no entries.
 */
async function transitionPollsToVoting(pool: Pool) {
  const query = `
    SELECT * FROM "poll" 
    WHERE "status" = 'Posting' 
      AND "entry_deadline" IS NOT NULL
      AND NOW() >= "entry_deadline"
  `;
  const { rows: polls } = await pool.query(query);

  for (const poll of polls) {
    const entryQuery = `SELECT COUNT(*) FROM "poll_entry" WHERE "poll_id" = $1`;
    const entryRes = await pool.query(entryQuery, [poll.id]);
    const entryCount = parseInt(entryRes.rows[0].count, 10);

    if (entryCount === 0) {
      logger.info({ pollId: poll.id }, '[Decision Engine] Poll entry deadline reached with no entries. Transitioning to Complete.');
      await updateRecord(pool, 'poll', poll.id, { status: 'Complete' });
      await notifyPollNoEntries(pool, poll.id);
    } else {
      logger.info({ pollId: poll.id }, '[Decision Engine] Poll entry deadline reached. Transitioning to Voting.');
      await updateRecord(pool, 'poll', poll.id, { status: 'Voting' });
      await notifyPollVotingOpen(pool, poll.id);
    }
  }
}

/**
 * Marks Voting polls as Complete once their vote deadline has passed and calculates winners.
 */
async function completeExpiredPolls(pool: Pool) {
  const query = `
    SELECT * FROM "poll" 
    WHERE "status" = 'Voting' 
      AND "vote_deadline" IS NOT NULL
      AND NOW() >= "vote_deadline"
  `;
  const { rows: polls } = await pool.query(query);

  for (const poll of polls) {
    logger.info({ pollId: poll.id }, '[Decision Engine] Poll vote deadline reached. Calculating winners...');
    
    const voteQuery = `
      SELECT poll_entry_id, COUNT(*) as vote_count 
      FROM "poll_vote" 
      WHERE "poll_id" = $1 
      GROUP BY poll_entry_id 
      ORDER BY vote_count DESC
    `;
    const voteRes = await pool.query(voteQuery, [poll.id]);
    
    if (voteRes.rows.length === 0) {
      logger.info({ pollId: poll.id }, '[Decision Engine] Poll vote deadline reached with no votes. Marking as Complete (no winners).');
      await updateRecord(pool, 'poll', poll.id, { status: 'Complete' });
      await notifyPollNoVotes(pool, poll.id);
      continue;
    }

    const maxVotes = parseInt(voteRes.rows[0].vote_count, 10);
    const winningEntryIds = voteRes.rows.filter(r => parseInt(r.vote_count, 10) === maxVotes).map(r => r.poll_entry_id);

    const client = await pool.connect();
    let winningEntriesDetails = [];
    try {
      await client.query('BEGIN');
      await client.query(`UPDATE "poll" SET "status" = 'Complete' WHERE "id" = $1`, [poll.id]);
      
      for (const entryId of winningEntryIds) {
        await client.query(`INSERT INTO "poll_winner" (poll_id, poll_entry_id) VALUES ($1, $2)`, [poll.id, entryId]);
      }
      
      const winningEntriesQuery = `
        SELECT pe.file_id, m.name as creator_name
        FROM "poll_entry" pe
        JOIN "member" m ON pe.creator_id = m.id
        WHERE pe.id = ANY($1::uuid[])
      `;
      const winningEntriesRes = await client.query(winningEntriesQuery, [winningEntryIds]);
      winningEntriesDetails = winningEntriesRes.rows;

      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      logger.error({ e, pollId: poll.id }, '[Decision Engine] Error saving poll winners');
      continue;
    } finally {
      client.release();
    }
    
    await notifyPollCompleted(pool, poll.id, winningEntriesDetails);
  }
}

/**
 * Main entry point to process all automated meetup status transitions.
 */
export async function processMeetupDecisions(pool: Pool) {
  try {
    await makeMeetupDecisions(pool);
    await transitionUpcomingMeetupsToOngoing(pool);
    await completeExpiredMeetups(pool);
    await activateRecurringMeetups(pool);

    await transitionPollsToVoting(pool);
    await completeExpiredPolls(pool);
  } catch (error) {
    logger.error({ error }, '[Decision Engine] Error processing meetup engine tasks');
  }
}
