import { Pool } from 'pg';

/**
 * Dummy function to select a proposal based on the meetup's decision method.
 */
async function executeDecisionMethod(meetup: any, pool: Pool): Promise<string | null> {
  console.log(`[Decision Engine] Executing dummy decision method '${meetup.decision_method}' for meetup ${meetup.id}`);
  
  const query = `SELECT * FROM "proposal" WHERE "meetup_id" = $1 AND "status" = 'pending'`;
  const proposalsRes = await pool.query(query, [meetup.id]);
  const proposals = proposalsRes.rows;

  if (proposals.length === 0) {
    console.log(`[Decision Engine] No pending proposals found for meetup ${meetup.id}`);
    return null;
  }

  // Dummy logic: Randomly select one proposal
  const selectedIndex = Math.floor(Math.random() * proposals.length);
  return proposals[selectedIndex].id;
}

/**
 * Clones a completed meetup to reschedule it based on its recurs_every_days interval.
 */
async function rescheduleMeetup(meetup: any, pool: Pool) {
  try {
    console.log(`[Decision Engine] Rescheduling meetup ${meetup.id} (recurs every ${meetup.recurs_every_days} days).`);
    const query = `
      INSERT INTO "meetup" (
        "creator_id", "tribe_id", "title", "details", 
        "status", "created_at", "decision_method", 
        "days_to_decide", "recurs_every_days"
      ) VALUES ($1, $2, $3, $4, 'pending', NOW(), $5, $6, $7)
      RETURNING id
    `;
    const values = [
      meetup.creator_id,
      meetup.tribe_id,
      meetup.title,
      meetup.details,
      meetup.decision_method,
      meetup.days_to_decide,
      meetup.recurs_every_days
    ];
    const res = await pool.query(query, values);
    console.log(`[Decision Engine] Successfully rescheduled meetup. New Meetup ID: ${res.rows[0].id}`);
  } catch (error) {
    console.error(`[Decision Engine] Failed to reschedule meetup ${meetup.id}:`, error);
  }
}

/**
 * Handles deciding on active meetups that have passed their decision deadline.
 */
async function makeMeetupDecisions(pool: Pool) {
  const query = `
    SELECT * FROM "meetup" 
    WHERE "status" = 'active' 
      AND "days_to_decide" IS NOT NULL
      AND NOW() >= "created_at" + ("days_to_decide" * INTERVAL '1 day')
  `;
  const { rows: meetups } = await pool.query(query);

  for (const meetup of meetups) {
    console.log(`[Decision Engine] Deadline reached for meetup ${meetup.id}. Selecting proposal...`);
    const selectedProposalId = await executeDecisionMethod(meetup, pool);
    
    if (selectedProposalId) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(`UPDATE "proposal" SET "status" = 'accepted' WHERE "id" = $1`, [selectedProposalId]);
        await client.query(`UPDATE "proposal" SET "status" = 'rejected' WHERE "meetup_id" = $1 AND "id" != $2`, [meetup.id, selectedProposalId]);
        await client.query(`UPDATE "meetup" SET "status" = 'decided' WHERE "id" = $1`, [meetup.id]);
        await client.query('COMMIT');
        console.log(`[Decision Engine] Meetup ${meetup.id} marked as decided.`);
      } catch (e) {
        await client.query('ROLLBACK');
        throw e;
      } finally {
        client.release();
      }
    } else {
      await pool.query(`UPDATE "meetup" SET "status" = 'cancelled' WHERE "id" = $1`, [meetup.id]);
      console.log(`[Decision Engine] Meetup ${meetup.id} marked as cancelled (no proposals).`);
    }
  }
}

/**
 * Marks decided meetups as completed once their selected proposal has expired.
 */
async function completeExpiredMeetups(pool: Pool) {
  const query = `
    SELECT m.*, p.id as proposal_id 
    FROM "meetup" m
    JOIN "proposal" p ON p.meetup_id = m.id
    WHERE m.status = 'decided' 
      AND p.status = 'accepted'
      AND NOW() >= p.date + INTERVAL '1 day'
  `;
  const { rows: meetups } = await pool.query(query);

  for (const meetup of meetups) {
    console.log(`[Decision Engine] Accepted proposal expired for meetup ${meetup.id}. Marking as completed...`);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`UPDATE "proposal" SET "status" = 'completed' WHERE "id" = $1`, [meetup.proposal_id]);
      await client.query(`UPDATE "meetup" SET "status" = 'completed' WHERE "id" = $1`, [meetup.id]);
      await client.query('COMMIT');
      
      // Reschedule if the meetup is recurring
      if (meetup.recurs_every_days && meetup.recurs_every_days > 0) {
        await rescheduleMeetup(meetup, pool);
      }
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
    WHERE "status" = 'pending' 
      AND "recurs_every_days" IS NOT NULL
      AND NOW() >= "created_at" + ("recurs_every_days" * INTERVAL '1 day')
  `;
  const { rows: meetups } = await pool.query(query);

  for (const meetup of meetups) {
    console.log(`[Decision Engine] Recurrence period reached for meetup ${meetup.id}. Transitioning to active...`);
    await pool.query(`UPDATE "meetup" SET "status" = 'active', "created_at" = NOW() WHERE "id" = $1`, [meetup.id]);
  }
}

/**
 * Main entry point to process all automated meetup status transitions.
 */
export async function processMeetupDecisions(pool: Pool) {
  try {
    await makeMeetupDecisions(pool);
    await completeExpiredMeetups(pool);
    await activateRecurringMeetups(pool);
  } catch (error) {
    console.error('[Decision Engine] Error processing meetup engine tasks:', error);
  }
}
