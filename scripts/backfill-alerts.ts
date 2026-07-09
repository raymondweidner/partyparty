import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://postgres:@127.0.0.1:5432/postgres',
});

async function run() {
  console.log('Starting backfill of registry_item_updated alert preference...');
  try {
    const memberRes = await pool.query('SELECT id FROM "member"');
    console.log(`Found ${memberRes.rows.length} members. Backfilling...`);
    
    for (const row of memberRes.rows) {
      // Use standard INSERT ON CONFLICT DO NOTHING (Postgres 9.5+)
      await pool.query(`
        INSERT INTO "member_alert_preference" (member_id, alert_type, email_enabled, push_enabled)
        VALUES ($1, $2, true, true)
        ON CONFLICT (member_id, alert_type) DO NOTHING
      `, [row.id, 'registry_item_updated']);
    }
    
    console.log('Backfill complete!');
  } catch (err) {
    console.error('Error during backfill:', err);
  } finally {
    await pool.end();
  }
}

run();
