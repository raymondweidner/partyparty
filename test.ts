import { Pool } from 'pg';

async function main() {
  const pool = new Pool({ connectionString: 'postgres://postgres:postgres@localhost:5432/postgres' });
  const res = await pool.query('SELECT column_name FROM information_schema.columns WHERE table_name = $1', ['member_contact']);
  console.log(res.rows);
  await pool.end();
}
main();
