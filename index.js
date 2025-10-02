const express = require('express');
const { Client } = require('pg');
const cors = require('cors'); // Import the cors package
const app = express();
const port = process.env.PORT || 3000;

// Enable CORS for all routes
app.use(cors());

const client = new Client({
  connectionString: process.env.DATABASE_URL,
});

client.connect();

app.get('/', (req, res) => {
  res.send('Hello World!');
});

app.get('/db', async (req, res) => {
  try {
    const result = await client.query('SELECT NOW()');
    res.send(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).send('Error connecting to database');
  }
});

// Example: Create a table
app.get('/create-table', async (req, res) => {
    try {
        const queryText = `
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                email VARCHAR(50) UNIQUE NOT NULL
            );
        `;
        await client.query(queryText);
        res.send('Table "users" created successfully (if it did not exist).');
    } catch (err) {
        console.error(err);
        res.status(500).send('Error creating table.');
    }
});

// Example: Insert data using parameterized query
app.get('/insert', async (req, res) => {
    try {
        const queryText = 'INSERT INTO users(name, email) VALUES($1, $2) RETURNING *';
        const values = ['John Doe', 'john.doe@example.com'];
        const result = await client.query(queryText, values);
        res.send(result.rows[0]);
    } catch (err) {
        console.error(err);
        res.status(500).send('Error inserting data. Have you created the table? Try /create-table first.');
    }
});

// Example: Select data
app.get('/users', async (req, res) => {
    try {
        const queryText = 'SELECT * FROM users';
        const result = await client.query(queryText);
        res.send(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).send('Error fetching users.');
    }
});


app.listen(port, () => {
  console.log(`Example app listening on port ${port}`);
});
