const pg = require('pg');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
require('dotenv').config({ path: path.resolve(__dirname, '../.env') });

const { APP_ENV } = process.env;
global.APP_ENV = APP_ENV || 'development';

console.log('Environment:', global.APP_ENV);

const config = require('../config/config');
const { sequelize } = require('../models');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

const askYesNo = (question) =>
  new Promise((resolve) => {
    rl.question(`${question} [Y/N]: `, (answer) => {
      resolve(answer.trim().toLowerCase() === 'y');
    });
  });

/**
 * Connect to postgres without a specific DB
 * (required for CREATE DATABASE)
 */
const getAdminPool = () =>
  new pg.Pool({
    host: config.sequelize.host,
    user: config.sequelize.username,
    password: config.sequelize.password,
    port: config.sequelize.port || 5432,
    database: 'postgres',
  });

const checkDatabaseExists = async (dbName) => {
  const pool = getAdminPool();
  try {
    const res = await pool.query(
      'SELECT 1 FROM pg_database WHERE datname = $1',
      [dbName]
    );
    return res.rowCount > 0;
  } finally {
    await pool.end();
  }
};

const createDatabase = async (dbName) => {
  const pool = getAdminPool();
  try {
    await pool.query(`CREATE DATABASE "${dbName}"`);
  } finally {
    await pool.end();
  }
};

const populateData = async () => {
  const sqlPath = path.resolve(__dirname, '../data-populate.sql');
  const sql = fs.readFileSync(sqlPath, 'utf8');

  const pool = new pg.Pool({
    ...config.sequelize,
    user: config.sequelize.username,
  });

  const client = await pool.connect();
  try {
    await client.query(sql);
  } finally {
    client.release();
    await pool.end();
  }
};

const refresh = async () => {
  const dbName = config.sequelize.database;
  const host = config.sequelize.host;

  const exists = await checkDatabaseExists(dbName);

  if (!exists) {
    const confirm = await askYesNo(
      `Database "${dbName}" on host "${host}" will be created.\n` +
      `All existing data (if any) will be lost.\n` +
      `Proceed with creation?`
    );

    if (!confirm) {
      console.log('Operation cancelled.');
      process.exit(0);
    }

    console.log(`Creating database "${dbName}" on host "${host}"...`);
    await createDatabase(dbName);
  } else {
    const confirm = await askYesNo(
      `Recreate database "${dbName}" on host "${host}"?`
    );

    if (!confirm) {
      console.log('Operation cancelled.');
      process.exit(0);
    }
  }

  console.log('Recreating schema...');
  await sequelize.sync({ force: true });

  console.log('Populating data. Please wait...');
  await populateData();

  console.log('✅ Done');
  rl.close();
  process.exit(0);
};

refresh().catch((err) => {
  console.error('Fatal error:', err);
  rl.close();
  process.exit(1);
});
