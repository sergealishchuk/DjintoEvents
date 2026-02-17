const express = require('express');
const bodyParser = require('body-parser');
const redis = require('redis');
const { Sequelize } = require('sequelize');
const EventModel = require('./models/Event');

const app = express();
app.use(bodyParser.json());

// --- Redis ---
const redisClient = redis.createClient({ url: 'redis://localhost:6379' });
redisClient.connect().catch(console.error);

// --- Sequelize Postgres ---
const sequelize = new Sequelize('postgres://backofficeuser:@N12vte81@localhost:5432/djintoEventsDB', {
  logging: false, // вимикає вивід SQL-запитів у консоль
});
const Event = EventModel(sequelize, Sequelize.DataTypes);

// --- Allowed API keys ---
const API_KEYS = new Set(['SUPERSECRETKEY123']);

// --- HTTP endpoint ---
app.post('/track', async (req, res) => {
  const apiKey = req.header('x-api-key');
  if (!API_KEYS.has(apiKey)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { event_name, payload, host } = req.body;
  if (!event_name || !host) {
    return res.status(400).json({ error: 'event_name and host required' });
  }

  const event = { event_name, payload, host, created_at: new Date() };
  console.log('event received:', event);
  try {
    await redisClient.rPush('events_queue', JSON.stringify(event));
    res.sendStatus(204); // fire-and-forget
  } catch (err) {
    console.error('Redis push error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// --- Worker: batch insert у Postgres ---
const BATCH_SIZE = 500;
const FLUSH_INTERVAL_MS = 1000;

setInterval(async () => {
  try {
    const len = await redisClient.lLen('events_queue');
    if (!len) return;

    const batchSize = Math.min(len, BATCH_SIZE);
    const batch = await redisClient.lRange('events_queue', 0, batchSize - 1);
    if (batch.length === 0) return;

    const events = batch.map(e => JSON.parse(e));

    await Event.bulkCreate(events);
    await redisClient.lTrim('events_queue', batchSize, -1);
  } catch (err) {
    console.error('Worker error:', err);
  }
}, FLUSH_INTERVAL_MS);

// --- Start server ---
const PORT = process.env.PORT || 5567;
app.listen(PORT, () => console.log(`Tracking server listening on port ${PORT}`));
