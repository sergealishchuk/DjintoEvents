const express = require('express');
const bodyParser = require('body-parser');
const redis = require('redis');
const { Sequelize, Op } = require('sequelize');
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

// --- HTTP endpoint for tracking ---
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

app.post('/stats', async (req, res) => {
  const apiKey = req.header('x-api-key');
  if (!API_KEYS.has(apiKey)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    // Отримуємо часовий офсет з тіла запиту (в хвилинах)
    const timezoneOffset = parseInt(req.body.timezone_offset) || 0;

    // Поточний час в UTC на сервері
    const nowUTC = new Date();

    // Локальний час клієнта = UTC + timezoneOffset хвилин
    const nowLocal = new Date(nowUTC.getTime() + timezoneOffset * 60 * 1000);

    // 1) Кількість активних користувачів online (за останні 15 хвилин UTC)
    const fifteenMinutesAgoUTC = new Date(nowUTC.getTime() - 15 * 60 * 1000);

    const onlineUsers = await Event.findAll({
      attributes: [
        [sequelize.fn('DISTINCT', sequelize.json('payload.userId')), 'userId']
      ],
      where: {
        created_at: {
          [Op.gte]: fifteenMinutesAgoUTC
        },
        [Op.and]: sequelize.where(
          sequelize.json('payload.userId'),
          'IS NOT',
          null
        )
      },
      raw: true
    });

    // 2) Тренд реєстрацій за останні 30 днів
    const registrationsByDay = await Event.findAll({
      attributes: [
        [sequelize.fn('DATE',
          sequelize.literal(`(created_at + INTERVAL '${timezoneOffset} minutes')`)),
          'date'
        ],
        [sequelize.fn('COUNT', sequelize.fn('DISTINCT', sequelize.json('payload.userId'))), 'count']
      ],
      where: {
        event_name: 'registration',
        created_at: {
          [Op.gte]: new Date(nowUTC.getTime() - 30 * 24 * 60 * 60 * 1000)
        }
      },
      group: [sequelize.fn('DATE',
        sequelize.literal(`(created_at + INTERVAL '${timezoneOffset} minutes')`))
      ],
      order: [[sequelize.literal('date'), 'ASC']],
      raw: true
    });

    // Створюємо мапу реєстрацій
    const registrationsMap = new Map();
    registrationsByDay.forEach(item => {
      registrationsMap.set(item.date, parseInt(item.count));
    });

    // Формуємо 30 днів тренду
    const registrationTrends = [];
    const startDate = new Date(nowLocal);
    startDate.setDate(startDate.getDate() - 29);
    startDate.setHours(0, 0, 0, 0);

    for (let i = 0; i < 30; i++) {
      const currentDate = new Date(startDate);
      currentDate.setDate(startDate.getDate() + i);

      const year = currentDate.getFullYear();
      const month = String(currentDate.getMonth() + 1).padStart(2, '0');
      const day = String(currentDate.getDate()).padStart(2, '0');
      const dateStr = `${year}-${month}-${day}`;

      registrationTrends.push(registrationsMap.get(dateStr) || 0);
    }

    // 3) Погодинна активність за останні 24 години
    // Поточна година в локальному часі (початок години)
    const currentHourStartLocal = new Date(nowLocal);
    currentHourStartLocal.setMinutes(0, 0, 0);

    // 24 години тому (початок години) в локальному часі
    const twentyFourHoursAgoStartLocal = new Date(currentHourStartLocal);
    twentyFourHoursAgoStartLocal.setHours(twentyFourHoursAgoStartLocal.getHours() - 24);

    // Для пошуку в БД нам потрібні UTC часи
    const twentyFourHoursAgoStartUTC = new Date(twentyFourHoursAgoStartLocal.getTime() - timezoneOffset * 60 * 1000);

    // Отримуємо активність з БД
    const hourlyActivity = await Event.findAll({
      attributes: [
        [sequelize.fn('EXTRACT',
          sequelize.literal(`HOUR FROM (created_at + INTERVAL '${timezoneOffset} minutes')`)),
          'hour'
        ],
        [sequelize.fn('EXTRACT',
          sequelize.literal(`DAY FROM (created_at + INTERVAL '${timezoneOffset} minutes')`)),
          'day'
        ],
        [sequelize.fn('EXTRACT',
          sequelize.literal(`MONTH FROM (created_at + INTERVAL '${timezoneOffset} minutes')`)),
          'month'
        ],
        [sequelize.fn('EXTRACT',
          sequelize.literal(`YEAR FROM (created_at + INTERVAL '${timezoneOffset} minutes')`)),
          'year'
        ],
        [sequelize.fn('COUNT',
          sequelize.fn('DISTINCT',
            sequelize.literal(`payload->>'userId'`)
          )
        ), 'users']
      ],
      where: {
        created_at: {
          [Op.gte]: twentyFourHoursAgoStartUTC,
          [Op.lt]: nowUTC
        },
        [Op.and]: sequelize.literal(`payload->>'userId' IS NOT NULL`)
      },
      group: [
        sequelize.fn('EXTRACT',
          sequelize.literal(`YEAR FROM (created_at + INTERVAL '${timezoneOffset} minutes')`)
        ),
        sequelize.fn('EXTRACT',
          sequelize.literal(`MONTH FROM (created_at + INTERVAL '${timezoneOffset} minutes')`)
        ),
        sequelize.fn('EXTRACT',
          sequelize.literal(`DAY FROM (created_at + INTERVAL '${timezoneOffset} minutes')`)
        ),
        sequelize.fn('EXTRACT',
          sequelize.literal(`HOUR FROM (created_at + INTERVAL '${timezoneOffset} minutes')`)
        )
      ],
      order: [
        [sequelize.literal('year'), 'ASC'],
        [sequelize.literal('month'), 'ASC'],
        [sequelize.literal('day'), 'ASC'],
        [sequelize.literal('hour'), 'ASC']
      ],
      raw: true
    });

    // Створюємо мапу для швидкого доступу
    const activityMap = new Map();
    hourlyActivity.forEach(item => {
      const year = Math.floor(parseFloat(item.year));
      const month = Math.floor(parseFloat(item.month));
      const day = Math.floor(parseFloat(item.day));
      const hour = Math.floor(parseFloat(item.hour));
      const key = `${year}-${month}-${day}-${hour}`;
      activityMap.set(key, parseInt(item.users));
    });

    // Формуємо масив на 24 години
    const hourlyActivityFormatted = [];

    for (let i = 0; i < 24; i++) {
      const hourLocal = new Date(twentyFourHoursAgoStartLocal);
      hourLocal.setHours(twentyFourHoursAgoStartLocal.getHours() + i);

      const year = hourLocal.getFullYear();
      const month = hourLocal.getMonth() + 1;
      const day = hourLocal.getDate();
      const hour = hourLocal.getHours();

      const key = `${year}-${month}-${day}-${hour}`;
      const usersCount = activityMap.get(key) || 0;

      hourlyActivityFormatted.push({
        hour: hour,
        users: usersCount
      });

    }

    // Перевіряємо конкретні записи

    const todayStart = new Date(nowLocal);
    todayStart.setHours(0, 0, 0, 0);
    const todayStartUTC = new Date(todayStart.getTime() - timezoneOffset * 60 * 1000);

    const todayEvents = await Event.findAll({
      where: {
        created_at: {
          [Op.gte]: todayStartUTC
        }
      },
      order: [['created_at', 'ASC']],
      raw: true
    });

    const response = {
      online_users: onlineUsers.length,
      registration_trends: registrationTrends,
      hourly_activity: hourlyActivityFormatted,
      meta: {
        timezone_offset_used: timezoneOffset,
        server_time_utc: nowUTC.toISOString(),
        client_local_time: nowLocal.toISOString(),
        current_hour_local: currentHourStartLocal.toISOString(),
        search_range: {
          from_utc: twentyFourHoursAgoStartUTC.toISOString(),
          to_utc: nowUTC.toISOString()
        }
      }
    };

    res.json(response);
  } catch (err) {
    console.error('Statistics error:', err);
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

