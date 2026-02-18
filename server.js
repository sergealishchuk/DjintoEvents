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

// --- HTTP endpoint for statistics (універсальний) ---
// app.post('/stats', async (req, res) => {
//   const apiKey = req.header('x-api-key');
//   if (!API_KEYS.has(apiKey)) {
//     return res.status(401).json({ error: 'Unauthorized' });
//   }

//   try {
//     const now = new Date();
    
//     // 1) Кількість активних користувачів online (за останні 15 хвилин)
//     const fifteenMinutesAgo = new Date(now.getTime() - 15 * 60 * 1000);
    
//     const onlineUsers = await Event.findAll({
//       attributes: [
//         [sequelize.fn('DISTINCT', sequelize.json('payload.userId')), 'userId']
//       ],
//       where: {
//         created_at: {
//           [Op.gte]: fifteenMinutesAgo
//         },
//         [Op.and]: sequelize.where(
//           sequelize.json('payload.userId'),
//           'IS NOT',
//           null
//         )
//       },
//       raw: true
//     });

//     // 2) Тренд реєстрацій за останні 30 днів
//     const thirtyDaysAgo = new Date(now);
//     thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 29); // -29 щоб включити поточний день
//     thirtyDaysAgo.setHours(0, 0, 0, 0);
    
//     const startOfToday = new Date(now);
//     startOfToday.setHours(0, 0, 0, 0);
    
//     const endOfToday = new Date(now);
//     endOfToday.setHours(23, 59, 59, 999);

//     // Отримуємо реєстрації згруповані по днях
//     const registrationsByDay = await Event.findAll({
//       attributes: [
//         [sequelize.fn('DATE', sequelize.col('created_at')), 'date'],
//         [sequelize.fn('COUNT', sequelize.fn('DISTINCT', sequelize.json('payload.userId'))), 'count']
//       ],
//       where: {
//         event_name: 'registration',
//         created_at: {
//           [Op.gte]: thirtyDaysAgo,
//           [Op.lte]: endOfToday
//         },
//         [Op.and]: sequelize.where(
//           sequelize.json('payload.userId'),
//           'IS NOT',
//           null
//         )
//       },
//       group: [sequelize.fn('DATE', sequelize.col('created_at'))],
//       order: [[sequelize.fn('DATE', sequelize.col('created_at')), 'ASC']],
//       raw: true
//     });

//     // Створюємо мапу для швидкого доступу
//     const registrationsMap = new Map();
//     registrationsByDay.forEach(item => {
//       const dateStr = item.date;
//       registrationsMap.set(dateStr, parseInt(item.count));
//     });

//     // Формуємо масив на 30 днів
//     const registrationTrends = [];
//     for (let i = 0; i < 30; i++) {
//       const currentDate = new Date(thirtyDaysAgo);
//       currentDate.setDate(thirtyDaysAgo.getDate() + i);
//       const dateStr = currentDate.toISOString().split('T')[0];
      
//       registrationTrends.push(registrationsMap.get(dateStr) || 0);
//     }

//     // 3) Погодинна активність за останні 24 години (від поточного моменту)
//     const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    
//     // Отримуємо активність по годинах
//     const hourlyActivity = await Event.findAll({
//       attributes: [
//         [sequelize.fn('EXTRACT', sequelize.literal('HOUR FROM created_at')), 'hour'],
//         [sequelize.fn('EXTRACT', sequelize.literal('DAY FROM created_at')), 'day'],
//         [sequelize.fn('EXTRACT', sequelize.literal('MONTH FROM created_at')), 'month'],
//         [sequelize.fn('EXTRACT', sequelize.literal('YEAR FROM created_at')), 'year'],
//         [sequelize.fn('COUNT', sequelize.fn('DISTINCT', sequelize.json('payload.userId'))), 'users']
//       ],
//       where: {
//         created_at: {
//           [Op.gte]: twentyFourHoursAgo,
//           [Op.lte]: now
//         },
//         [Op.and]: sequelize.where(
//           sequelize.json('payload.userId'),
//           'IS NOT',
//           null
//         )
//       },
//       group: [
//         sequelize.fn('EXTRACT', sequelize.literal('HOUR FROM created_at')),
//         sequelize.fn('EXTRACT', sequelize.literal('DAY FROM created_at')),
//         sequelize.fn('EXTRACT', sequelize.literal('MONTH FROM created_at')),
//         sequelize.fn('EXTRACT', sequelize.literal('YEAR FROM created_at'))
//       ],
//       order: [
//         [sequelize.fn('EXTRACT', sequelize.literal('YEAR FROM created_at')), 'ASC'],
//         [sequelize.fn('EXTRACT', sequelize.literal('MONTH FROM created_at')), 'ASC'],
//         [sequelize.fn('EXTRACT', sequelize.literal('DAY FROM created_at')), 'ASC'],
//         [sequelize.fn('EXTRACT', sequelize.literal('HOUR FROM created_at')), 'ASC']
//       ],
//       raw: true
//     });

//     // Створюємо мапу для швидкого доступу
//     const activityMap = new Map();
//     hourlyActivity.forEach(item => {
//       const key = `${item.year}-${item.month}-${item.day}-${item.hour}`;
//       activityMap.set(key, parseInt(item.users));
//     });

//     // Формуємо масив на 24 години (від поточного моменту назад)
//     const hourlyActivityFormatted = [];
//     const startTime = new Date(now);
//     startTime.setMinutes(0, 0, 0); // Початок поточної години
    
//     for (let i = 0; i < 24; i++) {
//       const currentHourTime = new Date(startTime.getTime() - i * 60 * 60 * 1000);
//       const year = currentHourTime.getFullYear();
//       const month = currentHourTime.getMonth() + 1;
//       const day = currentHourTime.getDate();
//       const hour = currentHourTime.getHours();
      
//       const key = `${year}-${month}-${day}-${hour}`;
//       const usersCount = activityMap.get(key) || 0;
      
//       // Додаємо на початок масиву, щоб зберегти хронологічний порядок
//       hourlyActivityFormatted.unshift({
//         hour: hour,
//         users: usersCount
//       });
//     }

//     // Формуємо відповідь
//     const response = {
//       online_users: onlineUsers.length,
//       registration_trends: registrationTrends,
//       hourly_activity: hourlyActivityFormatted
//     };

//     res.json(response);
//   } catch (err) {
//     console.error('Statistics error:', err);
//     res.status(500).json({ error: 'Internal server error' });
//   }
// });

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
    
    console.log('=================================');
    console.log(`Server time (UTC): ${nowUTC.toISOString()}`);
    console.log(`Local time (UTC+${timezoneOffset/60}): ${nowLocal.toISOString()}`);
    console.log(`Timezone offset: ${timezoneOffset} minutes`);
    console.log('=================================');

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
    
    console.log(`Current hour start (local): ${currentHourStartLocal.toISOString()}`);
    console.log(`24 hours ago start (local): ${twentyFourHoursAgoStartLocal.toISOString()}`);

    // Для пошуку в БД нам потрібні UTC часи
    // ВАЖЛИВО: created_at в БД зберігається в UTC
    // Нам потрібні події, де created_at (UTC) відповідає локальному часу в діапазоні
    const twentyFourHoursAgoStartUTC = new Date(twentyFourHoursAgoStartLocal.getTime() - timezoneOffset * 60 * 1000);
    const currentHourStartUTC = new Date(currentHourStartLocal.getTime() - timezoneOffset * 60 * 1000);
    
    console.log(`Search range UTC: ${twentyFourHoursAgoStartUTC.toISOString()} to ${currentHourStartUTC.toISOString()}`);

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
        [sequelize.fn('COUNT', sequelize.fn('DISTINCT', sequelize.json('payload.userId'))), 'users']
      ],
      where: {
        created_at: {
          [Op.gte]: twentyFourHoursAgoStartUTC,
          [Op.lt]: currentHourStartUTC
        }
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

    console.log('Hourly activity from DB:', hourlyActivity.map(item => ({
      time: `${item.year}-${item.month}-${item.day} ${item.hour}:00 (local)`,
      users: item.users
    })));

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
    console.log('\nChecking specific records:');
    
    // Всі записи з сьогоднішнього дня
    const todayStart = new Date(nowLocal);
    todayStart.setHours(0, 0, 0, 0);
    const todayStartUTC = new Date(todayStart.getTime() - timezoneOffset * 60 * 1000);
    
    console.log(`Today's records (since ${todayStartUTC.toISOString()} UTC):`);
    
    const todayEvents = await Event.findAll({
      where: {
        created_at: {
          [Op.gte]: todayStartUTC
        }
      },
      order: [['created_at', 'ASC']],
      raw: true
    });
    
    todayEvents.forEach(event => {
      const eventLocal = new Date(event.created_at.getTime() + timezoneOffset * 60 * 1000);
      console.log(`- ${event.created_at.toISOString()} UTC -> ${eventLocal.getHours()}:${eventLocal.getMinutes()} local, userId: ${event.payload?.userId}`);
    });

    console.log('\nFinal hourly_activity:');
    hourlyActivityFormatted.forEach((h, idx) => {
      console.log(`[${idx}] ${h.hour}:00 -> ${h.users} users`);
    });
    console.log('=================================');

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
          to_utc: currentHourStartUTC.toISOString()
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

