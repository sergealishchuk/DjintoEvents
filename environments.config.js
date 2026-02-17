module.exports = {
  "local": {
    "sequelize": {
      "username": "backofficeuser",
      "password": "@N12vte81",
      "database": "djintoEventsDB",
      "host": "localhost",
      "dialect": "postgres",
      logging: false,
      pool: {
        max: 60,
        min: 0,
        acquire: 60000,
        idle: 10000
      },
    },
    "frontend": {
      "host": "http://localhost",
      "port": 5567,
    },
  },
};