'use strict';
const { Model } = require('sequelize');

module.exports = (sequelize, DataTypes) => {
  class Events extends Model {}

  Events.init(
    {
      host: { type: DataTypes.STRING, allowNull: false },
      event_name: { type: DataTypes.STRING, allowNull: false },
      payload: { type: DataTypes.JSONB, allowNull: true },
      created_at: { type: DataTypes.DATE, defaultValue: DataTypes.NOW }
    },
    {
      sequelize,
      modelName: 'Events',  // модель у JS
      tableName: 'events',  // таблиця у Postgres
      timestamps: false
    }
  );

  return Events;
};
