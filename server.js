(function () {
  "use strict";

var fs = require('fs'),
    redis = require("redis"),
    nconf = require('nconf'),
    path = require('path'),
    syslog = require('node-syslog'),
    async = require('async'),
    subClient, pubClient, config,
    moment = require('moment-timezone'),
    underscore = require('underscore'),
    Sequelize = require("sequelize"),
    CBuffer = require('CBuffer'),
    numbers = [], models = {}, db = {}, job, purgeJob,
    configFile,
    latestUids = new CBuffer(20),
    sendReady = function() {
      var message = {
        status: "ok"
      };
      pubClient.publish("disneydining:readysearch", JSON.stringify(message));
    },
    setSubscription = function() {
      subClient.psubscribe("disneydining:*");
      console.log("Subscribing");
      subClient.on("pmessage", function (pattern, channel, message) {
        var _channel = channel.split(":"),
            subChannel = _channel[1];
        console.log("channel ", channel, ": ", message);
        message = JSON.parse(message);
        if (subChannel === "getsearch") {
          getSearch(message);
        }
      });
    };

var init = function() {
  syslog.init("crawler", syslog.LOG_PID || syslog.LOG_ODELAY, syslog.LOG_LOCAL0);
  syslog.log(syslog.LOG_INFO, "Server started");
  if (process.argv[2]) {
    if (fs.lstatSync(process.argv[2])) {
        configFile = require(process.argv[2]);
    } else {
        configFile = process.cwd() + '/config/settings.json';
    }
  } else {
    configFile = process.cwd()+'/config/settings.json';
  }

  config = nconf
  .argv()
  .env("__")
  .file({ file: configFile });
  
  subClient = redis.createClient(
    config.get("redis:port"),
    config.get("redis:host")
  );
  if (config.get("redis:db")) {
    subClient.select(
      config.get("redis:db"),
      function() {
        setSubscription();
      }
    );
  } else {
    setSubscription();
  }
  
  pubClient = redis.createClient(
    config.get("redis:port"),
    config.get("redis:host")
  );
  if (config.get("redis:db") && config.get("redis:db") > 0) {
    pubClient.select(
      config.get("redis:db"),
      function() {
        //console.log("Redis DB set to:", config.get("redis:db"));
        sendReady();
      }
    );
  }

  if (config.get("log")) {
    var access_logfile = fs.createWriteStream(config.get("log"), {flags: 'a'});
  }

  db.dining = new Sequelize(
    config.get("mysql:database"),
    config.get("mysql:username"),
    config.get("mysql:password"),
    {
      dialect: 'mariadb',
      omitNull: true,
      logging: config.get("mysql:logging") || false,
      host: config.get("mysql:host") || "localhost",
      port: config.get("mysql:port") || 3306,
      pool: { maxConnections: 5, maxIdleTime: 30},
      define: {
        freezeTableName: true,
        timestamps: true,
        paranoid: true
      }
  });

  models.Restaurants = db.dining.define('restaurants', {
    id:                   { type: Sequelize.STRING(255), primaryKey: true },
    name :                { type: Sequelize.STRING(255) }
  });

  models.UserSearches = db.dining.define('userSearches', {
    id:                   { type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true },
    restaurant:           { type: Sequelize.STRING(255) },
    date:                 {
      type: Sequelize.DATE,
      defaultValue: null,
      get: function(name) {
        return moment.utc(this.getDataValue(name)).format("YYYY-MM-DD HH:mm:ssZ");
      }
    },
    partySize:            { type: Sequelize.INTEGER },
    uid:                  { type: Sequelize.STRING(255) },
    user:                 { type: Sequelize.STRING(255) },
    enabled:              { type: Sequelize.BOOLEAN, defaultValue: 1 },
    deleted:              { type: Sequelize.BOOLEAN, defaultValue: 0 },
    lastEmailNotification:{
      type: Sequelize.DATE,
      defaultValue: null,
      get: function(name) {
        return moment.utc(this.getDataValue(name)).format("YYYY-MM-DD HH:mm:ssZ");
      }
    },
    lastSMSNotification:  {
      type: Sequelize.DATE,
      defaultValue: null,
      get: function(name) {
        return moment.utc(this.getDataValue(name)).format("YYYY-MM-DD HH:mm:ssZ");
      }
    }
  });

  models.GlobalSearches = db.dining.define('globalSearches', {
    id:                   { type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true },
    lastChecked:          {
      type: Sequelize.DATE,
      defaultValue: null,
      get: function(name) {
        return moment.utc(this.getDataValue(name)).format("YYYY-MM-DD HH:mm:ssZ");
      }
    },
    uid:                  { type: Sequelize.STRING(255) }
  });

  models.SearchLogs = db.dining.define('searchLogs',
    {
      id:                   { type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true },
      uid:                  { type: Sequelize.STRING(255) },
      dateSearched:         {
        type: Sequelize.DATE,
        defaultValue: null,
        get: function(name) {
          return moment.utc(this.getDataValue(name)).format("YYYY-MM-DD HH:mm:ssZ");
        }
      },
      message:              { type: Sequelize.STRING(255) },
      foundSeats:           { type: Sequelize.BOOLEAN, defaultValue: 0 },
      times:                { type: Sequelize.STRING }
    }
  );

  models.Users = db.dining.define('users', {
    id:                   { type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true },
    email :               { type: Sequelize.STRING(255) },
    password :            { type: Sequelize.STRING(255) },
    firstName :           { type: Sequelize.STRING(255) },
    lastName :            { type: Sequelize.STRING(255) },
    zipCode:              { type: Sequelize.STRING(15) },
    phone :               { type: Sequelize.STRING(25) },
    carrier:              { type: Sequelize.STRING(100) },
    sendTxt :             { type: Sequelize.BOOLEAN, defaultValue: 0 },
    sendEmail :           { type: Sequelize.BOOLEAN, defaultValue: 1 },
    emailTimeout:         { type: Sequelize.INTEGER, defaultValue: 14400 },
    smsTimeout:           { type: Sequelize.INTEGER, defaultValue: 14400 },
    activated :           { type: Sequelize.BOOLEAN, defaultValue: 0 },
    admin :               { type: Sequelize.BOOLEAN, defaultValue: 0 },
    subExpires:           {
      type: Sequelize.DATE,
      defaultValue: null,
      get: function(name) {
        return moment.utc(this.getDataValue(name)).format("YYYY-MM-DD HH:mm:ssZ");
      }
    },
    eula:           {
      type: Sequelize.DATE,
      defaultValue: null,
      get: function(name) {
        return moment.utc(this.getDataValue(name)).format("YYYY-MM-DD HH:mm:ssZ");
      }
    }
  });

  models.SmsGateways = db.dining.define('smsGateways', {
    id:                   { type: Sequelize.STRING(100), primaryKey: true },
    country :             { type: Sequelize.STRING(255) },
    name :                { type: Sequelize.STRING(255) },
    gateway :             { type: Sequelize.STRING(255) },
    prepend:              { type: Sequelize.STRING(255) }
  });

  models.PasswordReset = db.dining.define('passwordReset', {
    id:                   { type: Sequelize.INTEGER(11), primaryKey: true },
    user :                { type: Sequelize.INTEGER(11) },
    token :               { type: Sequelize.STRING(255) },
    expire :              { type: Sequelize.DATE, defaultValue: '1969-01-01 00:00:00' },
    used :                { type: Sequelize.BOOLEAN, defaultValue: 0 }
  });

  models.ActivationCodes = db.dining.define('activationCodes', {
    id:                   { type: Sequelize.INTEGER(11), primaryKey: true },
    token :               { type: Sequelize.STRING(255) },
    used :                { type: Sequelize.BOOLEAN, defaultValue: 0 }
  });

};

var getSearch = function(search) {
  var now = parseInt(moment().tz("America/New_York").format("H"), 10),
      offset = (now >= 3 && now < 6) ? "30" : "5",
      limit = config.get("limit") ? config.get("limit") : "10",
      sql = "SELECT "+
            " globalSearches.*, "+
            " userSearches.restaurant, "+
            " restaurants.name, "+
            " userSearches.date, "+
            " userSearches.partySize "+
            "FROM globalSearches "+
            "JOIN userSearches ON globalSearches.uid = userSearches.uid "+
            "JOIN restaurants ON userSearches.restaurant = restaurants.id "+
            "WHERE userSearches.date >= UTC_TIMESTAMP() + INTERVAL 1 HOUR AND globalSearches.lastChecked < UTC_TIMESTAMP() - INTERVAL "+offset+" MINUTE "+
            " AND userSearches.deleted = 0 AND userSearches.enabled = 1 AND globalSearches.deletedAt IS NULL ";
    if (latestUids.toArray().length > 0) {
      var uids = latestUids.toArray().map(function(uid){
          // This will wrap each element of the uids array with quotes
          return "'" + uid + "'";
      }).join(",");
      sql += " AND globalSearches.uid NOT IN ("+uids+") ";
    }
    sql += "GROUP BY globalSearches.uid ";
    sql += "ORDER BY globalSearches.lastChecked ASC ";
    sql += "LIMIT 1";

  db.dining.query(
    sql,
    { 
      replacements: {},
      type: Sequelize.QueryTypes.SELECT 
    }
  ).then(
    function(searches) {
      if (searches.length > 0) {
        var message = underscore.extend(searches[0], search);
        latestUids.push(message.uid);
        pubClient.publish("disneydining:sendsearch", JSON.stringify(message));
      }
    }
  );
};

init();

}());
