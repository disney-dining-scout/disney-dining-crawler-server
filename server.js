(function () {
  "use strict";

var fs = require('fs'),
    redis = require("redis"),
    nconf = require('nconf'),
    path = require('path'),
    syslog = require('node-syslog'),
    async = require('async'),
    mysql = require('mysql'),
    subClient, pubClient, config,
    moment = require('moment-timezone'),
    underscore = require('underscore'),
    Sequelize = require("sequelize"),
    CBuffer = require('CBuffer'),
    numbers = [], models = {}, db = {}, job, purgeJob,
    configFile, pool,
    latestUids = new CBuffer(20),
    sendReady = function() {
      var message = {
        status: "ok"
      };
      console.log("Send Ready");
      pubClient.publish("disneydining:readysearch", JSON.stringify(message));
    },
    setSubscription = function() {
      subClient.psubscribe("disneydining:*");
      console.log("Subscribing");
      subClient.on("pmessage", function (pattern, channel, message) {
        var _channel = channel.split(":"),
            subChannel = _channel[1];
        //console.log("channel ", channel, ": ", message);
        message = JSON.parse(message);
        if (subChannel === "getsearch") {
          getSearch(message);
        }
      });
    },
    connectDB = function() {
      pool = mysql.createPool({
        connectionLimit : 10,
        host            : config.get("mysql:host") || "localhost",
        port            : config.get("mysql:port") || 3306,
        user            : config.get("mysql:username"),
        password        : config.get("mysql:password"),
        database        : config.get("mysql:database")
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
  
  connectDB();

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
  if (config.get("redis:db")) {
    pubClient.select(
      config.get("redis:db"),
      function() {
        //console.log("Redis DB set to:", config.get("redis:db"));
      }
    );
  }
  setTimeout(
    function() {
      sendReady();
    },
    3000
  );

  if (config.get("log")) {
    var access_logfile = fs.createWriteStream(config.get("log"), {flags: 'a'});
  }

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

  pool.getConnection(
    function(err, connection) {
      connection.query(
        sql,
        function(error, searches) {
          if (!error) {
            if (searches.length > 0) {
              var message = underscore.extend(searches[0], search);
              latestUids.push(message.uid);
              pubClient.publish("disneydining:sendsearch", JSON.stringify(message));
            }
          }
          connection.release();
        }
      );
    }
  );
};

init();

}());
