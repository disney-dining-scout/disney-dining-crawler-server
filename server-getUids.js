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
      numbers = [], models = {}, db = {}, job, purgeJob,
      configFile, pool, queue, subCounter = 0,
      setSubscription = function() {
        subClient.psubscribe("disneydining:*");
        console.log("Subscribing");
        subClient.on("pmessage", function (pattern, channel, message) {
          var _channel = channel.split(":"),
              subChannel = _channel[1];
          message = JSON.parse(message);
          if (subChannel === "getsearch") {
            //console.log("channel ", channel, ": ", message);
            //getSearch(message);
            queue.push(
              message,
              function (err) {
                console.log('finished processing search request', message.clientId);
              }
            );
          }
        });
      },
      getConfiguration = function() {
        config = nconf
        .argv()
        .env("__")
        .file({ file: configFile });
      },
      refreshConfiguration = function() {
        console.log("refreshing configuration");
        getConfiguration();
        setTimeout(
          refreshConfiguration,
          300000
        );
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

      },
      init = function() {
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

        getConfiguration();

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
        if (config.get("redis:db") >= 0) {
          pubClient.select(
            config.get("redis:db"),
            function() {
              //console.log("Redis DB set to:", config.get("redis:db"));
            }
          );
        }

        if (config.get("log")) {
          var access_logfile = fs.createWriteStream(config.get("log"), {flags: 'a'});
        }

      };

  queue = async.queue(
    function (search, callback) {
      console.log('queue length:', queue.length());
      pool.getConnection(
        function(err, connection) {
          var now = parseInt(moment().tz("America/New_York").format("H"), 10),
              offset = "5",
              limit = config.get("limit") ? config.get("limit") : "10",
              typeOfSearch = (search.type === "paid") ? "IN" : "NOT IN",
              sql = "SELECT "+
                    " globalSearches.*, "+
                    " userSearches.restaurant, "+
                    " userSearches.secondary, "+
                    " restaurants.name, "+
                    " secondary.name AS secondaryName, "+
                    " userSearches.date, "+
                    " userSearches.partySize "+
                    "FROM globalSearches "+
                    "JOIN userSearches ON globalSearches.uid = userSearches.uid "+
                    "JOIN restaurants ON userSearches.restaurant = restaurants.id "+
                    "LEFT JOIN restaurants AS secondary ON userSearches.secondary = secondary.id "+
                    "WHERE userSearches.date >= UTC_TIMESTAMP() AND globalSearches.lastChecked < UTC_TIMESTAMP() - INTERVAL "+offset+" MINUTE "+
                    " AND userSearches.date <= UTC_TIMESTAMP() + INTERVAL 180 DAY "+
                    " AND userSearches.deleted = 0 AND userSearches.enabled = 1 AND globalSearches.deletedAt IS NULL " +
                    " AND userSearches.user " + typeOfSearch + " (SELECT id FROM `users` WHERE subExpires >= UTC_TIMESTAMP())",
              returnSearch = function(error, searches) {
                if (!error) { console.log(error); }
                if (!error) {
                  var i = 0,
                      finished = function() {
                        connection.release();
                        callback();
                      };
                  if (searches.length > 0) {
                    async.mapSeries(
                      searches,
                      function(search, cback) {
                        pubClient.lpush('uids', search.uid);
                        pubClient.ltrim('uids', 0, 2000);
                        cback(null, search.uid);
                      },
                      function(err, uids){
                        var message = underscore.extend(search, {searches: searches});
                        pubClient.publish("disneydining:requestsearch", JSON.stringify(message));
                        finished();

                      }
                    );
                  } else if (searches.length > 0) {
                    var message = underscore.extend(search, {searches: searches});
                    pubClient.publish("disneydining:requestsearch", JSON.stringify(message));
                    finished();
                  } else {
                    finished();
                  }
                }
              };
          pubClient.lrange(
            "uids",
            0,
            -1,
            function(err, uids){
              var finishSql = function() {
                sql += "GROUP BY globalSearches.uid ";
                sql += "ORDER BY globalSearches.lastChecked ASC ";
                sql += "LIMIT " + search.number.toString();
                connection.query(
                  sql,
                  function(error, searches) {
                    if (!error) {
                      returnSearch(null, searches);
                    } else {
                      returnSearch(error);
                    }
                  }
                );
              };

              if (uids) {
                async.map(
                  uids,
                  function(uid, cback) {
                    cback(null, "'"+uid+"'");
                  },
                  function(err, uids){
                    sql += " AND globalSearches.uid NOT IN ("+uids.join(",")+") ";
                    finishSql();
                  }
                );

              } else {
               finishSql();
              }

            }
          );
        }
      );
    },
    1
  );

  queue.drain = function() {
    console.log('all searches have been processed');
  };

  queue.saturated = function() {
    console.log('queue is saturated');
  };


  init();

}());
