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
      configFile, pool, queue, subCounter = 0,
      latestUids = new CBuffer(250),
      updateIpLog = function(message) {
        pool.getConnection(
          function(err, connection) {
            var sql = "UPDATE ipAddressLog SET success = ? WHERE id = ?";
            connection.query(
              sql,
              [message.success, message.id],
              function(error, update) {
                if (error) {
                  console.log(error);
                }
                connection.release();
              }
            );
          }
        );
      },
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
            //getSearch(message);
            queue.push(
              message,
              function (err) {
                console.log('finished processing search request');
              }
            );

          } else if (subChannel === "iplogupdate") {
            updateIpLog(message);
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

    refreshConfiguration();

    connectDB();

    latestUids = (config.get("uidBufferSize")) ? new CBuffer(config.get("uidBufferSize")) : new CBuffer(75);

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

  queue = async.queue(
    function (search, callback) {
      pool.getConnection(
        function(err, connection) {
          async.parallel(
            {
              searches: function(cb) {
                var now = parseInt(moment().tz("America/New_York").format("H"), 10),
                  offset = (now >= 3 && now < 6) ? "30" : "5",
                  limit = config.get("limit") ? config.get("limit") : "10",
                  typeOfSearch = (search.type === "paid") ? "IN" : "NOT IN",
                  sql = "SELECT "+
                        " globalSearches.*, "+
                        " userSearches.restaurant, "+
                        " restaurants.name, "+
                        " userSearches.date, "+
                        " userSearches.partySize "+
                        "FROM globalSearches "+
                        "JOIN userSearches ON globalSearches.uid = userSearches.uid "+
                        "JOIN restaurants ON userSearches.restaurant = restaurants.id "+
                        "WHERE userSearches.date >= UTC_TIMESTAMP() AND globalSearches.lastChecked < UTC_TIMESTAMP() - INTERVAL "+offset+" MINUTE "+
                        " AND userSearches.date <= UTC_TIMESTAMP() + INTERVAL 180 DAY "+
                        " AND userSearches.deleted = 0 AND userSearches.enabled = 1 AND globalSearches.deletedAt IS NULL " +
                        " AND userSearches.user " + typeOfSearch + " (SELECT id FROM `users` WHERE subExpires >= UTC_TIMESTAMP())";
                if (latestUids.toArray().length > 0) {
                  var uids = latestUids.toArray().map(function(uid){
                      // This will wrap each element of the uids array with quotes
                      return "'" + uid + "'";
                  }).join(",");
                  sql += " AND globalSearches.uid NOT IN ("+uids+") ";
                }
                sql += "GROUP BY globalSearches.uid ";
                sql += "ORDER BY globalSearches.lastChecked ASC ";
                sql += "LIMIT " + search.number.toString();
                if (subCounter > config.get("freeLimit")) {
                  console.log('Using free searches:', typeOfSearch, subCounter);
                  subCounter = 0;
                }
                subCounter += search.number;

                connection.query(
                  sql,
                  function(error, searches) {
                    if (!error) {
                      cb(null, searches);
                    } else {
                      cb(error);
                    }
                  }
                );
              },
              ips: function(cb) {
                var sql = "SELECT ipAddress.*, max.createdAt " +
                          "FROM ipAddress " +
                          "LEFT JOIN  ( " +
                          "              SELECT ipAddressId, " +
                          "                      MAX(createdAt) AS createdAt " +
                          "              FROM ipAddressLog " +
                          "              GROUP BY ipAddressId " +
                          "            ) max ON (max.ipAddressId = ipAddress.id) " +
                          "WHERE ipAddress.deletedAt IS NULL AND ipAddress.enabled = 1 " +
                          "ORDER BY max.createdAt ASC, ipAddress.id ASC " +
                          "LIMIT " + search.number.toString();
                connection.query(
                  sql,
                  function(error, ips) {
                    if (!error) {
                      cb(null, ips);
                    } else {
                      cb(error);
                    }
                  }
                );
              },
              agents: function(cb) {
                var sql = "SELECT * " +
                          "FROM userAgents, ( " +
                          "        SELECT id AS sid " +
                          "        FROM userAgents " +
                          "        ORDER BY RAND( ) " +
                          "        LIMIT ? " +
                          "    ) tmp " +
                          "WHERE userAgents.id = tmp.sid";
                connection.query(
                  sql,
                  [search.number],
                  function(error, agents) {
                    if (!error) {
                      cb(null, agents);
                    } else {
                      cb(error);
                    }
                  }
                );
              }
            },
            function(error, results) {
              if (!error) {
                var i = 0,
                    finished = function() {
                      connection.release();
                      callback();
                    };
                if (results.searches.length > 0) {
                  async.mapSeries(
                    results.searches,
                    function(search, cback) {
                      var sql = "INSERT INTO ipAddressLog (ipAddressId, createdAt, updatedAt) values(?, UTC_TIMESTAMP(), UTC_TIMESTAMP())";
                      search.ipAddress = {
                        ip: results.ips[i].ipAddress,
                        id: null
                      };
                      search.userAgent = results.agents[i].agent;
                      latestUids.push(search.uid);
                      connection.query(
                        sql,
                        [results.ips[i].id],
                        function(error, res) {
                          i++;
                          if (!error) {
                            search.ipAddress.id = res.insertId;
                            cback(null, search);
                          } else {
                            console.log(error);
                            cback(null, search);
                          }
                        }
                      );

                    },
                    function(err, searches){
                      var message = underscore.extend(search, {searches: searches});
                      pubClient.publish("disneydining:sendsearch", JSON.stringify(message));
                      finished();

                    }
                  );
                } else {
                  finished();
                }
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


  init();

}());
