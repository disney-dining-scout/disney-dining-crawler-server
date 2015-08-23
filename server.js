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
      latestUids = new CBuffer(350),
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
          console.log("channel ", channel, ": ", message);
          message = JSON.parse(message);
          if (subChannel === "requestsearch") {
            //getSearch(message);
            processSearch(message);
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

      },init = function() {
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
        if (config.get("redis:db") >= 0) {
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

  var processSearch = function (request) {
    pool.getConnection(
      function(err, connection) {
        async.parallel(
          {
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
                        "LIMIT " + request.number.toString();
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
                [request.number],
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
            console.log(error);
            if (!error) {
              var i = 0,
                  finished = function() {
                    connection.release();
                    console.log('finished processing search request', request.clientId);
                  };
              if (request.searches.length > 0 && results.ips.length > 0) {
                async.mapSeries(
                  request.searches,
                  function(search, cback) {
                    var sql = "INSERT INTO ipAddressLog (ipAddressId, createdAt, updatedAt) values(?, UTC_TIMESTAMP(), UTC_TIMESTAMP())";
                    search.ipAddress = {
                      ip: results.ips[i].ipAddress,
                      id: null
                    };
                    search.userAgent = results.agents[i].agent;
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
                    var message = underscore.extend(request, {searches: searches});
                    pubClient.publish("disneydining:sendsearch", JSON.stringify(message));
                    finished();

                  }
                );
              } else if (request.searches.length > 0) {
                pubClient.publish("disneydining:sendsearch", JSON.stringify(request));
                finished();
              } else {
                finished();
              }
            }
          }
        );
      }
    );
  };

  init();

}());
