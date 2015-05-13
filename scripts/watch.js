(function(){
  "use strict";

  var fs = require('fs'),
      redis = require("redis"),
      nconf = require('nconf'),
      path = require('path'),
      async = require('async'),
      moment = require('moment-timezone'),
      underscore = require('underscore'),
      supervisord = require('supervisord'),
      client = supervisord.connect(),
      numbers = [], models = {}, db = {},
      configFile, subClient, config, timeout,
      tenMinutes = 600000,
      setSubscription = function() {
        subClient.psubscribe("disneydining:*");
        console.log("Subscribing");
        subClient.on("pmessage", function (pattern, channel, message) {
          console.log("channel ", channel, ": ", message);
          message = JSON.parse(message);
          parseMessage(channel, message);
        });
      },
      parseMessage = function(channel, message) {
        var _channel = channel.split(":"),
            subChannel = _channel[1];
        if (subChannel === "crawler-ping") {
          clearTimeout(timeout);
          startTimeout();
        }
      },
      restartCrawler = function() {
        var service = 'disney-dining-crawler';
        async.waterfall(
          [
            function(callback) {
              client.stopProcess(
                service,
                function(err, result) {
                  if (err) console(err);
                  //console.log([result]);
                  callback(err);
                }
              );
            },
            function(results, callback) {
              client.startProcess(
                service,
                function(err, result) {
                  if (err) console(err);
                  //console.log(result);
                  results.push(result);
                  callback(err, results);
                }
              );
            },
          ],
          function (err, results) {
            if (err) console(err);
            console.log(results);
            startTimeout();
          }
        );
      },
      startTimeout = function() {
        timeout = setTimeout(
          function() {
            restartCrawler;
          },
          tenMinutes
        );
      },
      init = function() {
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

        if (config.get("log")) {
          var access_logfile = fs.createWriteStream(config.get("log"), {flags: 'a'});
        }

        client.getAllProcessInfo(
          function(err, result) {
            if (err) console(err);
            console.log(result);
          }
        );

        startTimeout();
      };

  init();

}());
