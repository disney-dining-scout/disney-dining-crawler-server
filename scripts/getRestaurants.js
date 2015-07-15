var request = require('request'),
    cheerio = require('cheerio'),
    mysql = require('mysql'),
    async = require('async'),
    Sequelize = require("sequelize"),
    nconf = require('nconf'),
    redis = require("redis"),
    db = {}, models = {};

var getRestaurants = function() {
  request(
    {
      uri: 'https://disneyworld.disney.go.com/dining/map/',
      method: "GET",
      proxy: "http://37.58.52.41:2020"
    },
    function (error, response, body) {
      console.log("Got page!");
      var $ = cheerio.load(body),
          js = $('#finderBlob').text(),
          PEP = PEP || {};
      eval(js);
      async.each(
        Object.keys(PEP.Finder.List.cards),
        function(id, callback) {
          var card = PEP.Finder.List.cards[id];
          console.log(card.name);
          models.Restaurants
            .findOrCreate(
              {
                where: {
                  id: card.id
                },
                defaults: {
                  id: card.id,
                  name: card.name
                }
              }
            )
            .spread(
              function(user, created) {
                //console.log(user);
                //console.log(created)
                callback(null);
              }
          );
        },
        function(error) {
          pubClient.publish("disneydining:restaurantupdate", JSON.stringify({status: "updated"}));
          console.log("done!");
        }
      );
    }
  );
};

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

db.dining = new Sequelize(
  config.get("mysql:database"),
  config.get("mysql:username"),
  config.get("mysql:password"),
  {
      dialect: 'mysql',
      omitNull: true,
      logging: true,
      host: config.get("mysql:host") || "localhost",
      port: config.get("mysql:port") || 3306,
      pool: { maxConnections: 5, maxIdleTime: 30},
      define: {
        freezeTableName: true
      }
});

models.Restaurants = db.dining.define('restaurants', {
  id:                   { type: Sequelize.STRING(255), primaryKey: true },
  name :                { type: Sequelize.STRING(255) }
});

pubClient = redis.createClient(
  config.get("redis:port"),
  config.get("redis:host")
);
if (config.get("redis:db") && config.get("redis:db") > 0) {
  pubClient.select(
    config.get("redis:db"),
    function() {
      //console.log("Redis DB set to:", config.get("redis:db"));
    }
  );
}

getRestaurants();

