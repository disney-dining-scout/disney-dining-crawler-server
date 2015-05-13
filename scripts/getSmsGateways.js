var request = require('request'),
    cheerio = require('cheerio'),
    mysql = require('mysql'),
    Sequelize = require("sequelize"),
    nconf = require('nconf'),
    db = {}, models = {};

var getGateways = function() {
  request({
    uri: 'https://raw.githubusercontent.com/cubiclesoft/email_sms_mms_gateways/master/sms_mms_gateways.txt',
    method: "GET"
  }, function (error, response, body) {
    var gateways = JSON.parse(body),
        sql = "INSERT INTO smsGateways (id, country, name, gateway) "+
              "VALUES (:id, :country, :name, :gateway) "+
              "ON DUPLICATE KEY UPDATE "+
              "  id = :id, "+
              "  country = :country, "+
              "  name = :name, "+
              "  gateway = :gateway";
    //console.log(gateways.sms_carriers);
    for(var country in gateways.sms_carriers) {
      var carriers = gateways.sms_carriers[country];
      for(var ca in carriers) {
        var carrier = carriers[ca];
        //console.log(carrier);
        db.dining.query(
          sql,
          null,
          { raw: true },
          { id: ca,
            country: country,
            name: carrier[0],
            gateway: carrier[1]
          }
        ).success(function(results) {
          console.log(results);
        });
      };
      //var card = PEP.Finder.List.cards[id];
      /**
      db.dining.query(
        sql,
        null,
        { raw: true },
        { id: card.id,
          name: card.name
        }
      ).success(function(projects) {
        console.log(projects);
        //console.log("id:", card.id);
        //console.log("name:", card.name);
      });
      **/
    };
  });
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
      host: config.get("mysql:host") || "localhost",
      port: config.get("mysql:port") || 3306,
      pool: { maxConnections: 5, maxIdleTime: 30},
      define: {
        freezeTableName: true,
        timestamps: false
      }
});

models.SmsGateways = db.dining.define('smsGateways', {
  id:                   { type: Sequelize.STRING(100), primaryKey: true },
  country :             { type: Sequelize.STRING(255) },
  name :                { type: Sequelize.STRING(255) },
  gateway :             { type: Sequelize.STRING(255) }
});

getGateways();

