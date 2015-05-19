var fs = require('fs'),
    async = require('async'),
    xml2js = require('xml2js');

var parser = new xml2js.Parser();
fs.readFile(__dirname + '/useragentswitcher.xml', function(err, data) {
    parser.parseString(
      data,
      function (err, result) {
        async.each(
          result.useragentswitcher.folder,
          function(folder, cback) {
            if ("useragent" in folder) {
              async.each(
                folder.useragent,
                function(agent, cb) {
                  console.log("INSERT INTO userAgents (agent, createdAt, updatedAt) values ('"+agent['$'].useragent+"', UTC_TIMESTAMP(), UTC_TIMESTAMP());");
                  cb(null);
                },
                function(err, res) {
                  cback(null);
                }
              );
            } else {
              cback(null);
            }
          },
          function(err, result) {
            console.log("done");
          }
        );

      }
    );
});
