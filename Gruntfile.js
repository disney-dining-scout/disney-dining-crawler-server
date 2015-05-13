module.exports = function(grunt) {
  // Load Grunt tasks declared in the package.json file
  require('matchdep').filterDev('grunt-*').forEach(grunt.loadNpmTasks);
  // Project configuration.
  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),
    bower: {
      install: {
        options: {
          targetDir: './lib',
          layout: 'byType',
          install: true,
          verbose: false,
          cleanTargetDir: true,
          cleanBowerDir: false
        }
      }
    },
    jshint: {
      server: ['server.js']
    },
    watch: {
      grunt: {
        files: ['Gruntfile.js'],
        tasks: ['build', 'express:dev', 'watch'],
        options: {
          spawn: true,
        },
      },
      server: {
        files: ['server.js'],
        tasks: ['jshint:server'],
        options: {
          nospawn: true //Without this option specified express won't be reloaded
        }
      }
    },
    nodemon: {
      dev: {
        script: 'server.js',
        options: {
          nodeArgs: ['--debug=5890']
        }
      }
    },
    'node-inspector': {
      default: {}
    },
    concurrent: {
      options: {
        limit: 3,
        logConcurrentOutput: true
      },
      dev: {
        tasks: ["nodemon:dev", "watch"]
      }
    }
  });

  grunt.registerTask('build', [
    'jshint:server',
  ]);

  grunt.event.on('watch', function(action, filepath, target) {
    grunt.log.writeln(target + ': ' + filepath + ' has ' + action);
  });

  grunt.registerTask('server', [ 'build', 'concurrent:dev' ]);

  // Default task(s).
  grunt.registerTask('default', ['build']);

};
