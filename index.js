var RSVP = require('rsvp');
var Promise = RSVP.Promise;
var fetch = require('fetch');
var fetchUrl = RSVP.denodeify(function(url, cb) {
  fetch.fetchUrl(url, function(err, meta, body) {
    return err ? cb(err) : cb(null, { meta: meta, body: body });
  })
});
var readFile = RSVP.denodeify(require('fs'));
var throat = require('throat')(Promise);
var _ = require('lodash');

function url(packageName) {
  return "https://pypi.python.org/pypi/" + packageName + "/json";
}

function Index(names) {
  this.names = names;
  this.httpErrors = Object.create(null);
  this.empties = [];
  this.errors = Object.create(null);
  this.ctimes = Object.create(null);
}

Index.fetch = function fetch() {
  return fetchUrl('https://pypi.python.org/simple/')
    .then(response => {
      var lines = response.body.toString().trim().split(/\n/).slice(1, -1);
      var names = lines.map(line => line.replace(/^.*href=\'/, "")
                                        .replace(/\'.*$/, ""));
      return new Index(names);
    });
};

Index.prototype.ctime = function ctime(name, opts) {
  if (typeof opts === 'undefined' || !opts) {
    opts = {};
  }

  if (typeof name === 'number') {
    name = this.names[name];
  }

  if (this.ctimes[name]) {
    return this.ctimes[name];
  }

  if (opts.progress) {
    console.log("fetching " + name + " ctime...");
  }

  var p = fetchUrl(url(name))
      .then(response => {
        if (response.meta.status !== 200) {
          this.httpErrors[name] = response.meta.status;
          return null;
        }
        var metadata = JSON.parse(response.body);
        var releases = metadata.releases;
        var versions = Object.keys(releases).map(version => {
          if (!releases[version] || !releases[version].length) {
            return null;
          }
          // Just pick the first file in the list. Even if it's not the earliest ctime it'll be close enough.
          return releases[version][0];
        }).filter(version => version);
        var timestamps = versions.map(function(file) {
          return new Date(file.upload_time);
        });
        if (!timestamps.length) {
          this.empties.push(name);
          return null;
        }
        return timestamps.sort(function(a, b) { return a - b })[0];
      })
      .catch(err => {
        this.errors[name] = err;
        this.ctimes[name] = null;
        throw err;
      });
  this.ctimes[name] = p;
  return p;
};

var CONCURRENCY = 16;

Index.prototype.allCTimes = function allCTimes() {
  return Promise.all(this.names.map(throat(CONCURRENCY, name => {
    return this.ctime(name, { progress: true })
               .catch(e => {
                 console.log("error (" + name + "): " + e);
                 return null;
               });
  })));
};

Index.prototype.retry = function retry() {
  var retries = this.names.filter(name => !this.ctimes[name]);
  return Promise.all(retries.map(throat(CONCURRENCY, name => {
    return this.ctime(name, { progress: true })
               .catch(e => {
                 console.log("error (" + name + "): " + e);
                 return null;
               });
  })));
};

Index.prototype.report = function report() {
  return Promise.all(this.names.map(name => Promise.cast(this.ctimes[name])
                                                   .then(ctime => ctime ? { name: name, date: ctime.toLocaleDateString() } : null)))
                .then(records => {
                  var state = _.toPairs(_.groupBy(records.filter(x=>x), 'date'))
                      .sort((a, b) => (new Date(a[0])) - (new Date(b[0])))
                      .reduce((state, pairs) => {
                        var date = pairs[0];
                        var packages = pairs[1];
                        state.total += packages.length;
                        state.result[date] = {
                          new: packages.length,
                          total: state.total
                        };
                        return state;
                      }, {
                        total: 0,
                        result: Object.create(null)
                      });
                  return state.result;
                });
};

module.exports = Index;
