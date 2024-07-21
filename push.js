
const async =    require ('async');
const _ =        require ('lodash');
const stream =   require ('stream');
const readline = require ('readline');


//////////////////////////////////////////////////////////////////////////////////
function toJson (opts) {
  if (!opts) opts = {};

  return new stream.Transform({
    objectMode: true,
    highWaterMark: opts.hwm || 3,
    transform(chunk, encoding, cb) {
      if (!chunk) return cb();
      let data;
      try {
        data = JSON.parse(chunk);
        this.push(data);
      } catch (err) {
        console.log('can not parse json', err.toString());
      }

      cb();
    }
  });
}


//////////////////////////////////////////////////////////////////////////////////
function pushToKeuss (agent, opts) {
  if (!opts) opts = {};

  return new stream.Writable({
    objectMode: true,
    highWaterMark: opts.hwm || 3,
    write(obj, encoding, cb) {
      agent._push (obj, cb);
    },
  });
}


//////////////////////////////////////////////////////////////////////////////////
class Push {
  constructor (ctx) {
    this._ctx = ctx;
    this._cnt = 0;

    this._parallel = 1;
    this._push_opts = {};

    process.on( 'SIGINT',  () => this.end ());
    process.on( 'SIGQUIT', () => this.end ());
    process.on( 'SIGTERM', () => this.end ());
    process.on( 'SIGHUP',  () => this.end ());
  }


  //////////////////////////////////////////////////////////////////////////////////
  run (cb) {
    this._tojson = toJson({hwm: 2});
    this._ptk = pushToKeuss(this, {hwm: 2});

    this._str = stream.pipeline (
      process.stdin,
      (input) => { 
        this._liner = readline.createInterface({ input }); 
        return this._liner; 
      },
      this._tojson,
      this._ptk,
      err => {
        console.log ('--- pipeline Push --- done', err)
        cb (err);
      }
    );

    // add ourselves to the beginning of the terminatin list
    this._ctx._terminators.unshift (cb => this.end(cb))
  }


  //////////////////////////////////////////////////////////////////////////////////
  end (cb) {
    // allow termination: disconnect from input
    console.log ('--- end Push --- start')

    this._liner.close();
    console.log ('--- end Push --- done')
    if (cb) cb();
  }


  //////////////////////////////////////////////////////////////////////////////////
  _push (obj, cb) {
    this._ctx.q().push (obj, this._push_opts, (err, res) => {
      if (err) {
        if (err == 'drain') {
          this._ctx.logUpdate().done();
          this._ctx.out ('queue in drain, stopping producer');
          this._ctx.logUpdate().done();
        }

        return cb (err);
      }

      this._cnt++;
      this._ctx.out (`${this._ctx.chalk().bold(this._cnt + '')} items pushed`);
      cb();
    });
  }
};


module.exports = Push;
