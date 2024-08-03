
const async =    require ('async');
const _ =        require ('lodash');
const stream =   require ('stream');


//////////////////////////////////////////////////////////////////////////////////
function toStr (opts) {
  if (!opts) opts = {};

  return new stream.Transform({
    objectMode: true,
    highWaterMark: opts.hwm || 4096,
    transform(chunk, encoding, cb) {
      if (!chunk) return cb();
      let data;
      try {
        data = JSON.stringify(chunk);
        this.push(data);
      } catch (err) {
        console.error('can not convert to json', err.toString());
      }

      cb();
    }
  });
}


//////////////////////////////////////////////////////////////////////////////////
function popFromKeuss (agent, opts) {
  if (!opts) opts = {};

  return new stream.Readable({
    objectMode: true,
    highWaterMark: opts.hwm || 3,
    read (size) {
      agent._pop ((err, obj) => {
        if (err) {
          // break in error?
          console.error('error in popFromKeuss:', err.toString());
          return this.push (null);
        }

        this.push (obj);
      });
    },
  });
}


//////////////////////////////////////////////////////////////////////////////////
class Pop {
  constructor (ctx) {
    this._ctx = ctx;
    this._cnt = 0;

    this._inflight = 0;

    this._parallel = 1;
    this._elems =    ctx.cmd_opts().count || -1;

    process.on( 'SIGINT',  () => this.end ());
    process.on( 'SIGQUIT', () => this.end ());
    process.on( 'SIGTERM', () => this.end ());
    process.on( 'SIGHUP',  () => this.end ());
  }


  //////////////////////////////////////////////////////////////////////////////////
  run (cb) {
    this._toStr = toStr ({});
    this._pfk = popFromKeuss(this, {});

    this._str = stream.pipeline (
      this._pfk,
      this._toStr,
      process.stdout,
      err => {
        console.error ('--- pipeline Pop --- done', err)
        cb (err);
      }
    );

    // add ourselves to the beginning of the terminatin list
    this._ctx._terminators.unshift (cb => this.end(cb))
  }


  //////////////////////////////////////////////////////////////////////////////////
  end (cb) {
    // allow termination: disconnect from input
    console.error ('--- end Pop --- start')
    this._ctx.q ().cancel();
    console.error ('--- end Pop --- done')
    if (cb) cb();
  }


  //////////////////////////////////////////////////////////////////////////////////
  _pop (cb) {
    if (this._elems == 0) return cb (null, null);
    if (this._elems > 0) this._elems--;
    
    this._inflight++
    console.error (`++ we are ${this._inflight} in flight`)

    this._ctx.q ().pop ('keuss-cli', {reserve: false}, (err, res) => {
      this._inflight--
      console.error (`-- we are ${this._inflight} in flight`)
    
      if (err) {
        if (err == 'cancel') {
          this._ctx.logUpdate().done();
          this._ctx.out ('canceled, stopping consumer');
          this._ctx.logUpdate().done();
          return cb(null, null);  // pass null for EOF
        }

        return cb (err);
      }

      this._cnt++;
      this._ctx.out (`${this._ctx.chalk().bold(this._cnt + '')} items popped`);
      cb (null, res);
    });
  }
};


module.exports = Pop;
