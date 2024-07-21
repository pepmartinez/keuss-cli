const Chance =   require ('chance');
const rand_obj = require ('random-object');
const async =    require ('async');
const _ =        require ('lodash');

const chance = new Chance();


//////////////////////////////////////////////////////////////////////////////////
class Gen {
  constructor (ctx) {
    this._ctx = ctx;
    this._pool_of_objs = [];
    this._cnt = 0;

    this._elems =    ctx.cmd_opts().count || -1;
    this._parallel = ctx.cmd_opts().parallel || 1;
    this._delay =    ctx.cmd_opts().delay || 0;
    this._dump =     ctx.cmd_opts().dumpProduced || false;

    if (ctx.cmd_opts().object) {
      this._pool_of_objs.push (ctx.cmd_opts().object);
    }
    else {
      // prepare pool of random objects
      for (let i = 0; i < 111; i++) this._pool_of_objs.push (rand_obj.randomObject ());
    }
  }


  //////////////////////////////////////////////////////////////////////////////////
  run (cb) {
    const loops = [];

    for (let c = 0; c < (this._parallel); c++) {
      loops.push (cb => {
        this._ctx.out (`push: initiating gen loop #${c}`);
        this._ctx.logUpdate().done();
        this._push_loop (cb);
      });
    }

    async.parallel (loops, cb);
  }


  //////////////////////////////////////////////////////////////////////////////////
  _push_loop (cb) {
    if (this._elems == 0) return cb ();

    const opts = {};
    const obj = this._pool_of_objs [chance.integer ({min:0, max:_.size (this._pool_of_objs)})];

    if (this._elems > 0) this._elems--;

    this._ctx.q().push (obj, opts, (err, res) => {
      if (err) {
        if (err == 'drain') {
          this._ctx.logUpdate().done();
          this._ctx.out ('queue in drain, stopping producer');
          this._ctx.logUpdate().done();
          return;
        }

        return cb (err);
      }

      this._cnt++;

      if (this._dump) console.log ('%j', obj);

      if (this._elems == -1) {
        this._ctx.out (`${this._ctx.chalk().bold(this._cnt + '')} items pushed`);
      }
      else {
        this._ctx.out (`${this._ctx.chalk().bold(this._cnt + '')} items pushed, ${this._ctx.chalk().bold(this._elems + '')} to go`);
      }

      if (this._delay) {
        setTimeout (() => this._push_loop (cb), this._delay);
      }
      else {
        this._push_loop (cb);
      }
    });
  }
};


module.exports = Gen;
