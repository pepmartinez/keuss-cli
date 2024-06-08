#!/usr/bin/env node

const async =    require ('async');
const program =  require ('commander');
const _ =        require ('lodash');
const rand_obj = require ('random-object');
const Chance =   require('chance');
const util =     require ('util');

const chance = new Chance();


/////////////////////////////////////////
function out (ctx, ...args) {
  if (ctx.main_opts.verbose) console.log ('-->', ...args);
}


/////////////////////////////////////////
function create_mq (ctx, cb) {
  const opts = ctx.main_opts;
  out (ctx, `MQ.init: using backend ${opts.backend || 'mongo'}`);

  const mq_opts = _.clone (opts.backendOpt || {});

  if (opts.signaller) {
    const signal_provider = require ('keuss/signal/' + opts.signaller);
    mq_opts.signaller = {
      provider: signal_provider
    };

    out (ctx, `MQ.init: using signeller ${opts.signaller}`);
  }

  if (opts.stats) {
    const stats_provider = require ('keuss/stats/' + opts.stats);
    mq_opts.stats = {
      provider: stats_provider
    };

    out (ctx, `MQ.init: using stats ${opts.stats}`);
  }

  out (ctx, 'MQ.init: use opts', mq_opts);

  const MQ = require ('keuss/backends/' + (opts.backend || 'mongo'));
  MQ (mq_opts, (err, factory) => {
    if (err) return cb (err);
    out (ctx, 'MQ.init: backend initiated');
    ctx.factory = factory;
    cb ();
  });
}


/////////////////////////////////////////
function select_q (ctx, cb) {
  ctx.q = ctx.factory.queue (ctx.qname || 'test', ctx.main_opts.queueOpt);
  ctx.q.init (cb);
}


/////////////////////////////////////////
function pop_loop (ctx, state, cb) {
  if (state.g.n == 0) return cb ();

  const next_n = (state.g.n == -1) ? state.g.n : (state.g.n ? state.g.n - 1 : state.g.n);
  state.g.n = next_n;

  ctx.q.pop ('keuss-cli', (err, res) => {
    if (err) {
      if (err == 'cancel') return out (ctx, state.id, 'cancelled, stopping consumer');
      return cb (err);
    }

    if (ctx.cmd_opts.dumpProduced) console.log ('%j', res);

    out (ctx, state.id, `got (pop) element, ${state.g.n} to go`);

    if (ctx.cmd_opts.delay) {
      setTimeout (() => pop_loop (ctx, state, cb), ctx.cmd_opts.delay);
    }
    else {
      pop_loop (ctx, state, cb);
    }
  });
}


/////////////////////////////////////////
function rcr_loop (ctx, state, cb) {
  if (state.g.n == 0) return cb ();

  const next_n = (state.g.n == -1) ? state.g.n : (state.g.n ? state.g.n - 1 : state.g.n);
  state.g.n = next_n;

  ctx.q.pop ('keuss-cli', {reserve: true}, (err, res) => {
    if (err) {
      if (err == 'cancel') return out (ctx, state.id, 'cancelled, stopping consumer');
      return cb (err);
    }

    ctx.q.ok (res, err => {
      if (err) return cb (err);
      if (ctx.cmd_opts.dumpProduced) console.log ('%j', res);

      out (ctx, state.id, `got (reserve+commit) element, ${state.g.n} to go`);

      if (ctx.cmd_opts.delay) {
        setTimeout (() => rcr_loop (ctx, state, cb), ctx.cmd_opts.delay);
      }
      else {
        rcr_loop (ctx, state, cb);
      }
    });
  });
}


/////////////////////////////////////////
function push_loop (ctx, state, cb) {
  if (state.g.n == 0) return cb ();

  const opts = {};

  const obj = state.obj || state.pool_of_objs[chance.integer ({min:0, max:110})];

  const next_n = (state.g.n == -1) ? state.g.n : (state.g.n ? state.g.n - 1 : state.g.n);
  state.g.n = next_n;

  ctx.q.push (obj, opts, (err, res) => {
    if (err) {
      if (err == 'drain') return out (ctx, state.id, 'queue in drain, stopping producer');
      return cb (err);
    }

    if (ctx.cmd_opts.dumpProduced) console.log ('%j', obj);
    out (ctx, state.id, `pushed element, ${state.g.n} to go`);

    if (ctx.cmd_opts.delay) {
      setTimeout (() => push_loop (ctx, state, cb), ctx.cmd_opts.delay);
    }
    else {
      push_loop (ctx, state, cb);
    }
  });
}


/////////////////////////////////////////
function parseXOpts (value, prev) {
  const arr = value.split (':');
  if (arr.length < 2) throw new program.InvalidArgumentError('value must be k:v');
  const k = arr.shift();
  const v = arr.join (':');

  if (!prev) prev = {};
  prev[k] = v;
  return prev;
}


/////////////////////////////////////////
function farewell_and_good_night (ctx) {
  if (ctx.in_terminus) return;
  ctx.in_terminus = true;

  out (ctx, 'farewell...');

  async.series ([
    cb => {
      if (ctx.q) {
        ctx.q.drain (cb);
      }
      else {
        cb();
      }
    },
    cb => {if (ctx.q) ctx.q.cancel (); cb ();},
    cb => setTimeout (cb, 100),
    cb => {if (ctx.factory) ctx.factory.close(); cb ();},
  ], err => {
    if (err) console.error (err);
    out (ctx, '...and good night');
    ctx.in_terminus = false;
  });
}


/////////////////////////////////////////
function prepare_for_termination (ctx, cb) {
  process.on( 'SIGINT',  () => farewell_and_good_night (ctx));
  process.on( 'SIGQUIT', () => farewell_and_good_night (ctx));
  process.on( 'SIGTERM', () => farewell_and_good_night (ctx));
  process.on( 'SIGHUP',  () => farewell_and_good_night (ctx));
  cb ();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
program
.version ('1.2.0')
.usage   ('[options]')
.option  ('-v, --verbose', 'be verbose')
.option  ('-O, --queue-opt <opt>', 'option to b added to queue, format k:v', parseXOpts)
.option  ('-b, --backend <value>', 'use queue backend. defaults to \'mongo\'')
.option  ('-o, --backend-opt <value>', 'option to b added to backend, format k:v', parseXOpts)
.option  ('-s, --signaller <value>', 'use signaller backend')
.option  ('-t, --stats <value>', 'use stats backend');

//////////////////////////////////////////////
program
.command ('list')
.description ('list queues inside backend')
.option  ('-d, --details', 'show stats-provided details')
.option  ('-i, --info', 'run an info command on each queue')
.action (function () {
  const ctx = {main_opts: program.opts(), cmd_opts: this.opts()};
  out (ctx, 'ctx is', ctx);

  async.series ([
    cb => create_mq (ctx, cb),
    cb => prepare_for_termination (ctx, cb),
    cb => ctx.factory.list ({full: ctx.cmd_opts.details}, cb),
  ], (err, res) => {
    if (err) console.error (err);
    else console.log (util.inspect(res[2], {depth: null, colors: true}));
    farewell_and_good_night (ctx);
  });
});

//////////////////////////////////////////////
program
.command ('info')
.description ('show info and stats for a queue')
.argument ('<queue>', 'queue to operate on')
.action (function (queue) {
  const ctx = {qname: queue, main_opts: program.opts(), cmd_opts: this.opts()};
  out (ctx, 'ctx is', ctx);

  async.series ([
    cb => create_mq (ctx, cb),
    cb => select_q (ctx, cb),
    cb => prepare_for_termination (ctx, cb),
    cb => ctx.q.info (cb),
  ], (err, res) => {
    if (err) console.error (err);
    else console.log (util.inspect(res[3], {depth: null, colors: true}));
    farewell_and_good_night (ctx);
  });
});

//////////////////////////////////////////////
program
.command ('pause')
.description ('pauses a queue')
.argument ('<queue>', 'queue to operate on')
.action (function (queue) {
  const ctx = {qname: queue, main_opts: program.opts(), cmd_opts: this.opts()};
  out (ctx, 'ctx is', ctx);

  async.series ([
    cb => create_mq (ctx, cb),
    cb => select_q (ctx, cb),
    cb => prepare_for_termination (ctx, cb),
    cb => {ctx.q.pause (true); cb(); },
    cb => setTimeout (cb, 100),
    cb => ctx.q.paused (cb)
  ], (err, res) => {
    if (err) console.error (err);
    else console.log (res[5]);
    farewell_and_good_night (ctx);
  });
});

//////////////////////////////////////////////
program
.command ('resume')
.description ('resumes a queue')
.argument ('<queue>', 'queue to operate on')
.action (function (queue) {
  const ctx = {qname: queue, main_opts: program.opts(), cmd_opts: this.opts()};
  out (ctx, 'ctx is', ctx);

  async.series ([
    cb => create_mq (ctx, cb),
    cb => select_q (ctx, cb),
    cb => prepare_for_termination (ctx, cb),
    cb => {ctx.q.pause (false); cb(); },
    cb => setTimeout (cb, 100),
    cb => ctx.q.paused (cb)
  ], (err, res) => {
    if (err) console.error (err);
    else console.log (res[5]);
    farewell_and_good_night (ctx);
  });
});

//////////////////////////////////////////////
program
.command ('pop')
.description ('consumes (pops) from a queue')
.argument ('<queue>', 'queue to operate on')
.option  ('-c, --count <n>', 'number of elements to consume, -1 for infinite', parseInt)
.option  ('-p, --parallel <n>', 'number of consumers', parseInt)
.option  ('-d, --delay <ms>', 'delay at the end of each loop cycle, im millisecs', parseInt)
.option  ('-D, --dump-produced', 'dump text of produced messages to stdout')
.option  ('-R, --reserve', 'doa reserve+commit instead of a pop')
.action (function (queue) {
  const ctx = {qname: queue, main_opts: program.opts(), cmd_opts: this.opts()};
  out (ctx, 'ctx is', ctx);

  const global_state = {
    n: ctx.cmd_opts.count || -1
  };

  const loops = [];
  for (let c = 0; c < (ctx.cmd_opts.parallel || 1); c++) {
    loops.push (cb => {
      const state = {
        g: global_state,
        id: `loop#${c}`,
      };

      out (ctx, `pop: initiating pop loop #${c}`);
      if (ctx.cmd_opts.reserve) {
        rcr_loop (ctx, state, cb);
      }
      else {
        pop_loop (ctx, state, cb);
      }
    });
  }

  async.series ([
    cb => create_mq (ctx, cb),
    cb => select_q (ctx, cb),
    cb => prepare_for_termination (ctx, cb),
    cb => async.parallel (loops, cb),
  ], (err, res) => {
    if (err) console.error (err);
    farewell_and_good_night (ctx);
  });
});

//////////////////////////////////////////////
program
.command ('push')
.description ('produces (pushes) to a queue')
.argument ('<queue>', 'queue to operate on')
.option  ('-c, --count <n>', 'number of elements to produce, -1 for infinite', parseInt)
.option  ('-p, --parallel <n>', 'number of producers', parseInt)
.option  ('-d, --delay <ms>', 'delay at the end of each loop cycle, im millisecs', parseInt)
.option  ('-D, --dump-produced', 'dump text of produced messages to stdout')
.option  ('-J, --object <value>', 'dump text of produced messages to stdout', JSON.parse)
.action (function (queue) {
  const ctx = {qname: queue, main_opts: program.opts(), cmd_opts: this.opts()};
  out (ctx, 'ctx is', ctx);

  const global_state = {
    n: ctx.cmd_opts.count || -1
  };

  const loops = [];
  for (let c = 0; c < (ctx.cmd_opts.parallel || 1); c++) {
    loops.push (cb => {
      const state = {
        g: global_state,
        id: `loop#${c}`,
      };

      if (ctx.cmd_opts.object) {
        state.obj = ctx.cmd_opts.object;
      }
      else {
        // prepare pool of random objects
        state.pool_of_objs = [];
        for (let i = 0; i < 111; i++) state.pool_of_objs.push (rand_obj.randomObject ());
      }

      out (ctx, `pop: initiating push loop #${c}`);
      push_loop (ctx, state, cb);
    });
  }

  async.series ([
    cb => create_mq (ctx, cb),
    cb => select_q (ctx, cb),
    cb => prepare_for_termination (ctx, cb),
    cb => async.parallel (loops, cb),
  ], (err, res) => {
    if (err) console.error (err);
    farewell_and_good_night (ctx);
  });
});

//////////////////////////////////////////////
program.parse (process.argv);
