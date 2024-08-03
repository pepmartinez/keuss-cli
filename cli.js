#!/usr/bin/env node

const async =    require ('async');
const _ =        require ('lodash');
const rand_obj = require ('random-object');
const util =     require ('util');

const Ctx =  require ('./ctx');
const Gen =  require ('./gen');
const Push = require ('./push');
const Pop  = require ('./pop');

const { Command } = require('commander');
const program = new Command();

/*
/////////////////////////////////////////
function pop_loop (ctx, state, cb) {
  if (state.g.n == 0) return cb ();

  const next_n = (state.g.n == -1) ? state.g.n : (state.g.n ? state.g.n - 1 : state.g.n);
  state.g.n = next_n;

  ctx.q.pop ('keuss-cli', (err, res) => {
    if (err) {
      if (err == 'cancel') {
        logUpdate.done();
        out (ctx, chalk.yellow.bold(state.id), 'cancelled, stopping consumer');
        logUpdate.done();
        return;
      }

      return cb (err);
    }

    if (ctx.cmd_opts.dumpProduced) console.log ('%j', res);

    out (ctx, chalk.yellow.bold(state.id), `got (pop) element, ${chalk.bold(state.g.n + '')} to go`);

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
      if (err == 'cancel') {
        logUpdate.done();
        out (ctx, chalk.yellow.bold(state.id), 'cancelled, stopping consumer');
        logUpdate.done();
        return;
      }
    }

    ctx.q.ok (res, err => {
      if (err) return cb (err);
      if (ctx.cmd_opts.dumpProduced) console.log ('%j', res);

      out (ctx, chalk.yellow.bold(state.id), `got (reserve+commit) element, ${chalk.bold(state.g.n + '')} to go`);

      if (ctx.cmd_opts.delay) {
        setTimeout (() => rcr_loop (ctx, state, cb), ctx.cmd_opts.delay);
      }
      else {
        rcr_loop (ctx, state, cb);
      }
    });
  });
}
*/

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

  ctx.logUpdate().done();
  ctx.out ('farewell...');

  async.series (ctx._terminators, err => {
    if (err) console.error (err);
    ctx.logUpdate().done();
    ctx.out ('...and good night');
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


const ctx = new Ctx();


(async () => {
  await ctx.init();

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////
  program
  .version ('2.0.0')
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
    ctx.set_main_opts (program.opts());
    ctx.set_cmd_opts (this.opts());
    
    async.series ([
      cb => ctx.create_mq (cb),
      cb => ctx.factory().list ({full: ctx.cmd_opts().details}, cb),
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
    ctx.set_main_opts (program.opts());
    ctx.set_cmd_opts (this.opts());
    ctx.set_qname (queue);

    async.series ([
      cb => ctx.create_mq (cb),
      cb => ctx.select_q (cb),
      cb => ctx.q().info (cb),
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
    ctx.set_main_opts (program.opts());
    ctx.set_cmd_opts (this.opts());
    ctx.set_qname (queue);

    async.series ([
      cb => ctx.create_mq (cb),
      cb => ctx.select_q (cb),
      cb => {ctx.q().pause (true); cb(); },
      cb => setTimeout (cb, 100),
      cb => ctx.q().paused (cb)
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
    ctx.set_main_opts (program.opts());
    ctx.set_cmd_opts (this.opts());
    ctx.set_qname (queue);

    async.series ([
      cb => ctx.create_mq (cb),
      cb => ctx.select_q (cb),
      cb => {ctx.q().pause (false); cb(); },
      cb => setTimeout (cb, 100),
      cb => ctx.q().paused (cb)
    ], (err, res) => {
      if (err) console.error (err);
      else console.log (res[5]);
      farewell_and_good_night (ctx);
    });
  });


  //////////////////////////////////////////////
  program
  .command ('push')
  .description ('produces (pushes) to a queue, reading from stdin')
  .argument ('<queue>', 'queue to operate on')
  .action (function (queue) {
    ctx.set_main_opts (program.opts());
    ctx.set_cmd_opts (this.opts());
    ctx.set_qname (queue);

    const push = new Push (ctx);

    async.series ([
      cb => ctx.create_mq (cb),
      cb => ctx.select_q (cb),
      cb => push.run (cb),
    ], (err, res) => {
      if (err) console.error ('--- push cmd --- done', err);
      farewell_and_good_night (ctx);
    });
  });


  //////////////////////////////////////////////
  program
  .command ('pop')
  .description ('consumes (pops) from a queue, writing to stdout')
  .argument ('<queue>', 'queue to operate on')
  .option  ('-R, --reserve', 'doa reserve+commit instead of a pop')
  .option  ('-c, --count <n>', 'number of elements to produce, -1 for infinite', parseInt)
  .action (function (queue) {
    ctx.set_main_opts (program.opts());
    ctx.set_cmd_opts (this.opts());
    ctx.set_qname (queue);

    const pop = new Pop (ctx);

    async.series ([
      cb => ctx.create_mq (cb),
      cb => ctx.select_q (cb),
      cb => pop.run (cb),
    ], (err, res) => {
      if (err) console.error ('--- push cmd --- done', err);
      farewell_and_good_night (ctx);
    });
  });


  //////////////////////////////////////////////
  program
  .command ('gen')
  .description ('produces (pushes) fixed or random objects to a queue')
  .argument ('<queue>', 'queue to operate on')
  .option  ('-c, --count <n>', 'number of elements to produce, -1 for infinite', parseInt)
  .option  ('-p, --parallel <n>', 'number of producers', parseInt)
  .option  ('-d, --delay <ms>', 'delay at the end of each loop cycle, im millisecs', parseInt)
  .option  ('-D, --dump-produced', 'dump text of produced messages to stdout')
  .option  ('-J, --object <value>', 'dump text of produced messages to stdout', JSON.parse)
  .action (function (queue) {
    ctx.set_main_opts (program.opts());
    ctx.set_cmd_opts (this.opts());
    ctx.set_qname (queue);

    const gen = new Gen (ctx);

    async.series ([
      cb => ctx.create_mq (cb),
      cb => ctx.select_q (cb),
      cb => prepare_for_termination (ctx, cb),
      cb => gen.run (cb),
    ], err => {
      if (err) console.error (err);
      farewell_and_good_night (ctx);
    });
  });


  //////////////////////////////////////////////
  program.parse (process.argv);

})()