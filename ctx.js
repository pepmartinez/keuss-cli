const _ =     require ('lodash');
const async = require ('async');


/////////////////////////////////////////////////////////////////////////////////////////////////////
class Ctx {
  constructor () {
    this._ctx = {};
  }


  /////////////////////////////////////////////////////////////////////////////////////////////////////
  async init () {
    const createLogUpdate = (await import('log-update')).createLogUpdate;

    this._ctx.logUpdate = createLogUpdate(process.stderr, {});
    this._ctx.chalk =     (await import('chalk')).default;

    // list of terminator functions
    this._terminators = [];

    // add ourselves to it
    this._terminators.push (cb => this.end (cb));
  }


  set_main_opts (o) {this._ctx.main_opts = o}
  set_cmd_opts (o)  {this._ctx.cmd_opts = o}

  main_opts () {return this._ctx.main_opts || {}}
  cmd_opts ()  {return this._ctx.cmd_opts || {}}

  set_qname (q) {this._qname = q}
  qname (q)     {return this._qname}

  logUpdate () {return this._ctx.logUpdate}
  chalk ()     {return this._ctx.chalk}

  factory() {return this._ctx.factory}
  q() {return this._ctx.q}

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  out (...args) {
    if (this.main_opts().verbose) {
      this.logUpdate() ('-->', ...args);
    }
  }


  /////////////////////////////////////////////////////////////////////////////////////////////////////
  create_mq (cb) {
    const opts = this.main_opts();
    this.out (`MQ.init: using backend ${this.chalk().blue.bold(opts.backend || 'mongo')}`);
    this._ctx.logUpdate.done();

    const mq_opts = _.clone (opts.backendOpt || {});

    if (opts.signaller) {
      const signal_provider = require ('keuss/signal/' + opts.signaller);
      mq_opts.signaller = {
        provider: signal_provider
      };

      this.out (`MQ.init: using signaller ${chalk.blue.bold(opts.signaller)}`);
      this.logUpdate().done();
    }

    if (opts.stats) {
      const stats_provider = require ('keuss/stats/' + opts.stats);
      mq_opts.stats = {
        provider: stats_provider
      };

      this.out (`MQ.init: using stats ${this.chalk().blue.bold(opts.stats)}`);
      this.logUpdate().done();
    }

    const MQ = require ('keuss/backends/' + (opts.backend || 'mongo'));
    MQ (mq_opts, (err, factory) => {
      if (err) return cb (err);
      this.out ('MQ.init: keuss initiated');
      this.logUpdate().done();
      this._ctx.factory = factory;
      cb ();
    });
  }


  /////////////////////////////////////////
  select_q (cb) {
    this.factory().queue (this.qname() || 'test', this.main_opts().queueOpt, (err, q) => {
      this._ctx.q = q;
      cb (err);
    });
  }


  /////////////////////////////////////////
  end (cb) {
    const tasks = [];

    if (this.q()) {
      tasks.push (cb => this.q().drain (cb));
      tasks.push (cb => {this.q().cancel (); cb(); });
      tasks.push (cb => setTimeout (cb, 100));
    }
    
    if (this.factory()) {
      tasks.push (cb => {this.factory().close(); cb(); })
    }

    console.error ('--- ctx end --- start')

    async.series (tasks, err => {
      console.error ('--- ctx end --- done')
      cb(err);
    });
  }
}


module.exports = Ctx;

