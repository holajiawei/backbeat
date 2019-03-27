const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const { attachReqUids } = require('../../../lib/clients/utils');
const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { getAccountCredentials } =
          require('../../../lib/credentials/AccountCredentials');

class GarbageCollectorTask extends BackbeatTask {
    /**
     * Process a lifecycle object entry
     *
     * @constructor
     * @param {GarbageCollector} gc - garbage collector instance
     */
    constructor(gc) {
        super();
        const gcState = gc.getStateVars();
        Object.assign(this, gcState);

        this._setup();
    }

    _setup() {
        const accountCreds = getAccountCredentials(
            this.gcConfig.auth, this.logger);
        const s3 = this.s3Config;
        const transport = this.transport;
        this.logger.debug('creating backbeat client', { transport, s3 });
        this._backbeatClient = new BackbeatClient({
            endpoint: `${transport}://${s3.host}:${s3.port}`,
            credentials: accountCreds,
            sslEnabled: transport === 'https',
            httpOptions: { agent: this.httpAgent, timeout: 0 },
            maxRetries: 0,
        });
    }

    _executeDeleteData(entry, log, done) {
        log.debug('action execution starts', entry.getLogInfo());
        const locations = entry.getAttribute('target.locations');
        console.log('EXECUTING DELETE DATA');
        console.log('sourceObject', entry.getAttribute('sourceObject'));
        console.log('sourceObject.lastModified', entry.getAttribute('sourceObject.lastModified'));
        const req = this._backbeatClient.batchDelete({
            IfUnmodifiedSince: entry.getAttribute('sourceObject.lastModified'),
            Locations: locations.map(location => ({
                key: location.key,
                dataStoreName: location.dataStoreName,
                size: location.size,
                dataStoreVersionId: location.dataStoreVersionId,
            })),
        });
        attachReqUids(req, log);
        return req.send(err => {
            entry.setEnd(err);
            log.info('action execution ended', entry.getLogInfo());
            if (err) {
                log.error('an error occurred on deleteData method to ' +
                          'backbeat route',
                          Object.assign({
                              method: 'LifecycleObjectTask._executeDeleteData',
                              error: err.message,
                              httpStatus: err.statusCode,
                          }, entry.getLogInfo()));
                return done(err);
            }
            return done();
        });
    }

    /**
     * Execute the action specified in kafka queue entry
     *
     * @param {ActionQueueEntry} entry - kafka queue entry object
     * @param {String} entry.action - entry action name (e.g. 'deleteData')
     * @param {Object} entry.target - entry action target object
     * @param {Function} done - callback funtion
     * @return {undefined}
     */

    processActionEntry(entry, done) {
        const log = this.logger.newRequestLogger();

        if (entry.getActionType() === 'deleteData') {
            return this._executeDeleteData(entry, log, done);
        }
        log.warn('skipped unsupported action', entry.getLogInfo());
        return process.nextTick(done);
    }
}

module.exports = GarbageCollectorTask;
