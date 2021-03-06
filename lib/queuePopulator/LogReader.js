const async = require('async');

const jsutil = require('arsenal').jsutil;

const config = require('../../conf/Config');
const BackbeatProducer = require('../BackbeatProducer');
const ReplicationQueuePopulator =
    require('../../extensions/replication/ReplicationQueuePopulator');

const { metricsExtension, metricsTypeQueued } =
    require('../../extensions/replication/constants');

class LogReader {

    /**
     * Create a log reader object to populate kafka topics from a
     * single log source (e.g. one raft session)
     *
     * @constructor
     * @param {Object} params - constructor params
     * @param {Object} params.zkClient - zookeeper client object
     * @param {Object} params.kafkaConfig - kafka configuration object
     * @param {Object} [params.logConsumer] - log consumer object
     * @param {function} params.logConsumer.readRecords - function
     *   that fetches records from the log, called by LogReader
     * @param {String} params.logId - log source unique identifier
     * @param {Logger} params.logger - logger object
     * @param {QueuePopulatorExtension[]} params.extensions - array of
     *   queue populator extension modules
     * @param {MetricsProducer} params.metricsProducer - instance of metrics
     *   producer
     */
    constructor(params) {
        this.zkClient = params.zkClient;
        this.kafkaConfig = params.kafkaConfig;
        this.logConsumer = params.logConsumer;
        this.pathToLogOffset = `${config.queuePopulator.zookeeperPath}/` +
            `logState/${params.logId}/logOffset`;
        this.logOffset = null;
        this.log = params.logger;
        this._producers = {};
        this._extensions = params.extensions;
        this._mProducer = params.metricsProducer;

        // internal variable to carry over a tailable cursor across batches
        this._openLog = null;
    }

    _setEntryBatch(entryBatch) {
        this._extensions.forEach(ext => ext.setBatch(entryBatch));
    }

    _unsetEntryBatch() {
        this._extensions.forEach(ext => ext.unsetBatch());
    }

    setLogConsumer(logConsumer) {
        this.logConsumer = logConsumer;
    }

    /**
     * prepare the log reader before starting to populate entries
     *
     * This function may be overriden by subclasses, if source log
     * initialization is needed, in which case they must still call
     * LogReader.setup().
     *
     * @param {function} done - callback function
     * @return {undefined}
     */
    setup(done) {
        this.log.debug('setting up log source',
            { method: 'LogReader.setup',
                logSource: this.getLogInfo() });
        this._readLogOffset((err, offset) => {
            if (err) {
                this.log.error('log source setup failed',
                    { method: 'LogReader.setup',
                        logSource: this.getLogInfo() });
                return done(err);
            }
            this.logOffset = offset;
            this.log.info('log reader is ready to populate backbeat queue(s)',
                { logSource: this.getLogInfo(),
                    logOffset: this.logOffset });
            return done();
        });
    }

    /**
     * get the next offset to fetch from source log (aka. log sequence
     * number)
     *
     * @return {string} the next log offset to fetch
     */
    getLogOffset() {
        return this.logOffset;
    }

    _readLogOffset(done) {
        const pathToLogOffset = this.pathToLogOffset;
        this.zkClient.getData(pathToLogOffset, (err, data) => {
            if (err) {
                if (err.name !== 'NO_NODE') {
                    this.log.error(
                        'Could not fetch log offset',
                        { method: 'LogReader._readLogOffset',
                            error: err });
                    return done(err);
                }
                return this.zkClient.mkdirp(pathToLogOffset, err => {
                    if (err) {
                        this.log.error(
                            'Could not pre-create path in zookeeper',
                            { method: 'LogReader._readLogOffset',
                                zkPath: pathToLogOffset,
                                error: err });
                        return done(err);
                    }
                    return done(null, 1);
                });
            }
            if (data) {
                let logOffset;
                if (config.queuePopulator.logSource === 'mongo') {
                    logOffset = data.toString();
                } else {
                    const logOffsetNumber = Number.parseInt(data, 10);
                    if (Number.isNaN(logOffsetNumber)) {
                        this.log.error(
                            'invalid stored log offset',
                            { method: 'LogReader._readLogOffset',
                                zkPath: pathToLogOffset,
                                logOffset: data.toString() });
                        return done(null, 1);
                    }
                    logOffset = logOffsetNumber.toString();
                }
                this.log.debug(
                    'fetched current log offset successfully',
                    { method: 'LogReader._readLogOffset',
                        zkPath: pathToLogOffset,
                        logOffset });
                return done(null, logOffset);
            }
            return done(null, 1);
        });
    }

    _writeLogOffset(logger, done) {
        const pathToLogOffset = this.pathToLogOffset;
        const offsetString = this.logOffset.toString();
        this.zkClient.setData(
            pathToLogOffset,
            Buffer.from(offsetString), -1,
            err => {
                if (err) {
                    logger.error(
                        'error saving log offset',
                        { method: 'LogReader._writeLogOffset',
                            zkPath: pathToLogOffset,
                            logOffset: this.logOffset });
                    return done(err);
                }
                logger.debug(
                    'saved log offset',
                    { method: 'LogReader._writeLogOffset',
                        zkPath: pathToLogOffset,
                        logOffset: this.logOffset });
                return done();
            });
    }

    /**
     * Process log entries, up to the maximum defined in params or
     * until the timeout is reached waiting for new entries to come
     * (if tailable oplog)
     *
     * @param {Object} [params] - parameters object
     * @param {Number} [params.maxRead] - max number of records to process
     *   from the log. Records may contain multiple entries and all entries
     *   are not queued, so the number of queued entries is not directly
     *   related to this number.
     * @param {Number} [params.timeoutMs] - timeout after which the
     *   batch will be stopped if still running (default 10000ms)
     * @param {function} done - callback when done processing the
     *   entries
     * @return {undefined}
     */
    processLogEntries(params, done) {
        const batchState = {
            logRes: null,
            logStats: {
                nbLogRecordsRead: 0,
                nbLogEntriesRead: 0,
            },
            entriesToPublish: {},
            publishedEntries: {},
            maxRead: params.maxRead,
            startTime: Date.now(),
            timeoutMs: params.timeoutMs,
            logger: this.log.newRequestLogger(),
        };

        async.waterfall([
            next => this._processReadRecords(params, batchState, next),
            next => this._processPrepareEntries(batchState, next),
            next => this._processPublishEntries(batchState, next),
            next => this._processSaveLogOffset(batchState, next),
        ],
            err => {
                if (err) {
                    return done(err);
                }
                const queuedEntries = {};
                const { publishedEntries } = batchState;
                Object.keys(publishedEntries).forEach(topic => {
                    queuedEntries[topic] = publishedEntries[topic].length;
                });
                const stats = {
                    readRecords: batchState.logStats.nbLogRecordsRead,
                    readEntries: batchState.logStats.nbLogEntriesRead,
                    skippedRecords: batchState.logStats.skippedRecords,
                    queuedEntries,
                };
                // Use heuristics to log when:
                // - at least one entry is pushed to any topic
                // - many records have been skipped (e.g. when starting up
                //   consumption from mongo oplog)
                // - the batch took a significant time to complete.
                const useInfoLevel =
                      Object.keys(stats.queuedEntries).length > 0 ||
                      stats.skippedRecords > 1000;
                const endLog = batchState.logger.end();
                const logFunc = (useInfoLevel ? endLog.info : endLog.debug)
                      .bind(endLog);
                logFunc('batch completed', {
                    stats,
                    logSource: this.getLogInfo(),
                    logOffset: this.getLogOffset(),
                });
                return done();
            });
        return undefined;
    }

    /* eslint-disable no-param-reassign */

    _processReadRecords(params, batchState, done) {
        if (this._openLog) {
            // an open tailable cursor already exists: reuse it
            batchState.logRes = this._openLog;
            return done();
        }
        const { logger } = batchState;
        const readOptions = {};
        if (this.logOffset !== undefined) {
            readOptions.startSeq = this.logOffset;
        }
        if (params && params.maxRead !== undefined) {
            // only has effect for non-tailable oplog
            readOptions.limit = params.maxRead;
        }
        logger.debug('reading records', { readOptions });
        return this.logConsumer.readRecords(readOptions, (err, res) => {
            if (err) {
                logger.error(
                    'error while reading log records',
                    { method: 'LogReader._processReadRecords',
                        params, error: err });
                return done(err);
            }
            logger.debug(
                'readRecords callback',
                { method: 'LogReader._processReadRecords',
                    params });
            batchState.logRes = res;
            if (res.tailable) {
                // carry tailable cursor over for future batches
                this._openLog = res;
            }
            return done();
        });
    }

    _processLogEntry(batchState, record, entry) {
        if (entry.value === undefined) {
            return;
        }
        this._extensions.forEach(ext => ext.filter({
            type: entry.type,
            bucket: record.db,
            key: entry.key,
            value: entry.value,
        }));
    }

    _processPrepareEntries(batchState, done) {
        const { entriesToPublish, logRes, logStats, logger } = batchState;

        // for raft log only
        if (logRes.info && logRes.info.start === null) {
            return done(null);
        }

        let shipBatchCb;
        const dataEventHandler = record => {
            logger.debug('received log data', {
                nbEntries: record.entries.length,
            });
            logStats.nbLogRecordsRead += 1;

            this._setEntryBatch(entriesToPublish);
            record.entries.forEach(entry => {
                logStats.nbLogEntriesRead += 1;

                this._processLogEntry(batchState, record, entry);
            });
            this._unsetEntryBatch();

            // Pause tailable streams when reaching the record batch
            // limit, as those streams do not emit 'end' events but
            // continuously send 'data' events as they come.
            if (logRes.tailable &&
                logStats.nbLogRecordsRead >= batchState.maxRead) {
                logger.debug('ending batch', {
                    method: 'LogReader._processPrepareEntries',
                    reason: 'limit on number of read records reached',
                    readRecords: logStats.nbLogRecordsRead,
                });
                shipBatchCb();
            }
        };
        const errorEventHandler = err => {
            logger.error('error fetching entries from log', {
                method: 'LogReader._processPrepareEntries',
                error: err,
            });
            return shipBatchCb(err);
        };
        const endEventHandler = () => {
            logger.debug('ending record stream', {
                method: 'LogReader._processPrepareEntries',
            });
            return shipBatchCb();
        };
        logRes.log.on('data', dataEventHandler);
        logRes.log.on('error', errorEventHandler);
        logRes.log.on('end', endEventHandler);

        if (logRes.tailable) {
            //
            // For tailable oplog, set an interval check to timely end
            // and ship the current batch "timeoutMs" after the
            // beginning of the batch.
            //
            // This should guarantee regular updates to the stored
            // offset in zookeeper, while allowing a batch enough time
            // to maximize its rate of consumption of log messages
            // (deemed useful to reduce startup time).
            //
            const checker = setInterval(() => {
                const now = Date.now();
                if (now >= batchState.startTime + batchState.timeoutMs) {
                    logger.debug('ending batch', {
                        method: 'LogReader._processPrepareEntries',
                        reason: 'batch timeout reached',
                        readRecords: logStats.nbLogRecordsRead,
                    });
                    shipBatchCb();
                }
            }, 1000);
            shipBatchCb = jsutil.once(err => {
                logRes.log.pause();
                logRes.log.removeListener('data', dataEventHandler);
                logRes.log.removeListener('error', errorEventHandler);
                logRes.log.removeListener('end', endEventHandler);
                clearInterval(checker);
                done(err);
            });
        } else {
            shipBatchCb = jsutil.once(done);
            // for raft log
            logRes.log.on('info', info => {
                logger.debug('received record stream "info" event', {
                    method: 'LogReader._processPrepareEntries',
                    info,
                });
                logRes.info = info;
            });
        }
        if (logRes.tailable && logRes.log.isPaused()) {
            logger.debug('resuming tailable cursor', {
                method: 'LogReader._processPrepareEntries',
            });
            logRes.log.resume();
        }
        if (logRes.log.getSkipCount) {
            logStats.initialSkipCount = logRes.log.getSkipCount();
        }
        return undefined;
    }

    _setupProducer(topic, done) {
        if (this._producers[topic] !== undefined) {
            return process.nextTick(done);
        }
        const producer = new BackbeatProducer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic,
        });
        producer.once('error', done);
        producer.once('ready', () => {
            this.log.debug('producer is ready',
                           { kafkaConfig: this.kafkaConfig,
                             topic });
            producer.removeAllListeners('error');
            producer.on('error', err => {
                this.log.error('error from backbeat producer',
                               { topic, error: err });
            });
            this._producers[topic] = producer;
            done();
        });
        return undefined;
    }

    _processPublishEntries(batchState, done) {
        const { entriesToPublish, logRes, logStats, logger } = batchState;

        // for raft log only
        if (logRes.info && logRes.info.start === null) {
            return done();
        }

        if (logRes.log.getOffset) {
            batchState.nextLogOffset = logRes.log.getOffset();
        } else {
            batchState.nextLogOffset =
                logRes.info.start + logStats.nbLogRecordsRead;
        }
        if (logStats.initialSkipCount !== undefined) {
            const finalSkipCount = logRes.log.getSkipCount();
            logStats.skippedRecords =
                finalSkipCount - logStats.initialSkipCount;
        }
        return async.each(Object.keys(entriesToPublish), (topic, done) => {
            const topicEntries = entriesToPublish[topic];
            if (topicEntries.length === 0) {
                return done();
            }
            return async.series([
                done => this._setupProducer(topic, done),
                done => this._producers[topic].send(topicEntries, done),
            ], err => {
                if (err) {
                    logger.error(
                        'error publishing entries from log to topic',
                        { method: 'LogReader._processPublishEntries',
                          topic,
                          entryCount: topicEntries.length,
                          error: err });
                    return done(err);
                }
                logger.debug('entries published successfully to topic',
                             { method: 'LogReader._processPublishEntries',
                               topic, entryCount: topicEntries.length });
                batchState.publishedEntries[topic] = topicEntries;
                return done();
            });
        }, err => {
            // TODO: On error, produce error metrics
            if (err) {
                return done(err);
            }
            // Find the CRR Class extension
            const crrExtension = this._extensions.find(ext => (
                ext instanceof ReplicationQueuePopulator
            ));
            const extMetrics = crrExtension.getAndResetMetrics();
            if (Object.keys(extMetrics).length > 0) {
                this._mProducer.publishMetrics(extMetrics, metricsTypeQueued,
                    metricsExtension, () => {});
            }
            return done();
        });
    }

    _processSaveLogOffset(batchState, done) {
        const { logRes, nextLogOffset, logger } = batchState;
        if (nextLogOffset !== undefined && nextLogOffset !== this.logOffset
            && (!logRes.log.reachedUnpublishedListing ||
                logRes.log.reachedUnpublishedListing())) {
            this.logOffset = nextLogOffset;
            return this._writeLogOffset(logger, done);
        }
        return process.nextTick(() => done());
    }

    /* eslint-enable no-param-reassign */

    /**
     * return an object containing useful info about the log
     * source. The default implementation returns an empty object,
     * subclasses may override to provide source-specific info.
     *
     * @return {object} log info object
     */
    getLogInfo() {
        return {};
    }
}

module.exports = LogReader;
