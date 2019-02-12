const async = require('async');

const { errors, jsutil, models } = require('arsenal');
const { ObjectMD } = models;

const BackbeatTask = require('../../../lib/tasks/BackbeatTask');
const { attachReqUids } = require('../../../lib/clients/utils');
const { metricsExtension, metricsTypeQueued, metricsTypeCompleted } =
    require('../constants');

function _getExtMetrics(site, bytes, actionEntry) {
    const extMetrics = {};
    extMetrics[site] = {
        bytes,
        bucketName: actionEntry.getAttribute('target.bucket'),
        objectKey: actionEntry.getAttribute('target.key'),
        versionId: actionEntry.getAttribute('target.version'),
    };
    return extMetrics;
}

class CopyLocationTask extends BackbeatTask {

    _getReplicationEndpointType() {
        const replicationEndpoint = this.destConfig.bootstrapList
            .find(endpoint => endpoint.site === this.site);
        return replicationEndpoint.type;
    }

    constructor(qp) {
        const qpState = qp.getStateVars();
        super();
        Object.assign(this, qpState);
        this.destType = null;
        if (this.destConfig && this.destConfig.bootstrapList) {
            this.destType = this._getReplicationEndpointType();
            const retryParams =
                  this.repConfig.queueProcessor.retry[this.destType];
            if (retryParams) {
                this.retryParams = retryParams;
            }
        }
    }

    processQueueEntry(actionEntry, kafkaEntry, done) {
        const log = this.logger.newRequestLogger();
        actionEntry.addLoggedAttributes({
            bucketName: 'target.bucket',
            objectKey: 'target.key',
            versionId: 'target.version',
            copyToLocation: 'toLocation',
        });
        log.debug('action execution starts', actionEntry.getLogInfo());
        return async.waterfall([
            next => this._getSourceMD(actionEntry, log, (err, objMD) => {
                if (err && err.code === 'ObjNotFound') {
                    // The object was deleted before entry is processed, we
                    // can safely skip this entry.
                    return next(errors.InvalidObjectState);
                }
                if (err) {
                    return next(err);
                }
                return next(null, objMD);
            }),
            (objMD, next) =>
                // TODO check that MD5 matches between objMD and
                // target.contentMd5, return InvalidObjectState if not
                this._getAndPutObject(actionEntry, objMD, log, next),
        ], err => this._publishCopyLocationStatus(
            err, actionEntry, kafkaEntry, log, done));
    }

    _getSourceMD(actionEntry, log, cb) {
        const params = {
            bucket: actionEntry.getAttribute('target.bucket'),
            objectKey: actionEntry.getAttribute('target.key'),
            versionId: actionEntry.getAttribute('target.version'),
        };
        return this.backbeatMetadataProxy.getMetadata(
        params, log, (err, blob) => {
            if (err) {
                log.error('error getting metadata blob from S3', Object.assign({
                    method: 'CopyLocationTask._getSourceMD',
                    error: err,
                }, actionEntry.getLogInfo()));
                return cb(err);
            }
            const res = ObjectMD.createFromBlob(blob.Body);
            if (res.error) {
                log.error('error parsing metadata blob', Object.assign({
                    error: res.error,
                    method: 'CopyLocationTask._getSourceMD',
                }, actionEntry.getLogInfo()));
                return cb(errors.InternalError.
                    customizeDescription('error parsing metadata blob'));
            }
            return cb(null, res.result);
        });
    }

    _getAndPutObject(actionEntry, objMD, log, cb) {
        const partLogger = this.logger.newRequestLogger(log.getUids());
        const extMetrics = _getExtMetrics(this.site,
            objMD.getContentLength(), actionEntry);
        this.mProducer.publishMetrics(extMetrics,
            metricsTypeQueued, metricsExtension, () => {});
        this.retry({
            actionDesc: 'stream part data',
            logFields: { entry: actionEntry.getLogInfo() },
            actionFunc: done => this._getAndPutObjectOnce(
                actionEntry, objMD, partLogger, done),
            shouldRetryFunc: err => err.retryable,
            log: partLogger,
        }, cb);
    }

    _getAndPutObjectOnce(actionEntry, objMD, log, done) {
        log.debug('getting object data', actionEntry.getLogInfo());
        const doneOnce = jsutil.once(done);
        const size = objMD.getContentLength();
        let incomingMsg = null;
        if (size !== 0) {
            const { bucket, key, version } = actionEntry.getAttribute('target');
            const sourceReq = this.backbeatClient.getObject({
                Bucket: bucket,
                Key: key,
                VersionId: version,
                LocationConstraint: objMD.getDataStoreName(),
            });
            attachReqUids(sourceReq, log);
            sourceReq.on('error', err => {
                // eslint-disable-next-line no-param-reassign
                if (err.statusCode === 404) {
                    log.error('the source object was not found', Object.assign({
                        method: 'CopyLocationTask._getAndPutObjectOnce',
                        peer: this.sourceConfig.s3,
                        error: err.message,
                        httpStatus: err.statusCode,
                    }, actionEntry.getLogInfo()));
                    return doneOnce(err);
                }
                log.error('an error occurred on getObject from S3',
                    Object.assign({
                        method: 'CopyLocationTask._getAndPutObjectOnce',
                        peer: this.sourceConfig.s3,
                        error: err.message,
                        httpStatus: err.statusCode,
                    }, actionEntry.getLogInfo()));
                return doneOnce(err);
            });
            incomingMsg = sourceReq.createReadStream();
            incomingMsg.on('error', err => {
                if (err.statusCode === 404) {
                    log.error('the source object was not found', Object.assign({
                        method: 'CopyLocationTask._getAndPutObjectOnce',
                        peer: this.sourceConfig.s3,
                        error: err.message,
                        httpStatus: err.statusCode,
                    }, actionEntry.getLogInfo()));
                    return doneOnce(errors.ObjNotFound);
                }
                // eslint-disable-next-line no-param-reassign
                log.error('an error occurred when streaming data from S3',
                    Object.assign({
                        method: 'CopyLocationTask._getAndPutObjectOnce',
                        peer: this.sourceConfig.s3,
                        error: err.message,
                    }, actionEntry.getLogInfo()));
                return doneOnce(err);
            });
            log.debug('putting data', actionEntry.getLogInfo());
        }
        return this._sendMultipleBackendPutObject(
            actionEntry, objMD, size, incomingMsg, log, doneOnce);
    }

    /**
     * Send the put object request to Cloudserver.
     * @param {ActionQueueEntry} actionEntry - the action entry
     * @param {ObjectMD} objMD - metadata object
     * @param {Number} size - The size of object to stream
     * @param {Readable} incomingMsg - The stream of data to put
     * @param {Werelogs} log - The logger instance
     * @param {Function} doneOnce - The callback to call
     * @return {undefined}
     */
    _sendMultipleBackendPutObject(actionEntry, objMD, size,
        incomingMsg, log, doneOnce) {
        const { bucket, key, version } = actionEntry.getAttribute('target');
        const destReq = this.backbeatClient.multipleBackendPutObject({
            Bucket: bucket,
            Key: key,
            CanonicalID: objMD.getOwnerId(),
            ContentLength: size,
            ContentMD5: objMD.getContentMd5(),
            StorageType: this.destType,
            StorageClass: this.site,
            VersionId: version,
            UserMetaData: objMD.getUserMetadata(),
            ContentType: objMD.getContentType() || undefined,
            CacheControl: objMD.getCacheControl() || undefined,
            ContentDisposition:
                objMD.getContentDisposition() || undefined,
            ContentEncoding: objMD.getContentEncoding() || undefined,
            Body: incomingMsg,
        });
        attachReqUids(destReq, log);
        return destReq.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                log.error('an error occurred on putObject to S3',
                Object.assign({
                    method: 'CopyLocationTask._sendMultipleBackendPutObject',
                    error: err.message,
                }, actionEntry.getLogInfo()));
                return doneOnce(err);
            }
            actionEntry.setSuccess({
                location: [{
                    // XXX the key should be returned in
                    // multipleBackendPutObject() response
                    key: `${bucket}/${key}`,
                    size,
                    start: 0,
                    dataStoreName: this.site,
                    dataStoreType: this.destType,
                    // XXX same
                    dataStoreETag: `1:${objMD.getContentMd5()}`,
                    dataStoreVersionId: data.versionId,
                }],
            });
            const extMetrics = _getExtMetrics(this.site, size, actionEntry);
            this.mProducer.publishMetrics(extMetrics,
                metricsTypeCompleted, metricsExtension, () => {});
            return doneOnce(null, data);
        });
    }

    _publishCopyLocationStatus(err, actionEntry, kafkaEntry, log, done) {
        if (err) {
            actionEntry.setError(err);
        }
        log.info('action execution ended', actionEntry.getLogInfo());
        if (!actionEntry.getResultsTopic()) {
            // no result requested, we may commit immediately
            return process.nextTick(() => done(null, { committable: true }));
        }
        this.replicationStatusProducer.sendToTopic(
            actionEntry.getResultsTopic(),
            [{ message: actionEntry.toKafkaMessage() }], deliveryErr => {
                if (deliveryErr) {
                    log.error('error in entry delivery to results topic',
                    Object.assign({
                        method: 'CopyLocationTask._publishCopyLocationStatus',
                        topic: actionEntry.getResultsTopic(),
                        error: deliveryErr.message,
                    }, actionEntry.getLogInfo()));
                }
                // Commit whether there was an error or not delivering
                // the message to allow progress of the consumer, as
                // best effort measure when there are errors.
                this.dataMoverConsumer.onEntryCommittable(kafkaEntry);
            });
        return process.nextTick(() => done(null, { committable: false }));
    }
}

module.exports = CopyLocationTask;
