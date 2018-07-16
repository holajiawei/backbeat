'use strict'; // eslint-disable-line

const http = require('http');
const { EventEmitter } = require('events');
const Logger = require('werelogs').Logger;
const { errors } = require('arsenal');

const LifecycleObjectTask = require('../tasks/LifecycleObjectTask');
const BackbeatConsumer = require('../../../lib/BackbeatConsumer');

/**
 * @class LifecycleObjectProcessor
 *
 * @classdesc Handles consuming entries from the object tasks topic
 * and executing the expiration actions on the local CloudServer
 * endpoint using the S3 API.
 */
class LifecycleObjectProcessor extends EventEmitter {

    /**
     * Constructor of LifecycleObjectProcessor
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {String} zkConfig.connectionString - zookeeper connection string
     *  as "host:port[/chroot]"
     * @param {Object} kafkaConfig - kafka configuration object
     * @param {string} kafkaConfig.hosts - list of kafka brokers
     *   as "host:port[,host:port...]"
     * @param {Object} lcConfig - lifecycle configuration object
     * @param {String} lcConfig.auth - authentication info
     * @param {String} lcConfig.objectTasksTopic - lifecycle object topic name
     * @param {Object} lcConfig.consumer - kafka consumer object
     * @param {String} lcConfig.consumer.groupId - kafka consumer group id
     * @param {Number} [lcConfig.consumer.retryTimeoutS] - number of seconds
     *  before giving up retries of an entry lifecycle action
     * @param {Number} [lcConfig.consumer.concurrency] - number of max allowed
     *  concurrent operations
     * @param {Object} [lcConfig.backlogMetrics] - param object to
     * publish backlog metrics to zookeeper (see {@link
     * BackbeatConsumer} constructor)
     * @param {Object} s3Config - S3 configuration
     * @param {Object} s3Config.host - s3 endpoint host
     * @param {Number} s3Config.port - s3 endpoint port
     * @param {String} [transport="http"] - transport method ("http"
     *  or "https")
     */
    constructor(zkConfig, kafkaConfig, lcConfig, s3Config,
                transport = 'http') {
        super();
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
        this.lcConfig = lcConfig;
        this.authConfig = lcConfig.auth;
        this.s3Config = s3Config;
        this._transport = transport;
        this._consumer = null;

        this.logger = new Logger('Backbeat:Lifecycle:ObjectConsumer');

        // global variables
        // TODO: for SSL support, create HTTPS agents instead
        this.httpAgent = new http.Agent({ keepAlive: true });
    }


    /**
     * Start kafka consumer. Emits a 'ready' event when
     * consumer is ready.
     *
     * @return {undefined}
     */
    start() {
        let consumerReady = false;
        this._consumer = new BackbeatConsumer({
            zookeeper: {
                connectionString: this.zkConfig.connectionString,
            },
            kafka: { hosts: this.kafkaConfig.hosts },
            topic: this.lcConfig.objectTasksTopic,
            groupId: this.lcConfig.consumer.groupId,
            concurrency: this.lcConfig.consumer.concurrency,
            queueProcessor: this.processKafkaEntry.bind(this),
            autoCommit: true,
            backlogMetrics: this.lcConfig.backlogMetrics,
        });
        this._consumer.on('error', () => {
            if (!consumerReady) {
                this.logger.fatal('error starting lifecycle consumer');
                process.exit(1);
            }
        });
        this._consumer.on('ready', () => {
            consumerReady = true;
            this._consumer.subscribe();
            this.logger.info('lifecycle consumer successfully started');
            return this.emit('ready');
        });
    }

    /**
     * Close the lifecycle consumer
     * @param {function} cb - callback function
     * @return {undefined}
     */
    close(cb) {
        this.logger.debug('closing object tasks consumer');
        this._consumer.close(cb);
    }

    /**
     * Proceed to the lifecycle action of an object given a kafka
     * object lifecycle queue entry
     *
     * @param {object} kafkaEntry - entry generated by the queue populator
     * @param {function} done - callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, done) {
        this.logger.debug('processing kafka entry');

        let entryData;
        try {
            entryData = JSON.parse(kafkaEntry.value);
        } catch (err) {
            this.logger.error('error processing lifecycle object entry',
                { error: err });
            return process.nextTick(() => done(errors.InternalError));
        }
        const task = new LifecycleObjectTask(this);
        return task.processQueueEntry(entryData, done);
    }

    getStateVars() {
        return {
            s3Config: this.s3Config,
            lcConfig: this.lcConfig,
            authConfig: this.authConfig,
            transport: this._transport,
            httpAgent: this.httpAgent,
            logger: this.logger,
        };
    }

    isReady() {
        return this._consumer && this._consumer.isReady();
    }

}

module.exports = LifecycleObjectProcessor;
