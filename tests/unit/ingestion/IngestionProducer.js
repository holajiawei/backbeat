const assert = require('assert');
const async = require('async');
const AWS = require('aws-sdk');

const IngestionProducer =
    require('../../../lib/queuePopulator/IngestionProducer');
const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const testConfig = require('../../config.json');
const fakeLogger = require('../../utils/fakeLogger');

const sourceConfig = testConfig.extensions.ingestion.sources[0];
const { bucket } = sourceConfig;

const s3sourceCredentials = new AWS.Credentials({
    accessKeyId: 'accessKey1',
    secretAccessKey: 'verySecretKey1',
});
const awsClient = new AWS.S3({
    endpoint: 'http://localhost:8000',
    credentials: s3sourceCredentials,
    sslEnabled: false,
    maxRetries: 0,
    httpOptions: { timeout: 0 },
    s3ForcePathStyle: true,
    signatureVersion: 'v4',
});

function cleanupBucketAndObjects(done) {
    awsClient.getBucketVersioning({ Bucket: bucket }, (err, res) => {
        if (err) {
            if (err.code === 'NoSuchBucket') {
                return done();
            }
            return done(err);
        }
        const list = [];
        return async.waterfall([
            next => {
                if (!res.Status) {
                    // bucket is not versioned
                    return awsClient.listObjects({ Bucket: bucket },
                    (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        list.push(...res.Contents.map(o => ({ Key: o.Key })));
                        return next();
                    });
                }
                // bucket may have versions
                return awsClient.listObjectVersions({ Bucket: bucket },
                (err, res) => {
                    if (err) {
                        return next(err);
                    }
                    if (res.Versions && res.Versions.length) {
                        list.push(...res.Versions.map(v => ({
                            Key: v.Key,
                            VersionId: v.VersionId,
                        })));
                    }
                    if (res.DeleteMarkers && res.DeleteMarkers.length) {
                        list.push(...res.DeleteMarkers.map(dm => ({
                            Key: dm.Key,
                            VersionId: dm.VersionId,
                        })));
                    }
                    return next();
                });
            },
            next => {
                if (!list.length) {
                    return next();
                }
                return awsClient.deleteObjects({
                    Bucket: bucket,
                    Delete: { Objects: list },
                }, next);
            },
            next => awsClient.deleteBucket({ Bucket: bucket }, next),
        ], done);
    });
}

describe('Ingestion Producer', () => {
    before(() => {
        const adjustedConfig = Object.assign({}, sourceConfig, {
            auth: { accessKey: 'accessKey1', secretKey: 'verySecretKey1' },
        });
        this.ingestionProducer = new IngestionProducer(adjustedConfig,
            testConfig.queuePopulator, testConfig.s3);
    });

    it('should setup clients synchronously when creating new instance',
    done => {
        const bbClient = this.ingestionProducer._ringReader;
        const { endpoint } = bbClient;
        const { credentials } = bbClient.config;

        assert(bbClient instanceof BackbeatClient);
        assert.strictEqual(endpoint.port, sourceConfig.port);
        assert.strictEqual(endpoint.hostname, sourceConfig.host);
        assert.strictEqual(credentials.accessKey, sourceConfig.accessKey);

        const s3Client = this.ingestionProducer._s3Client;
        // just perform a list buckets and see if I get a response
        s3Client.listBuckets((err, res) => {
            assert.ifError(err);
            assert(res);
            done();
        });
    });

    describe('::validateRemoteAccess', () => {
        afterEach(done => {
            cleanupBucketAndObjects(done);
        });

        it('should return an error if source bucket does not exist', done => {
            this.ingestionProducer.validateRemoteAccess(bucket, fakeLogger,
            error => {
                assert.strictEqual(error.code, 'NoSuchBucket');
                return done();
            });
        });

        it('should return an error if source bucket is not version enabled',
        done => {
            awsClient.createBucket({ Bucket: bucket }, err => {
                assert.ifError(err);

                this.ingestionProducer.validateRemoteAccess(bucket, fakeLogger,
                error => {
                    assert.strictEqual(error.code, 500);
                    done();
                });
            });
        });

        it('should return an error if given invalid secret key', done => {
            // by default, credentials in test config file will not work.
            // Secret key is encrypted
            const iProducer = new IngestionProducer(sourceConfig,
                testConfig.queuePopulator, testConfig.s3);
            async.series([
                next => awsClient.createBucket({ Bucket: bucket }, next),
                next => iProducer.validateRemoteAccess(bucket, fakeLogger,
                    error => {
                        assert.strictEqual(error.code, 'SignatureDoesNotMatch');
                        next();
                    }),
            ], done);
        });

        it('should return no error with valid location information', done => {
            async.series([
                next => awsClient.createBucket({ Bucket: bucket }, next),
                next => awsClient.putBucketVersioning({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Enabled' },
                }, next),
                next => this.ingestionProducer.validateRemoteAccess(bucket,
                    fakeLogger, next),
            ], err => {
                assert.ifError(err);
                done();
            });
        });
    });
});
