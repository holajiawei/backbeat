const http = require('http');
const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const VaultClientCache = require('./clients/VaultClientCache');
const BackbeatClient = require('./clients/BackbeatClient');
const BackbeatTask = require('./tasks/BackbeatTask');
const { attachReqUids } = require('./clients/utils');
const RoleCredentials = require('./credentials/RoleCredentials');
const { getAccountCredentials } = require('./credentials/AccountCredentials');

class BackbeatMetadataProxy extends BackbeatTask {
    constructor(s3Endpoint, s3Auth, sourceHTTPAgent) {
        super();
        this._s3Endpoint = s3Endpoint;
        this._s3Auth = s3Auth;
        // TODO: For SSL support, create HTTPS agents instead.
        this._sourceHTTPAgent = sourceHTTPAgent ||
            new http.Agent({ keepAlive: true });
        this._setupVaultclientCache();
    }

    _setupVaultclientCache() {
        this.vaultclientCache = new VaultClientCache();
        if (this._s3Auth.type === 'role') {
            const { host, port } = this._s3Auth.vault;
            this.vaultclientCache
                .setHost('source:s3', host)
                .setPort('source:s3', port);
        }
    }

    _createCredentials(log) {
        const accountCredentials = getAccountCredentials(this._s3Auth, log);
        if (accountCredentials) {
            return accountCredentials;
        }
        const vaultclient = this.vaultclientCache.getClient('source:s3');
        const extension = 'replication';
        const role = this.sourceRole;
        return new RoleCredentials(vaultclient, extension, role, log);
    }

    putMetadata(params, mdBlob, log, cb) {
        this.retry({
            actionDesc: 'update metadata on source',
            logFields: { entry: params },
            actionFunc: done =>
                this._putMetadataOnce(params, mdBlob, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _putMetadataOnce(params, mdBlob, log, cb) {
        log.debug('putting metadata', {
            where: 'source',
            bucket: params.bucket,
            objectKey: params.objectKey,
            versionId: params.versionId,
        });

        // sends extra header x-scal-replication-content to the target
        // if it's a metadata operation only
        const req = this.backbeatSource.putMetadata({
            Bucket: params.bucket,
            Key: params.objectKey,
            ContentLength: Buffer.byteLength(mdBlob),
            Body: mdBlob,
        });
        attachReqUids(req, log);
        req.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                if (err.ObjNotFound || err.code === 'ObjNotFound') {
                    return cb(err);
                }
                log.error('an error occurred when putting metadata to S3',
                    { method: 'BackbeatMetadataProxy._putMetadataOnce',
                      entry: params,
                      origin: 'source',
                      endpoint: this._s3Endpoint,
                      error: err.message });
                return cb(err);
            }
            log.debug('PutMetadata returned with payload', {
                method: 'BackbeatMetadataProxy._putMetadataOnce',
                entry: params,
                endpoint: this._s3Endpoint,
                payload: data,
            });
            return cb(null, data);
        });
    }

    getMetadata(params, log, cb) {
        this.retry({
            actionDesc: 'get metadata from source',
            logFields: { entry: params },
            actionFunc: done => this._getMetadataOnce(params, log, done),
            shouldRetryFunc: err => err.retryable,
            log,
        }, cb);
    }

    _getMetadataOnce(params, log, cb) {
        log.debug('getting metadata', {
            where: 'source',
            entry: params,
            method: 'BackbeatMetadataProxy._getMetadataOnce',
        });

        const cbOnce = jsutil.once(cb);

        const req = this.backbeatSource.getMetadata({
            Bucket: params.bucket,
            Key: params.objectKey,
            VersionId: params.versionId,
        });
        attachReqUids(req, log);
        req.send((err, data) => {
            if (err) {
                // eslint-disable-next-line no-param-reassign
                err.origin = 'source';
                if (err.ObjNotFound || err.code === 'ObjNotFound') {
                    return cbOnce(err);
                }
                log.error('an error occurred when getting metadata from S3', {
                    method: 'BackbeatMetadataProxy._getMetadataOnce',
                    entry: params,
                    origin: 'source',
                    endpoint: this._s3Endpoint,
                    error: err,
                    errMsg: err.message,
                    errCode: err.code,
                    errStack: err.stack,
                });
                return cbOnce(err);
            }
            return cbOnce(null, data);
        });
    }

    setupSourceRole(entry, log) {
        log.debug('getting bucket replication', { entry: entry.getLogInfo() });
        const entryRolesString = entry.getReplicationRoles();
        let entryRoles;
        if (entryRolesString !== undefined) {
            entryRoles = entryRolesString.split(',');
        }
        if (entryRoles === undefined ||
            (entryRoles.length !== 1 && entryRoles.length !== 2)) {
            const errMessage = 'expecting one or two roles in bucket ' +
                'replication configuration';
            log.error(errMessage, {
                method: 'BackbeatMetadataProxy.setupSourceRole',
                entry: entry.getLogInfo(),
                roles: entryRolesString,
            });
            return { error: errors.BadRole.customizeDescription(errMessage) };
        }
        this.sourceRole = entryRoles[0];
        return this;
    }

    setSourceRole(sourceRole) {
        this.sourceRole = sourceRole;
    }

    setSourceClient(log) {
        this.backbeatSource = new BackbeatClient({
            endpoint: this._s3Endpoint,
            credentials: this._createCredentials(log),
            sslEnabled: this._s3Endpoint.startsWith('https:'),
            httpOptions: { agent: this._sourceHTTPAgent, timeout: 0 },
            maxRetries: 0, // Disable retries, use our own retry policy
        });
        return this;
    }
}

module.exports = BackbeatMetadataProxy;