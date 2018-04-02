const { usersBucket } = require('arsenal').constants;

const ObjectQueueEntry = require('./ObjectQueueEntry');
const BucketQueueEntry = require('./BucketQueueEntry');
const BucketMdQueueEntry = require('./BucketMdQueueEntry');
const DeleteOpQueueEntry = require('./DeleteOpQueueEntry');

class QueueEntry {

    /**
     * factory method that creates the proper sub-class instance
     * depending on the type of kafka entry (object/bucket/whatever)
     *
     * @param {Object} kafkaEntry - entry as read from Kafka queue

     * @return {Object} - an object which inherits from
     *   {@link QueueEntry} base class
     */
    static createFromKafkaEntry(kafkaEntry) {
        console.log('KAFKA ENTRY in QueueEntry', kafkaEntry);
        console.log(JSON.stringify(kafkaEntry));
        console.log(kafkaEntry.value.toString());
        try {
            const record = JSON.parse(kafkaEntry.value);
            if (record.bootstrapId) {
                return { error: 'bootstrap entry' };
            }
            let entry;
            if (record.type === 'del') {
                entry = new DeleteOpQueueEntry(record.bucket, record.key);
            } else if (record.bucket === usersBucket) {
                // BucketQueueEntry class just handles puts of keys
                // to usersBucket
                entry = new BucketQueueEntry(record.key, record.value);
            } else if (record.value) {
                const metadataVal = JSON.parse(record.value);
                console.log('we found metadataVal', metadataVal);
                if (metadataVal.mdBucketModelVersion) {
                    // it's bucket metadata
                    entry = new BucketMdQueueEntry(record.key, metadataVal);
                } else if (metadataVal.attributes) {
                    // S3 Connector bucket metadata is within
                    // an attributes object
                    const nestedVal = JSON.parse(metadataVal.attributes);
                    entry = new BucketMdQueueEntry(nestedVal.name, nestedVal);
                } else {
                    // it's object metadata

                    // TODO: consider having a separate elseif/entry type
                    // for mpu parts since those entries do not have
                    // full object metadata
                    entry = new ObjectQueueEntry(record.bucket, record.key,
                        metadataVal);
                    entry.setSite(record.site);
                }
            } else {
                return { error: 'unknown kafka entry format' };
            }
            const err = entry.checkSanity();
            if (err) {
                return { error: err };
            }
            return entry;
        } catch (err) {
            return { error: { message: 'malformed JSON in kafka entry',
                              description: err.message } };
        }
    }
}

module.exports = QueueEntry;