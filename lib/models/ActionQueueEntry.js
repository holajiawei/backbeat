/**
 * @class
 * @classdesc Generic class holding an action-typed Kafka queue entry,
 * that modules can use for their own purpose. "target" and "details"
 * can be any free-form serializable JS objects.
 */
class ActionQueueEntry {

    /**
     * @constructor
     * @param {string} actionType - predefined action type
     * @param {object} [actionTarget] - target for the action, as a
     * custom JS object holding info about the main target for the
     * action
     * @param {object} [actionDetails] - extra details needed or
     * useful to execute the action, as a custom JS object holding
     * action-specific attributes
     */
    constructor(actionType, actionTarget, actionDetails) {
        this._actionType = actionType;
        this._actionTarget = actionTarget;
        this._actionDetails = actionDetails;
    }

    getActionType() {
        return this._actionType;
    }

    getActionTarget() {
        return this._actionTarget;
    }

    getActionDetails() {
        return this._actionDetails;
    }
}

module.exports = ActionQueueEntry;
