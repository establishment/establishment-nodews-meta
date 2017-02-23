const {Glue} = require("establishment-node-service-core");

class NodeWSServerData {
    constructor(metadataObserver) {
        this.metadataObserver = metadataObserver;

        this.userConnection = new Map();
    }

    userConnectionNewEvent(connectionId) {
        let userConnection = {
            userId: null,
            streams: [],
            fields: {}
        };
        this.userConnection.set(connectionId, userConnection);

        this.metadataObserver.userConnectionNewEvent(connectionId);
    }

    userConnectionAddField(connectionId, key, value) {
        let userConnection = this.userConnection.get(connectionId);
        userConnection.fields[key] = value;
        this.userConnection.set(connectionId, userConnection);

        this.metadataObserver.userConnectionAddField(connectionId, key, value);
    }

    userConnectionIdentificationEvent(connectionId, userId) {
        let userConnection = this.userConnection.get(connectionId);
        if (userConnection == undefined) {
            Glue.logger.critical("Establishment::NodeWSServerData: userConnectionIdentificationEvent request to non " +
                                 "existent connectionId! This should not happen!");
            return;
        }
        userConnection.userId = userId;
        this.userConnection.set(connectionId, userConnection);

        this.metadataObserver.userConnectionIdentificationEvent(connectionId, userId);
    }

    userConnectionSubscribe(connectionId, userId, channel) {
        let userConnection = this.userConnection.get(connectionId);
        if (userConnection == undefined) {
            Glue.logger.critical("Establishment::NodeWSServerData: userConnectionSubscribe request to non existent " +
                                 "connectionId! This should not happen!");
            return;
        }
        userConnection.streams.push(channel);
        this.userConnection.set(connectionId, userConnection);

        this.metadataObserver.userConnectionSubscribe(connectionId, userId, channel);
    }

    userConnectionDestroyEvent(connectionId, userId) {
        this.userConnection.delete(connectionId);

        this.metadataObserver.userConnectionDestroyEvent(connectionId, userId);
    }

    clear() {
        let tempConnections = [];

        for (let [connectionId, userConnection] of this.userConnection) {
            tempConnections.push({
                connectionId: connectionId,
                userId: userConnection.userId
            });
        }

        for (let userConnection of tempConnections) {
            this.userConnectionDestroyEvent(userConnection.connectionId, userConnection.userId);
        }
    }

    destroy() {
        this.clear();

        this.metadataObserver = null;
        this.userConnection = null;
    }
}

module.exports = NodeWSServerData;
