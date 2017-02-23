const {RedisConnectionPool} = require("establishment-node-core");
const {Glue, UniqueIdentifierFactory} = require("establishment-node-service-core");

const MetadataObserver = require("./MetadataObserver.js6.js");
const NodeWSServerData = require("./NodeWSServerData.js6.js");

class MetadataServer {
    constructor(config) {
        this.config = config;
        this.redisSubscriber = RedisConnectionPool.getConnection(config.metadataBridge.redis.address);
        this.redisPublisher = RedisConnectionPool.getSharedConnection(config.metadataBridge.redis.address);
        this.uidFactory = new UniqueIdentifierFactory(config.uidFactory);
        this.metadataObserver = new MetadataObserver(config.metadataObserver);
        this.nodewsServers = new Map();
    }

    start() {
        this.metadataObserver.reset(() => {
            this.run();
        }, () => {
            Glue.logger.critical("Establishment::MetadataServer: Could not initialize meta-data observer.");

            this.metadataObserver = MetadataObserver.getDummyAPI();

            this.run();
        });
    }

    run() {
        this.redisSubscriber.on("error", (err) => {
            Glue.logger.error("Establishment::MetadataServer Redis Bridge: " + err);
        });

        this.redisSubscriber.on("ready", () => {
            Glue.logger.info("Establishment::MetadataServer Redis Bridge ready!");
        });

        this.redisSubscriber.on("message", (channel, message) => {
            if (channel == this.config.metadataBridge.redis.inputStream) {
                this.processMessage(message);
            } else {
                Glue.logger.critical("Establishment::MetadataServer Redis Bridge received message from unknown " +
                                     "stream #" + channel + ". This should not happen!");
            }
        });

        this.redisSubscriber.subscribe(this.config.metadataBridge.redis.inputStream);

        this.requestSyncAll();
    }

    getServer(serverId) {
        if (!this.nodewsServers.has(serverId)) {
            Glue.logger.warning("Establishment::MetadataServer: received update message before server " +
                                "synchronization. Going to ignore!");
            return null;
        }
        return this.nodewsServers.get(serverId);
    }

    getOrCreateServer(serverId) {
        if (!this.nodewsServers.has(serverId)) {
            let serverData = {
                server: new NodeWSServerData(this.metadataObserver),
                expireTime: this.config.metadataBridge.nodews.keepAlive.defaultExpireTime,
                expirePhase: MetadataServer.KEEPALIVE_PHASE.WAIT_FOR_NORMAL_KEEPALIVE,
                timeout: null
            };
            this.nodewsServers.set(serverId, serverData);
        }
        return this.nodewsServers.get(serverId);
    }

    resetOrCreateServer(serverId) {
        if (!this.nodewsServers.has(serverId)) {
            let serverData = {
                server: new NodeWSServerData(this.metadataObserver),
                expireTime: this.config.metadataBridge.nodews.keepAlive.defaultExpireTime,
                expirePhase: MetadataServer.KEEPALIVE_PHASE.WAIT_FOR_NORMAL_KEEPALIVE,
                timeout: null
            };
            this.nodewsServers.set(serverId, serverData);
        } else {
            let serverData = this.nodewsServers.get(serverId);
            serverData.expireTime = this.config.metadataBridge.nodews.keepAlive.defaultExpireTime;
            serverData.expirePhase = MetadataServer.KEEPALIVE_PHASE.WAIT_FOR_NORMAL_KEEPALIVE;
            if (serverData.timeout != null) {
                clearTimeout(serverData.timeout);
                serverData.timeout = null;
            }
            serverData.server.clear();
            this.nodewsServers.set(serverId, serverData);
        }
        return this.nodewsServers.get(serverId);
    }

    deleteServer(serverId) {
        if (!this.nodewsServers.has(serverId)) {
            Glue.logger.critical("Establishment::MetadataServer: tried to delete nonexistent server (serverId = " +
                                 serverId + ")! This should not happen!");
            return;
        }

        let serverData = this.nodewsServers.get(serverId);
        if (serverData.timeout != null) {
            clearTimeout(serverData.timeout);
            serverData.timeout = null;
        }

        serverData.server.clear();
        this.nodewsServers.delete(serverId);
    }

    keepAliveTimeoutEvent(serverId) {
        if (!this.nodewsServers.has(serverId)) {
            Glue.logger.critical("Establishment::MetadataServer: invalid serverId (" + serverId + ") in " +
                                 "keepAliveTimeoutEvent!");
            return false;
        }

        let serverData = this.nodewsServers.get(serverId);

        if (serverData.expirePhase == MetadataServer.KEEPALIVE_PHASE.WAIT_FOR_NORMAL_KEEPALIVE) {
            Glue.logger.info("Establishment::MetadataServer: nodews server with " + serverId + " serverId (" +
                             serverId + ") just reached keep alive timeout!");
            this.checkAlive(serverId);
        } else if (serverData.expirePhase == MetadataServer.KEEPALIVE_PHASE.WAIT_FOR_REQUESTED_KEEPALIVE) {
            Glue.logger.critical("Establishment::MetadataServer: nodews server with " + serverId +
                                 " serverId just got offline! (keepAlive)");
            this.deleteServer(serverId);
        }
    }

    checkAlive(serverId) {
        let serverData = this.nodewsServers.get(serverId);
        serverData.expirePhase = MetadataServer.KEEPALIVE_PHASE.WAIT_FOR_REQUESTED_KEEPALIVE;
        if (serverData.timeout != null) {
            clearTimeout(serverData.timeout);
        }
        serverData.timeout = setTimeout(() => {
            this.keepAliveTimeoutEvent(serverId);
        }, this.config.metadataBridge.nodews.keepAlive.requestedKeepAliveExpireTime);

        this.nodewsServers.set(serverId, serverData);

        let message = {
            type: "checkAlive",
            id: serverId
        };

        this.sendMessage(message);
    }

    checkAliveAll() {
        let tempServers = this.nodewsServers;
        for (let [serverId, serverData] of tempServers) {
            serverData.expirePhase = MetadataServer.KEEPALIVE_PHASE.WAIT_FOR_REQUESTED_KEEPALIVE;
            if (serverData.timeout != null) {
                clearTimeout(serverData.timeout);
            }
            serverData.timeout = setTimeout(() => {
                this.keepAliveTimeoutEvent(serverId);
            }, this.config.metadataBridge.nodews.keepAlive.requestedKeepAliveExpireTime);

            this.nodewsServers.set(serverId, serverData);
        }

        let message = {
            type: "checkAliveAll"
        };

        this.sendMessage(message);
    }

    renewServerExpireTime(serverId, timeoutParam) {
        if (!this.nodewsServers.has(serverId)) {
            Glue.logger.critical("Establishment::MetadataServer: tried to renew server (serverId = " + serverId +
                                 ") expire time for invalid server! Going to request resync!");
            this.requestSync(serverId);
            return;
        }

        let serverData = this.nodewsServers.get(serverId);
        if (serverData.timeout != null) {
            clearTimeout(serverData.timeout);
        }

        if (timeoutParam != -1) {
            serverData.expireTime = timeoutParam * this.config.metadataBridge.nodews.keepAlive.expireTimeCoeff;
        }

        serverData.expirePhase = MetadataServer.KEEPALIVE_PHASE.WAIT_FOR_NORMAL_KEEPALIVE;

        serverData.timeout = setTimeout(() => {
            this.keepAliveTimeoutEvent(serverId);
        }, serverData.expireTime);

        this.nodewsServers.set(serverId, serverData);
    }

    sendMessage(message) {
        this.redisPublisher.publish(this.config.metadataBridge.redis.outputStream, JSON.stringify(message));
    }

    requestSync(serverId) {
        if (serverId == null) {
            this.requestSyncAll();
        } else {
            let message = {
                type: "requestSync",
                id: serverId
            };
            this.sendMessage(message);
        }
    }

    requestSyncAll() {
        let message = {
            type: "requestSyncAll"
        };
        this.sendMessage(message);
    }

    processMessage(message) {
        message = this.parseMessage(message);
        if (message == null) {
            return false;
        }

        let timeoutParam = -1;
        if (message.type == "userConnectionNewEvent") {
            if (!message.hasOwnProperty("connectionId")) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": userConnectionNewEvent requires connectionId param!");
                return false;
            }

            this.userConnectionNewEvent(message.id, message.connectionId);
        } else if (message.type == "userConnectionAddField") {
            if (!message.hasOwnProperty("connectionId")) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": userConnectionAddField requires connectionId param!");
                return false;
            }
            if (!message.hasOwnProperty("key")) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": userConnectionAddField requires key param!");
                return false;
            }
            if (!message.hasOwnProperty("value")) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": userConnectionAddField requires value param!");
                return false;
            }

            this.userConnectionAddField(message.id, message.connectionId, message.key, message.value);
        } else if (message.type == "userConnectionIdentificationEvent") {
            if (!message.hasOwnProperty("connectionId")) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": userConnectionIdentificationEvent requires connectionId param!");
                return false;
            }
            if (!message.hasOwnProperty("userId")) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": userConnectionIdentificationEvent requires userId param!");
                return false;
            }

            this.userConnectionIdentificationEvent(message.id, message.connectionId, message.userId);
        } else if (message.type == "userConnectionSubscribe") {
            if (!message.hasOwnProperty("connectionId")) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": userConnectionSubscribe requires connectionId param!");
                return false;
            }
            if (!message.hasOwnProperty("userId")) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": userConnectionSubscribe requires userId param!");
                return false;
            }
            if (!message.hasOwnProperty("channel")) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": userConnectionSubscribe requires channel param!");
                return false;
            }

            this.userConnectionSubscribe(message.id, message.connectionId, message.userId, message.channel);
        } else if (message.type == "userConnectionDestroyEvent") {
            if (!message.hasOwnProperty("connectionId")) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": userConnectionDestroyEvent requires connectionId param!");
                return false;
            }
            if (!message.hasOwnProperty("userId")) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": userConnectionDestroyEvent requires userId param!");
                return false;
            }

            this.userConnectionDestroyEvent(message.id, message.connectionId, message.userId);
        } else if (message.type == "syncWithState") {
            if (!message.hasOwnProperty("commands")) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": syncWithState requires state param!");
                return false;
            }

            this.syncWithState(message.commands, message.id);
        } else if (message.type == "keepAlive") {
            if (!message.hasOwnProperty("timeout")) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": keepAlive requires timeout param!");
                return false;
            }

            timeoutParam = message.timeout;
        } else {
            Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                 "\": invalid type value!");
            return false;
        }

        this.renewServerExpireTime(message.id, timeoutParam);

        return true;
    }

    userConnectionNewEvent(serverId, connectionId) {
        let serverData = this.getServer(serverId);
        if (serverData != null) {
            serverData.server.userConnectionNewEvent(connectionId);
        }
    }

    userConnectionAddField(serverId, connectionId, key, value) {
        let serverData = this.getServer(serverId);
        if (serverData != null) {
            serverData.server.userConnectionAddField(connectionId, key, value);
        }
    }

    userConnectionIdentificationEvent(serverId, connectionId, userId) {
        let serverData = this.getServer(serverId);
        if (serverData != null) {
            serverData.server.userConnectionIdentificationEvent(connectionId, userId);
        }
    }

    userConnectionSubscribe(serverId, connectionId, userId, channel) {
        let serverData = this.getServer(serverId);
        if (serverData != null) {
            serverData.server.userConnectionSubscribe(connectionId, userId, channel);
        }
    }

    userConnectionDestroyEvent(serverId, connectionId, userId) {
        let serverData = this.getServer(serverId);
        if (serverData != null) {
            serverData.server.userConnectionDestroyEvent(connectionId, userId);
        }
    }

    syncWithState(commands, id) {
        this.resetOrCreateServer(id);
        for (let command of commands) {
            let message = command;
            message.id = id;
            this.processMessage(message);
        }
    }

    parseMessage(message) {
        if (typeof message === "string") {
            try {
                message = JSON.parse(message);
            } catch (error) {
                Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                     "\": " + error);
                return null;
            }
        }
        if (!message.hasOwnProperty("id")) {
            Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                 "\": no id property");
            return null;
        }
        if (!message.hasOwnProperty("type")) {
            Glue.logger.critical("Establishment::MetadataServer: bad message \"" + JSON.stringify(message) +
                                 "\": no type property");
            return null;
        }
        return message;
    }
}

MetadataServer.KEEPALIVE_PHASE = {
    WAIT_FOR_NORMAL_KEEPALIVE: 0,
    WAIT_FOR_REQUESTED_KEEPALIVE: 1
};

module.exports = MetadataServer;
