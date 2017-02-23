const {RedisConnectionPool} = require("establishment-node-core");
const {Glue} = require("establishment-node-service-core");

class MetadataObserver {
    constructor(config) {
        this.redisConnection = RedisConnectionPool.getSharedConnection(config.redis.address);

        this.connectionIdsSet = config.redis.set.connectionIds;
        this.userIdsSet = config.redis.set.userIds;
        this.streamsSet = config.redis.set.streams;
        this.userIdToConnectionIdPrefix = config.redis.prefix.userIdToConnectionId;

        this.userIdToStreamsPrefix = config.redis.prefix.userIdToStreams;
        this.connectionIdToUserIdPrefix = config.redis.prefix.connectionIdToUserId;
        this.connectionIdToStreamsPrefix = config.redis.prefix.connectionIdToStreams;
        this.streamToConnectionIdsPrefix = config.redis.prefix.streamToConnectionIds;
        this.streamToUserIdsPrefix = config.redis.prefix.streamToUserIds;
        this.streamToUserIdConnectionCounterPrefix = config.redis.prefix.streamToUserIdConnectionCounter;
        this.connectionIdToDataPrefix = config.redis.prefix.connectionIdToData;

        this.streamEventsStream = config.redis.stream.streamEvents;

        this.guestConnectionsKey = config.redis.key.guestConnections;

        this.guestConnectionCounter = 0;

        this.redisConnection.on("error", (err) => {
            Glue.logger.error("Establishment::MetadataObserver redis connection: " + err);
        });
    }

    clearConnectionIds(callback, errback) {
        this.redisConnection.smembers(this.connectionIdsSet, (error, ids) => {
            if (error != null) {
                errback(error);
                return;
            }

            for (let id of ids) {
                this.redisConnection.del(this.connectionIdToStreamsPrefix + id);
                this.redisConnection.del(this.connectionIdToUserIdPrefix + id);
                this.redisConnection.del(this.connectionIdToDataPrefix + id);
            }

            this.redisConnection.del(this.connectionIdsSet);

            callback();
        });
    }

    clearUserIds(callback, errback) {
        this.redisConnection.smembers(this.userIdsSet, (error, ids) => {
            if (error != null) {
                errback(error);
                return;
            }

            for (let id of ids) {
                this.redisConnection.del(this.userIdToConnectionIdPrefix + id);
                this.redisConnection.del(this.userIdToStreamsPrefix + id);
            }

            this.redisConnection.del(this.userIdsSet);

            callback();
        });
    }

    clearStreams(callback, errback) {
        this.redisConnection.smembers(this.streamsSet, (error, streams) => {
            if (error != null) {
                errback(error);
                return;
            }

            for (let stream of streams) {
                this.redisConnection.del(this.streamToConnectionIdsPrefix + stream);
                this.redisConnection.del(this.streamToUserIdConnectionCounterPrefix + stream);
                this.redisConnection.del(this.streamToUserIdsPrefix + stream);
            }

            this.redisConnection.del(this.streamsSet);

            callback();
        });
    }

    reset(callback, errback) {
        this.redisConnection.del(this.guestConnectionsKey);
        this.redisConnection.set(this.guestConnectionsKey, 0);
        this.clearUserIds(() => {
            Glue.logger.info("Establishment::MetadataObserver: done clearing per user data");
            this.resetPart2(callback, errback);
        }, () => {
            Glue.logger.critical("Establishment::MetadataObserver: error clearing " + this.userIdsSet + " set: " +
                                 error);
            errback();
        });
    }

    resetPart2(callback, errback) {
        this.clearConnectionIds(() => {
            Glue.logger.info("Establishment::MetadataObserver: done clearing per connection data");
            this.resetPart3(callback, errback);
        }, () => {
            Glue.logger.critical("Establishment::MetadataObserver: error clearing " + this.connectionIdsSet + " set: " +
                                 error);
            errback();
        });
    }

    resetPart3(callback, errback) {
        this.clearStreams(() => {
            Glue.logger.info("Establishment::MetadataObserver: done clearing per stream data");
            callback();
        }, () => {
            Glue.logger.critical("Establishment::MetadataObserver: error clearing " + this.streamsSet + " set: " +
                                 error);
            errback();
        });
    }

    userConnectionNewEvent(connectionId) {
        this.redisConnection.sadd(this.connectionIdsSet, connectionId);
    }

    userConnectionAddField(connectionId, name, value) {
        if (name == null) {
            Glue.logger.critical("Establishment::MetadataObserver: userConnectionAddField name is null!");
            return;
        }
        if (name == undefined) {
            Glue.logger.critical("Establishment::MetadataObserver: userConnectionAddField name is undefined!");
            return;
        }
        if (value == null) {
            Glue.logger.warning("Establishment::MetadataObserver: userConnectionAddField value for field '" + name +
                                "' is null!");
            return;
        }
        if (value == null) {
            Glue.logger.warning("Establishment::MetadataObserver: userConnectionAddField value for field '" + name +
                                "' is undefined!");
            return;
        }
        this.redisConnection.hset(this.connectionIdToDataPrefix + connectionId, name, value, (error, reply) => {
            if (error != null) {
                Glue.logger.error("Establishment::MetadataObserver: error setting hash field: " + error);
            }
        });
    }

    userConnectionIdentificationEvent(connectionId, userId) {
        this.redisConnection.sadd(this.userIdsSet, userId);
        this.redisConnection.set(this.connectionIdToUserIdPrefix + connectionId, userId);
        this.redisConnection.sadd(this.userIdToConnectionIdPrefix + userId, connectionId);

        if (userId == 0) {
            this.redisConnection.incr(this.guestConnectionsKey, (error, reply) => {
                if (error != null) {
                    Glue.logger.error("Establishment::MetadataObserver: error increasing guest connection counter: " +
                                      error);
                    return;
                }
                this.guestConnectionCounter = reply;
            });
        }

        this.sendUpdateRequest();
    }

    userConnectionSubscribe(connectionId, userId, stream) {
        this.redisConnection.sadd(this.streamsSet, stream);
        this.redisConnection.sadd(this.streamToConnectionIdsPrefix + stream, connectionId);
        this.redisConnection.sadd(this.connectionIdToStreamsPrefix + connectionId, stream);

        this.redisConnection.hincrby(this.streamToUserIdConnectionCounterPrefix + stream, "user-" + userId, 1,
                                     (error, reply) => {
            if (reply == 1) {
                this.userJoinedStreamEvent(userId, stream);
            }
        });

        this.sendUpdateRequest();
    }

    userConnectionDestroyEvent(connectionId, userId) {
        this.redisConnection.srem(this.connectionIdsSet, connectionId);

        if (userId == 0) {
            this.redisConnection.decr(this.guestConnectionsKey, (error, reply) => {
                if (error != null) {
                    Glue.logger.error("Establishment::MetadataObserver: error decreasing guest connection counter: " +
                                      error);
                    return;
                }
                this.guestConnectionCounter = reply;
            });
        }

        // delete connectionId to stream
        this.redisConnection.smembers(this.connectionIdToStreamsPrefix + connectionId, (error, streams) => {
            if (error != null) {
                Glue.logger.critical("Establishment::MetadataObserver: error getting streams from connectionId: " +
                                     error);
                return;
            }
            if (streams == null) {
                return;
            }
            this.redisConnection.del(this.connectionIdToStreamsPrefix + connectionId);
            for (let stream of streams) {
                this.redisConnection.srem(this.streamToConnectionIdsPrefix + stream, connectionId);
                this.redisConnection.smembers(this.streamToConnectionIdsPrefix + stream, (error, reply) => {
                    if (reply == null || reply.length == 0 || reply.toString() == "") {
                        this.streamNoLongerUsedEvent(stream);
                    }
                });

                if (userId != -1) {
                    this.redisConnection.hincrby(this.streamToUserIdConnectionCounterPrefix + stream, "user-" + userId,
                                                 -1, (error, reply) => {
                        if (reply == 0) {
                            this.redisConnection.hdel(this.streamToUserIdConnectionCounterPrefix + stream, "user-" +
                                                      userId);
                            this.redisConnection.hlen(this.streamToUserIdConnectionCounterPrefix + stream,
                                                      (error, reply) => {
                                if (reply == 0) {
                                    this.redisConnection.del(this.streamToUserIdConnectionCounterPrefix + stream);
                                }
                            });
                            this.userLeftStreamEvent(userId, stream);
                        }
                    });
                }
            }

            this.sendUpdateRequest();
        });

        // delete connectionId to userId
        this.redisConnection.get(this.connectionIdToUserIdPrefix + connectionId, (error, userId) => {
            if (error != null) {
                Glue.logger.critical("Establishment::MetadataObserver: error getting userId from connectionId: " + error);
                return;
            }
            if (userId == null) {
                return;
            }
            this.redisConnection.del(this.connectionIdToUserIdPrefix + connectionId);
            if (userId != -1) {
                this.redisConnection.srem(this.userIdToConnectionIdPrefix + userId, connectionId);
                this.redisConnection.smembers(this.userIdToConnectionIdPrefix + userId, (error, reply) => {
                    if (reply == null || reply.length == 0 || reply.toString() == "") {
                        this.userNoLongerConnectedEvent(userId);
                    }
                });
            }

            this.sendUpdateRequest();
        });

        this.redisConnection.del(this.connectionIdToDataPrefix + connectionId);
    }

    userLeftStreamEvent(userId, stream) {
        // We don't care about guests meta-data, yet (deleting this will have implication in UI)
        if (userId == 0) {
            return;
        }

        Glue.logger.info("Establishment::MetadataObserver: user " + userId + " left #" + stream);

        this.redisConnection.srem(this.streamToUserIdsPrefix + stream, userId);
        this.redisConnection.srem(this.userIdToStreamsPrefix + userId, stream);

        this.sendLeftStreamEvent(userId, stream);
    }

    userJoinedStreamEvent(userId, stream) {
        // We don't care about guests meta-data, yet (deleting this will have implication in UI)
        if (userId == 0) {
            return;
        }

        Glue.logger.info("Establishment::MetadataObserver: user " + userId + " joined #" + stream);

        this.redisConnection.sadd(this.streamToUserIdsPrefix + stream, userId);
        this.redisConnection.sadd(this.userIdToStreamsPrefix + userId, stream);

        this.sendJoinedStreamEvent(userId, stream);
    }

    streamNoLongerUsedEvent(stream) {
        this.redisConnection.srem(this.streamsSet, stream);
        this.redisConnection.del(this.streamToConnectionIdsPrefix + stream);
        this.redisConnection.del(this.streamToUserIdsPrefix + stream);
    }

    userNoLongerConnectedEvent(userId) {
        this.redisConnection.srem(this.userIdsSet, userId);
        this.redisConnection.del(this.userIdToConnectionIdPrefix + userId);
        this.redisConnection.del(this.userIdToStreamsPrefix + userId);
    }

    sendUpdateRequest() {
        if (Glue.registryKeeper.get("enable-full-stream-update") == "true") {
            let request = {
                "command": "fullUpdate"
            };
            this.redisConnection.publish(this.streamEventsStream, JSON.stringify(request));
        }
    }

    sendLeftStreamEvent(userId, stream) {
        if (Glue.registryKeeper.get("enable-stream-events") == "true") {
            let request = {
                "command": "streamEvent",
                "event": "left",
                "userId": userId.toString(),
                "stream": stream
            };
            this.redisConnection.publish(this.streamEventsStream, JSON.stringify(request));
        }
    }

    sendJoinedStreamEvent(userId, stream) {
        if (Glue.registryKeeper.get("enable-stream-events") == "true") {
            let request = {
                "command": "streamEvent",
                "event": "joined",
                "userId": userId.toString(),
                "stream": stream
            };
            this.redisConnection.publish(this.streamEventsStream, JSON.stringify(request));
        }
    }

    getTotalGuestConnections() {
        return this.guestConnectionCounter;
    }

    static getDummyAPI() {
        return {
            userConnectionNewEvent: function(){},
            userConnectionAddField: function(){},
            userConnectionIdentificationEvent: function(){},
            userConnectionSubscribe: function(){},
            userConnectionDestroyEvent: function(){},

            userLeftStreamEvent: function(){},
            userJoinedStreamEvent: function(){},
            streamNoLongerUsedEvent: function() {},
            userNoLongerConnectedEvent: function(){},
            sendUpdateRequest: function(){},
            sendLeftStreamEvent: function(){},
            sendJoinedStreamEvent: function(){},

            getTotalGuestConnections: function() {},
            isDummy: true
        };
    }
}

module.exports = MetadataObserver;