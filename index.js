"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var redis = require("redis");
var RedisConfig = (function () {
    function RedisConfig(host, port, db, password) {
        this.host = host;
        this.port = port;
        this.db = db ? db : null;
        this.password = password ? password : null;
    }
    return RedisConfig;
}());
exports.RedisConfig = RedisConfig;
var RedisPriorityQueue = (function () {
    function RedisPriorityQueue(config, client) {
        this.DEFAULT_REDIS_HOST = "localhost";
        this.DEFAULT_REDIS_PORT = 6379;
        if (client) {
            this.client = client;
        }
        else {
            var options = {
                host: config.host || this.DEFAULT_REDIS_HOST,
                port: config.port || this.DEFAULT_REDIS_PORT,
                retry_strategy: function (status) {
                    if (status.error && status.error.code === "ECONNREFUSED") {
                        return new Error("The server refused the connection");
                    }
                    if (status.total_retry_time > 1000 * 60 * 60) {
                        return new Error("Retry time exhausted");
                    }
                    if (status.attempt > 10) {
                        return undefined;
                    }
                    return Math.min(status.attempt * 100, 3000);
                },
            };
            if (config.db) {
                options.db = config.db;
            }
            if (config.password) {
                options.password = config.password;
            }
            this.client = redis.createClient(options);
        }
    }
    RedisPriorityQueue.prototype.length = function (channel) {
        var _this = this;
        return new Promise(function (resolve, reject) {
            if (typeof channel !== "string") {
                throw new TypeError("Channel parameter must be a string");
            }
            _this.client.zcard(channel, function (err, reply) {
                if (err !== null) {
                    reject(err);
                }
                resolve(reply);
            });
        });
    };
    RedisPriorityQueue.prototype.isEmpty = function (channel) {
        var _this = this;
        return new Promise(function (resolve, reject) {
            if (typeof channel !== "string") {
                throw new TypeError("Channel parameter must be a string");
            }
            _this.client.zcard(channel, function (err, reply) {
                if (err !== null) {
                    reject(err);
                }
                resolve(reply === 0);
            });
        });
    };
    RedisPriorityQueue.prototype.insertWithPriority = function (channel, element, priority) {
        var _this = this;
        return new Promise(function (resolve, reject) {
            if (typeof channel !== "string") {
                throw new TypeError("Channel parameter must be a string");
            }
            if (typeof element !== "string") {
                throw new TypeError("Element parameter must be a string");
            }
            if (typeof priority !== "number") {
                throw new TypeError("Priority parameter must be a number");
            }
            _this.client.zincrby(channel, priority, element, function (err, reply) {
                if (err !== null) {
                    reject(err);
                }
                resolve();
            });
        });
    };
    RedisPriorityQueue.prototype.pullHighestPriority = function (channel) {
        var _this = this;
        return new Promise(function (resolve, reject) {
            if (typeof channel !== "string") {
                throw new TypeError("Channel parameter must be a string");
            }
            _this.client.multi()
                .zrevrange(channel, 0, 0, function (err, reply) {
                if (err !== null) {
                    reject(err);
                }
                var member = reply && Object.keys(reply).length > 0 ? reply : "none";
                _this.client.zrem(channel, member);
            })
                .exec(function (err, replies) {
                if (err !== null) {
                    reject(err);
                }
                var item = replies.length && replies.length > 0 ? replies[0].toString() : null;
                resolve(item && Object.keys(item).length > 0 ? item : null);
            });
        });
    };
    RedisPriorityQueue.prototype.peek = function (channel) {
        var _this = this;
        return new Promise(function (resolve, reject) {
            if (typeof channel !== "string") {
                throw new TypeError("Channel parameter must be a string");
            }
            _this.client.zrevrange(channel, 0, 0, function (err, reply) {
                if (err !== null) {
                    reject(err);
                }
                resolve(reply.length && reply.length > 0 ? reply[0] : null);
            });
        });
    };
    return RedisPriorityQueue;
}());
exports.RedisPriorityQueue = RedisPriorityQueue;
//# sourceMappingURL=index.js.map
