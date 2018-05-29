import * as redis from "redis";

export interface IPriorityQueue<T> {
    length(channel: string): Promise<number>;
    isEmpty(channel: string): Promise<boolean>;
    insertWithPriority(channel: string, element: any, priority: number): Promise<void>;
    pullHighestPriority(channel: string): Promise<T>;
    peek(channel: string): Promise<T>;
}

export class RedisConfig {
    public host: string;
    public port: number;
    public db: number;
    public password: string;

    constructor(host: string, port: number, db?: number, password?: string) {
        this.host = host;
        this.port = port;
        this.db = db ? db : null;
        this.password = password ? password : null;
    }
}

export class RedisPriorityQueue implements IPriorityQueue<string> {
    protected client: redis.RedisClient;
    protected readonly DEFAULT_REDIS_HOST: string = "localhost";
    protected readonly DEFAULT_REDIS_PORT: number = 6379;

    constructor(config: RedisConfig, client?: redis.RedisClient) {
        if (client) {
            this.client = client;
        } else {
            // build properties for instantiating Redis
            const options: {[key: string]: any} = {
                host: config.host || this.DEFAULT_REDIS_HOST,
                port: config.port || this.DEFAULT_REDIS_PORT,
                retry_strategy: (status: any) => {
                    if (status.error && status.error.code === "ECONNREFUSED") {
                        // End reconnecting on a specific error and flush all commands with
                        // a individual error
                        return new Error("The server refused the connection");
                    }
                    if (status.total_retry_time > 1000 * 60 * 60) {
                        // End reconnecting after a specific timeout and flush all commands
                        // with a individual error
                        return new Error("Retry time exhausted");
                    }
                    if (status.attempt > 10) {
                        // End reconnecting with built in error
                        return undefined;
                    }
                    // reconnect after
                    return Math.min(status.attempt * 100, 3000);
                },
            };
            if (config.db) { options.db = config.db; }
            if (config.password) { options.password = config.password; }

            this.client = redis.createClient(options);
        }
    }

    public length(channel: string): Promise<number> {
        return new Promise((resolve, reject) => {
            if (typeof channel !== "string") {
                throw new TypeError("Channel parameter must be a string");
            }

            this.client.zcard(channel, (err: Error, reply: number) => {
                if (err !== null) {
                    reject(err);
                }

                resolve(reply);
            });
        });
    }

    public isEmpty(channel: string): Promise<boolean> {
        return new Promise((resolve, reject) => {
            if (typeof channel !== "string") {
                throw new TypeError("Channel parameter must be a string");
            }

            this.client.zcard(channel, (err: Error, reply: number) => {
                if (err !== null) {
                    reject(err);
                }

                resolve(reply === 0);
            });
        });
    }

    public insertWithPriority(channel: string, element: string, priority: number): Promise<void> {
        return new Promise((resolve, reject) => {
            if (typeof channel !== "string") {
                throw new TypeError("Channel parameter must be a string");
            }
            if (typeof element !== "string") {
                throw new TypeError("Element parameter must be a string");
            }
            if (typeof priority !== "number") {
                throw new TypeError("Priority parameter must be a number");
            }

            this.client.zincrby(channel, priority, element, (err: Error, reply: number) => {
                if (err !== null) {
                    reject(err);
                }

                resolve();
            });
        });
    }

    public pullHighestPriority(channel: string): Promise<string> {
        return new Promise((resolve, reject) => {
            if (typeof channel !== "string") {
                throw new TypeError("Channel parameter must be a string");
            }

            this.client.multi()
                .zrevrange(channel, 0, 0, (err: Error | null, reply: any) => {
                    if (err !== null) {
                        reject(err);
                    }

                    const member: string = reply && Object.keys(reply).length > 0 ? reply : "none";
                    this.client.zrem(channel, member);
                })
                .exec((err: Error, replies: any) => {
                    if (err !== null) {
                        reject(err);
                    }

                    const item = replies.length && replies.length > 0 ? replies[0].toString() : null;
                    resolve(item && Object.keys(item).length > 0 ? item : null);
                });
        });
    }

    public peek(channel: string): Promise<string> {
        return new Promise((resolve, reject) => {
            if (typeof channel !== "string") {
                throw new TypeError("Channel parameter must be a string");
            }

            this.client.zrevrange(channel, 0, 0, (err: Error, reply: any) => {
                if (err !== null) {
                    reject(err);
                }

                resolve(reply.length && reply.length > 0 ? reply[0] : null);
            });
        });
    }
}
