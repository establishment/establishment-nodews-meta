const fs = require("fs");
const path = require("path");

const {GCScheduler} = require("establishment-node-core");
const {Glue, RPCServer, Util} = require("establishment-node-service-core");

const MetadataServer = require("./MetadataServer.js6.js");
const DefaultConfig = require("./DefaultConfig.js6.js");

module.exports.run = (params) => {
    let config = null;
    if (params) {
        if (params.hasOwnProperty("config") && params.config != null) {
            config = params.config;
        } else if (params.hasOwnProperty("configFilePath") && params.configFilePath != null) {
            config = JSON.parse(fs.readFileSync(params.configFilePath, "utf8"));
        }
    }

    if (!config) {
        config = DefaultConfig();
    }

    Util.setMockMachineId(config.machineId.mockId);
    Util.setMachineIdScript(config.machineId.script);
    Glue.initLogger(config.logging);
    Glue.initRegistryKeeper(config.registryKeeper);
    Glue.initService(config.service);

    GCScheduler.configure(config.gc);
    GCScheduler.setLogger(Glue.logger);
    GCScheduler.start();

    let rpcServer = new RPCServer(config.rpcServer);
    rpcServer.start();

    rpcServer.on("stop", (params, rpcCallback) => {
        Glue.stop(params, rpcCallback);
    });

    let metadataServer = new MetadataServer(config.server);
    metadataServer.start();
};
