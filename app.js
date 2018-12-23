'use strict'

const MQTT_TOPIC = "/hvac/intesis"
const MQTT_STATE_TOPIC = "/stat" + MQTT_TOPIC
const MQTT_COMMAND_TOPIC = "/cmnd" + MQTT_TOPIC

const argv = require('yargs')
    .usage('Usage: $0 [--discover] --mqtt [mqtt url] [--wmp ip address(,ip address,...)]')
    .demandOption(['mqtt'])
    .argv;

let supplied_intesis_ips = [];

if (argv.wmp) {
    supplied_intesis_ips = argv.wmp.split(',');
}

const mqtt_url = argv.mqtt;

var winston = require('winston')

const logger = require('winston').createLogger({
    level: 'debug',
    format: winston.format.combine(
        winston.format.splat(),
        winston.format.simple()
    ),
    transports: [
        new winston.transports.Console()
    ]
});

Object.assign({}, {});

const mqtt = require('mqtt')
const wmp = require('./wmp');

//todo detect connection failures
let mqttClient = mqtt.connect(mqtt_url)
mqttClient.on('error', function (error) {
    logger.error("Error from mqtt broker: %v", error)
});
mqttClient.on('connect', function (connack) {
    logger.info("Connected to mqtt broker")
});

let runWMP2Mqtt = function (mqttClient, wmpclient) {
    wmpclient.on('update', function (data) {
        logger.debug('Sending to MQTT: ' + JSON.stringify(data));
        mqttClient.publish(MQTT_STATE_TOPIC + "/" + wmpclient.mac + "/settings/" + data.feature.toLowerCase(), data.value.toString().toLowerCase())
    });
}

let parseCommand = function (topic, payload) {
    // format of commands is /<topic>/<mac>/<area>/<feature> payload (for set only is value
    let rv = {};
    //strip prefix and split
    let parts = topic.substr(MQTT_COMMAND_TOPIC.length).replace(/^\/+/g, '').split("/");

    rv['mac'] = parts[0];

    switch (parts[1].toUpperCase()) {
        case "SETTINGS":
            rv['feature'] = parts[2]
            if (payload && payload.length > 0) {
                rv['command'] = "SET";
                rv['value'] = payload;
            } else {
                rv['command'] = "GET";
            }
            break;
        default:
            rv['command'] = parts[1];
    }

    return rv;
}

var runMqtt2WMP = function (mqttClient, wmpclientMap) {
    mqttClient.subscribe(MQTT_COMMAND_TOPIC + "/#")

    mqttClient.on('message', function (topic, message) {
        let cmd = parseCommand(topic, message);
        let wmpclient = wmpclientMap[cmd.mac];

        if (!wmpclient) {
            logger.warn("Cannot find WMP server with MAC " + cmd.mac + "! Ignoring...")
            return;
        }

        switch (cmd.command) {
            case "ID":
                wmpclient.id().then(function (data) {
                    logger.debug("published to mqtt: %", JSON.stringify(data))
                    mqttClient.publish(MQTT_STATE_TOPIC, JSON.stringify(data))
                });
                break;
            case "INFO":
                wmpclient.info().then(function (data) {
                    logger.debug("published to mqtt: %", JSON.stringify(data))
                    mqttClient.publish(MQTT_STATE_TOPIC, JSON.stringify(data))
                });
                break;
            case "GET":
                wmpclient.get(cmd.feature);
                break;
            case "SET":
                wmpclient.set(cmd.feature, cmd.value);
                break;
        }
    })
}

var macToClient = {};

let wmpConnect = function (ip) {
    //todo: prevent duplicate registrations
    wmp.connect(ip).then(function (wmpclient) {
        logger.info("Connected to WMP at IP " + ip + " with MAC " + wmpclient.mac);

        wmpclient.on('close', function () {
            logger.warn('WMP Connection closed');
        });

        macToClient[wmpclient.mac] = wmpclient

        runWMP2Mqtt(mqttClient, wmpclient)
    })
};

supplied_intesis_ips.map(function (ip) {
    wmpConnect(ip);
});

if (argv.discover) {
    wmp.discover(1000, function (data) {
        wmpConnect(data.ip);
    })
};


runMqtt2WMP(mqttClient, macToClient);