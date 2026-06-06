"use strict";

const MQTT_TOPIC = "/hvac/intesis";
const MQTT_STATE_TOPIC = "/stat" + MQTT_TOPIC;
const MQTT_COMMAND_TOPIC = "/cmnd" + MQTT_TOPIC;

const argv = require("yargs")
  .usage(
    "Usage: $0 [--discover] --mqtt [mqtt url] [--mqttuser user --mqttpass pass] [--wmp ip address(,ip address,...)] [--retain [true/false]] [--offmode]",
  )
  .demandOption(["mqtt"])
  .option("offmode", {
    describe:
      "Fold ONOFF into MODE topic for HA compatibility (default: false)",
    type: "boolean",
    default: false,
  }).argv;

let supplied_intesis_ips = [];

if (argv.wmp) {
  supplied_intesis_ips = argv.wmp.split(",");
}

let retain_flag = argv.retain === "true" ? true : false;
const offmode_enabled = !!(argv.offmode || process.env.OFFMODE === "true");

const mqtt_url = argv.mqtt;

var winston = require("winston");

const logger = winston.createLogger({
  level: "debug",
  format: winston.format.combine(
    winston.format.splat(),
    winston.format.simple(),
  ),
  transports: [new winston.transports.Console()],
});

const options = {};
if (argv.mqttuser && argv.mqttpass) {
  options.username = argv.mqttuser;
  options.password = argv.mqttpass;
}

console.log("options", { options });

const mqtt = require("mqtt");
const wmp = require("./wmp");

//todo detect connection failures
let mqttClient = mqtt.connect(mqtt_url, options);
mqttClient.on("error", function (error) {
  logger.error("Error from mqtt broker: %v", error);
});
mqttClient.on("connect", function (connack) {
  logger.info("Connected to mqtt broker");
});

let runWMP2Mqtt = function (mqttClient, wmpclient) {
  if (offmode_enabled) {
    deviceState[wmpclient.mac] = {
      onoff: null,
      mode: null,
      pendingMode: null,
    };
  }

  wmpclient.on("update", function (data) {
    logger.debug("Sending to MQTT: " + JSON.stringify(data));
    const feature = data.feature.toLowerCase();
    const value = data.value.toString().toLowerCase();
    const statBase = MQTT_STATE_TOPIC + "/" + wmpclient.mac + "/settings/";

    if (offmode_enabled) {
      const state = deviceState[wmpclient.mac];
      if (feature === "onoff") {
        state.onoff = value;
        mqttClient.publish(statBase + "onoff", value, { retain: retain_flag });
        if (value === "off") {
          mqttClient.publish(statBase + "mode", "off", {
            retain: retain_flag,
          });
        } else {
          if (state.pendingMode !== null) {
            const modeToSet = state.pendingMode.toUpperCase();
            state.mode = state.pendingMode;
            state.pendingMode = null;
            logger.debug(
              "offmode: ONOFF=ON confirmed for " +
                wmpclient.mac +
                ", scheduling MODE=" +
                modeToSet +
                " after settle delay",
            );
            setTimeout(() => wmpclient.set("MODE", modeToSet), 1000);
          }
          if (state.mode !== null) {
            mqttClient.publish(statBase + "mode", state.mode, {
              retain: retain_flag,
            });
          }
        }
      } else if (feature === "mode") {
        state.mode = value;
        if (state.onoff !== null) {
          const effectiveMode = state.onoff === "off" ? "off" : state.mode;
          mqttClient.publish(statBase + "mode", effectiveMode, {
            retain: retain_flag,
          });
        }
      } else {
        mqttClient.publish(statBase + feature, value, { retain: retain_flag });
      }
    } else {
      mqttClient.publish(statBase + feature, value, { retain: retain_flag });
    }
  });
};

let parseCommand = function (topic, payload) {
  // format of commands is /<topic>/<mac>/<area>/<feature> payload (for set only is value
  let rv = {};
  //strip prefix and split
  let parts = topic
    .substr(MQTT_COMMAND_TOPIC.length)
    .replace(/^\/+/g, "")
    .split("/");

  rv["mac"] = parts[0];

  switch (parts[1].toUpperCase()) {
    case "SETTINGS":
      rv["feature"] = parts[2];
      if (payload && payload.length > 0) {
        rv["command"] = "SET";
        rv["value"] = payload;
      } else {
        rv["command"] = "GET";
      }
      break;
    default:
      rv["command"] = parts[1];
  }

  return rv;
};

var runMqtt2WMP = function (mqttClient, wmpclientMap) {
  mqttClient.subscribe(MQTT_COMMAND_TOPIC + "/#");

  mqttClient.on("message", function (topic, message) {
    let cmd = parseCommand(topic, message);
    let wmpclient = wmpclientMap[cmd.mac];

    if (!wmpclient) {
      logger.warn(
        "Cannot find WMP server with MAC " + cmd.mac + "! Ignoring...",
      );
      return;
    }

    switch (cmd.command) {
      case "ID":
        wmpclient.id().then(function (data) {
          logger.debug("published to mqtt: %", JSON.stringify(data));
          mqttClient.publish(MQTT_STATE_TOPIC, JSON.stringify(data), {
            retain: retain_flag,
          });
        });
        break;
      case "INFO":
        wmpclient.info().then(function (data) {
          logger.debug("published to mqtt: %", JSON.stringify(data));
          mqttClient.publish(MQTT_STATE_TOPIC, JSON.stringify(data), {
            retain: retain_flag,
          });
        });
        break;
      case "GET":
        wmpclient.get(cmd.feature);
        break;
      case "SET":
        if (offmode_enabled && cmd.feature.toLowerCase() === "mode") {
          if (cmd.value.toString().toLowerCase() === "off") {
            wmpclient.set("ONOFF", "OFF");
          } else {
            const state = deviceState[cmd.mac];
            const modeValue = cmd.value.toString();
            if (state && state.onoff === "on") {
              logger.debug(
                "offmode: unit already on, sending MODE=" +
                  modeValue +
                  " to " +
                  cmd.mac,
              );
              wmpclient.set("MODE", modeValue);
            } else {
              logger.debug(
                "offmode: unit off/unknown (onoff=" +
                  (state && state.onoff) +
                  "), queuing pendingMode=" +
                  modeValue +
                  " for " +
                  cmd.mac +
                  ", sending ONOFF=ON",
              );
              if (state) state.pendingMode = modeValue;
              wmpclient.set("ONOFF", "ON");
            }
          }
        } else {
          wmpclient.set(cmd.feature, cmd.value);
        }
        break;
    }
  });

  let keepalive = setInterval(function () {
    try {
      let wmpclients = Object.keys(wmpclientMap);
      wmpclients.forEach(function (mac) {
        logger.info("keepalive: keeping alive MAC " + mac);
        let wmpclient = wmpclientMap[mac];
        wmpclient.id().then(function (data) {
          wmpclient.get('ONOFF');
          wmpclient.get('MODE');
          wmpclient.get('AMBTEMP');
        });
      });
    } catch (err) {
      logger.warn(err);
      logger.warn("Failure in keepalive (connection dead?)");
    }
  }, 30000);
};

var macToClient = {};
let deviceState = {}; // { [mac]: { onoff: null, mode: null } } — used by offmode feature

let wmpConnect = function (ip) {
  //todo: prevent duplicate registrations
  wmp.connect(ip).then(function (wmpclient) {
    logger.info("Connected to WMP at IP " + ip + " with MAC " + wmpclient.mac);

    wmpclient.on("close", function () {
      logger.warn(
        "WMP Connection closed! Closing MQTT connection and exiting...",
      );
      mqttClient.end(false, {}, () => process.exit(-1));
    });

    macToClient[wmpclient.mac] = wmpclient;

    runWMP2Mqtt(mqttClient, wmpclient);
  });
};

supplied_intesis_ips.map(function (ip) {
  wmpConnect(ip);
});

const DISCOVER_WAIT = 10; //seconds

let doDiscover = function () {
  wmp.discover(
    1000,
    function (data) {
      logger.info("Discovered");
      wmpConnect(data.ip);
    },
    function () {
      if (Object.keys(macToClient).length === 0) {
        logger.info(
          "Nothing connected, retrying discovery in " +
            DISCOVER_WAIT +
            " seconds..",
        );
        setTimeout(doDiscover, DISCOVER_WAIT * 1000);
      }
    },
  );
};

if (argv.discover) {
  doDiscover();
}

runMqtt2WMP(mqttClient, macToClient);
