# Qolsys Controller

[![Build](https://github.com/EHylands/QolsysController/actions/workflows/build.yml/badge.svg)](https://github.com/EHylands/QolsysController/actions/workflows/build.yml)

A Python module that emulates a virtual IQ Remote device, enabling full **local control** of a Qolsys IQ Panel over MQTT — no cloud access required.

## QolsysController
- ✅ Connects directly to the **Qolsys Panel's local MQTT server as an IQ Remote**
- 🔐 Pairs by only using **Installer Code** (same procedure as standard IQ Remote pairing)
- 🔢 Supports **4-digit user codes**
- ⚠️ Uses a **custom local usercode database** — panel's internal user code verification process is not yet supported 
- 🌐 Includes a built-in MQTT bridge/broker for panel updates and incoming commands

## MQTT Bridge Quick Reference
- Default root topic: `qolsys_panel/v1/home`
- State topics:
  - `.../panel/status`
  - `.../panel/settings`
  - `.../partition/<partition_id>`
  - `.../zone/<zone_id>`
  - `.../automation/<virtual_node_id>`
- Command topics:
  - `.../panel/command`
  - `.../partition/command`
  - `.../automation/command`
- Supported panel commands: `execute_scene`, `trigger_police`, `trigger_auxilliary`, `trigger_fire`, `speak`
- Supported automation commands: locks, lights, covers/garage doors, sirens, valves, thermostats, and fan mode / setpoint control

## HTTP Bridge API
The MQTT bridge also exposes a tiny local HTTP server on `127.0.0.1:9123` by default.

Supported GET endpoints:
- `/health`, returns bridge health plus `connected`, `paired`, and `ca_ready`
- `/mqtt-bridge/ca`, returns the generated CA certificate when available

Example CA bootstrap:

```bash
curl --fail http://127.0.0.1:9123/mqtt-bridge/ca -o mqtt_bridge_ca.cer
```

This HTTP surface is used for CA bootstrap and health checks, not for panel command traffic.

## Functionality Highlights

| Category               | Feature                              | Status |
|------------------------|--------------------------------------|--------|
| **Panel**              | Diagnostic Sensors                   | ✅     |
|                        | Panel Scenes                         | ✅     |
|                        | Speak Command                        | ✅     |
|                        | Weather Forecast                     | ✅     |
| **Partition**          | Arming Status and Alarm State        | ✅     |
|                        | Home Instant Arming                  | ✅     |
|                        | Home Silent Disarming (Firmware 4.6.1)| ✅     |
|                        | Set Exit Sounds and Entry Delay      | ✅     |
| **Zones**              | Sensor Status                        | ✅     |
|                        | Tamper State                         | ✅     |
|                        | Battery Level                        | ✅     |
|                        | Temperature (supported PowerG device)| ✅     |
|                        | Light (supported PowerG device)      | ✅     |
|                        | Average and Latest dBm               | ✅     |


| Automation Devices| Z-Wave | PowerG | Alarm.com |
|-----------------|--------|--------|-------|
| Door Lock        | ✅     | ✅     | ❌    |
| Energy Clamp     | ✅     | ❌     | ❌    |
| External Siren   | ✅     | ❌     | ❌    |
| Garage Door      | ✅     | ❌     | ✅    |
| Lights           | ✅     | 🛠️     | ✅    |
| Smart Outlet.    | 🛠️     | ❌     | ❌    |
| Thermometer      | ✅     | ❌     | ❌    |
| Thermostat       | ✅     | ❌     | ❌    |
| Water Valve      | 🛠️     | ❌     | ❌    |


## 📦 Installation

```bash
pip install qolsys-controller
python3.12 qolsys-controller --verbose --config 'path_to_config_file'
```

```json config.json
# config.json
{
  "panel_ip": "IQ Panel IP",
  "panel_mac": "",
  "random_mac": "cc4b73865c89",
  "config_dir": "",
  "plugin_ip": "",
  "auto_discover_pki": false,
  "start_pairing": true,
  "pairing_resume": true,
  "check_user_code_on_arm": false,
  "check_user_code_on_disarm": false,
  "log_mqtt_messages": false,
  "mqtt_bridge": true,
  "mqtt_bridge_allow_anonymous": false,
  "mqtt_bridge_username": "bridge",
  "mqtt_bridge_password": "change-this-password",
  "mqtt_bridge_allowed_users": {
    "homeassistant": "another-strong-password"
  }
}
```

The MQTT bridge publishes under `qolsys_panel/v1/home/...` by default and starts automatically when `mqtt_bridge` is true.

### MQTT Bridge Security Defaults

- Anonymous MQTT access is now disabled by default.
- Configure either:
  - `mqtt_bridge_username` + `mqtt_bridge_password`, and/or
  - `mqtt_bridge_allowed_users` (map of username to password).
- Anonymous access is only enabled when `mqtt_bridge_allow_anonymous` is explicitly set to `true`.
- The bridge HTTP API binds to `127.0.0.1` and is intended for local CA bootstrap and health checks.

## ⚠️ Certificate Warning

During pairing, the main panel issues **only one signed client certificate** per virtual IQ Remote. If any key files are lost or deleted, re-pairing may become impossible. 

A new PKI, including a new private key, can be recreated under specific circumstances, though the precise conditions remain unknown at this time.

**Important:**  
Immediately back up the following files from the `pki/` directory after initial pairing:

- `.key` (private key)
- `.cer` (certificate)
- `.csr` (certificate signing request)
- `.secure` (signed client certificate)
- `.qolsys` (Qolsys Panel public certificate)

Store these files securely.
