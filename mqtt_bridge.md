# MQTT Bridge API Specification

Default bridge base topic: `qolsys_panel/v1/home`

If `friendly_name` is blank, the panel unique id is used instead of `home`.

## Security

- MQTT transport uses TLS with a local bridge CA and broker certificate created under `config/mqtt_bridge/`.
- Export the CA certificate via `GET /mqtt-bridge/ca` on the local HTTP API (`127.0.0.1:9123` by default).
- Anonymous MQTT access is **disabled by default**.
- To authenticate, configure:
  - `mqtt_bridge_username` + `mqtt_bridge_password`, and/or
  - `mqtt_bridge_allowed_users` as a username/password map.
- Set `mqtt_bridge_allow_anonymous: true` only for explicit local/trusted-network use cases.

## Topics

### Status Topics
- `root_topic/v1/friendly_name/panel/status`
- `root_topic/v1/friendly_name/panel/settings`

### State Topics
- `root_topic/v1/friendly_name/automation/<virtual_node_id>`
- `root_topic/v1/friendly_name/partition/<partition_id>`
- `root_topic/v1/friendly_name/zone/<zone_id>`

### Command Topics
- `root_topic/v1/friendly_name/automation/command`
- `root_topic/v1/friendly_name/partition/command`
- `root_topic/v1/friendly_name/panel/command`

## State Payloads

### Zone State Payload
```json
{
  "id": 1,
  "type": "zone",
  "state": {
    "status": "ACTIVE",
    "battery": "Normal",
    "ac": "On",
    "average_dbm": -50,
    "latest_dbm": -50,
    "powerg_temperature": "",
    "powerg_light": "",
    "powerg_battery_level": "",
    "powerg_battery_voltage": ""
  },
  "capabilities": {
    "ac": false,
    "battery": true,
    "average_dbm": false,
    "latest_dbm": false,
    "powerg": false,
    "powerg_temperature": false,
    "powerg_light": false,
    "powerg_battery_level": false,
    "powerg_battery_voltage": false
  },
  "attributes": {
    "name": "IQ Remote 1",
    "device_type": "KEYPAD",
    "partition_id": 0,
    "group": "FIXED_INTRUSION"
  },
  "timestamp": "2026-04-07T00:31:42.396363Z",
  "version": 1
}
```

`status` values include:
`ACTIVE`, `ACTIVATED`, `ALARMED`, `ARM_AWAY`, `ARM_STAY`, `AUXILIARY_EMERGENCY`, `BELL_TROUBLE`, `CLOSED`, `CONNECTED`, `DISARM`, `FAILURE`, `FIRE_EMERGENCY`, `OPEN`, `OCCUPIED`, `POLICE_EMERGENCY`, `INACTIVE`, `IDLE`, `NORMAL`, `NOT_NETWORKED`, `UNREACHABLE`, `SILENT_POLICE_EMERGENCY`, `SILENT_AUXILIARY_EMERGENCY`, `TAMPERED`, `SYNCHRONIZING`, `DISCONNECTED`, `TROUBLE`, `VACANT`

### Partition State Payload
```json
{
  "id": 0,
  "type": "partition",
  "state": {
    "status": "DISARM",
    "alarm_state": "NONE",
    "alarm_array": [],
    "status_changed_time": 1756141094939,
    "entry_delays": true,
    "exit_sounds": true
  },
  "attributes": {
    "name": "systemo"
  },
  "timestamp": "2026-04-07T00:32:27.859480Z",
  "version": 1
}
```

### Automation Device State Payload
```json
{
  "id": 7,
  "type": "automation_device",
  "state": {
    "services": [
      {
        "type": "LightService",
        "state": { "is_on": false, "level": 85 },
        "attributes": { "endpoint": 0 },
        "capabilities": { "supports_level": true }
      },
      {
        "type": "StatusService",
        "state": { "is_malfunctioning": true },
        "attributes": { "endpoint": 0 },
        "capabilities": { "supports_status": true }
      },
      {
        "type": "BatteryService",
        "state": { "is_disabled": true },
        "attributes": { "endpoint": 0 },
        "capabilities": { "supports_battery_level": false, "supports_battery_low": false }
      }
    ]
  },
  "attributes": {
    "protocol": "ZWAVE",
    "name": "Light",
    "type": "Light"
  },
  "timestamp": "2026-04-07T02:08:28.749064+00:00",
  "version": 1
}
```

`status` values include:
`DISARM`, `ARM_STAY`, `ARM_AWAY`, `ARM_NIGHT`, `ARM_AWAY_EXIT_DELAY`, `ARM_STAY_EXIT_DELAY`, `ARM_NIGHT_EXIT_DELAY`, `UNKNOWN`

`alarm_state` values include:
`NONE`, `DELAY`, `ALARM`, `UNKNOWN`

`alarm_array` values include:
`POLICE_EMERGENCY`, `SILENT_POLICE_EMERGENCY`, `FIRE_EMERGENCY`, `GAZ_CO`, `AUXILIARY_EMERGENCY`, `SILENT_AUXILIARY_EMERGENCY`, `WATER_SENSOR`

## Commands

### Partition Commands

#### DISARM
```json
{
  "command": "DISARM",
  "command_id": "46707d92-02f4-4817-8116-a4c3b23e6266",
  "partition_id": 0,
  "user_code": "1234",
  "silent_disarm": false,
  "response_topic": "per_client_unique_response_topic",
  "version": 1
}
```

#### ARM_AWAY
```json
{
  "command": "ARM_AWAY",
  "command_id": "46707d92-02f4-4817-8116-a4c3b23e6266",
  "partition_id": 0,
  "user_code": "1234",
  "exit_sounds": false,
  "response_topic": "per_client_unique_response_topic",
  "version": 1
}
```

#### ARM_STAY
```json
{
  "command": "ARM_STAY",
  "command_id": "46707d92-02f4-4817-8116-a4c3b23e6266",
  "partition_id": 0,
  "user_code": "1234",
  "exit_delay": true,
  "exit_sounds": false,
  "instant_arm": false,
  "response_topic": "per_client_unique_response_topic",
  "version": 1
}
```

#### ARM_NIGHT
```json
{
  "command": "ARM_NIGHT",
  "command_id": "46707d92-02f4-4817-8116-a4c3b23e6266",
  "partition_id": 0,
  "user_code": "1234",
  "exit_sounds": false,
  "response_topic": "per_client_unique_response_topic",
  "version": 1
}
```

### Automation Device Commands

```json
{
  "command": "light_off",
  "command_id": "46707d92-02f4-4817-8116-a4c3b23e6266",
  "virtual_node_id": 8,
  "endpoint": 0,
  "response_topic": "per_client_unique_response_topic",
  "version": 1
}
```

Supported `command` values:
- `light_on`
- `light_off`
- `light_level`
- `lock`
- `unlock`
- `cover_open`
- `cover_close`
- `cover_position`
- `siren_on`
- `siren_off`
- `valve_open`
- `valve_close`
- `valve_stop`
- `valve_position`
- `thermostat_mode`
- `thermostat_fan_mode`
- `thermostat_heat`
- `thermostat_cool`

### Panel Commands

Supported `command` values:
- `execute_scene` (`scene_id` required)
- `trigger_police` (`partition_id`, optional `silent`)
- `trigger_auxilliary` (`partition_id`, optional `silent`)
- `trigger_fire` (`partition_id`)
- `speak` (`text` required)

Example:
```json
{
  "command": "speak",
  "text": "Front door open",
  "response_topic": "per_client_unique_response_topic",
  "version": 1
}
```
