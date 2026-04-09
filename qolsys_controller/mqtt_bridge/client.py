import asyncio
import json
import logging
import ssl
from typing import TYPE_CHECKING

import aiomqtt

from qolsys_controller.automation.service_cover import CoverService
from qolsys_controller.automation.service_light import LightService
from qolsys_controller.automation.service_lock import LockService
from qolsys_controller.automation.service_siren import SirenService
from qolsys_controller.automation.service_thermostat import ThermostatService
from qolsys_controller.automation.service_valve import ValveService
from qolsys_controller.enum import PartitionArmingType, QolsysFanMode, QolsysHvacMode, QolsysNotification
from qolsys_controller.observable import Event

LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    from qolsys_controller.mqtt_bridge.bridge import MqttBridge


class MqttBridgeClient:
    def __init__(self, bridge: "MqttBridge") -> None:
        self._bridge = bridge
        self._client_id = "InternalClient"
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._ready_event = asyncio.Event()
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=1000)
        self._registered = False

    async def start(self) -> bool:
        if self._task and not self._task.done():
            LOGGER.warning("MQTT Bridge Client: Client already running")
            return False

        self._stop_event.clear()
        self._ready_event.clear()
        self._task = asyncio.create_task(self._run())
        await self._ready_event.wait()
        return True

    async def shutdown(self) -> None:
        LOGGER.debug("MQTT Bridge Client: Shutting down ...")

        self._stop_event.set()

        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        LOGGER.info("MQTT Bridge Client: Shutdown complete")

    def handle_event(self, event: Event) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            LOGGER.debug("MQTT Bridge Client: Queue is full. Dropping event: %s", event)

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            LOGGER.info("MQTT Bridge Client: Connecting...")

            try:
                tls_context = ssl.create_default_context()
                tls_context.load_verify_locations(cafile=str(self._bridge._controller._pki.mqtt_bridge_ca_cer_file_path))
                tls_context.check_hostname = False
                tls_context.verify_mode = ssl.CERT_REQUIRED

                username: str | None = None
                password: str | None = None
                if not self._bridge._controller.settings.mqtt_bridge_allow_anonymous:
                    username = self._bridge._controller.settings.mqtt_bridge_username.strip() or None
                    password = self._bridge._controller.settings.mqtt_bridge_password or None

                    if not username or not password:
                        for (
                            allowed_user,
                            allowed_password,
                        ) in self._bridge._controller.settings.mqtt_bridge_allowed_users.items():
                            if allowed_user and allowed_password:
                                username = allowed_user
                                password = allowed_password
                                break

                async with aiomqtt.Client(
                    hostname=self._bridge._controller.settings.plugin_ip,
                    port=self._bridge._controller.settings._mqtt_bridge_port,
                    tls_context=tls_context,
                    identifier=self._client_id,
                    username=username,
                    password=password,
                ) as client:
                    LOGGER.debug("MQTT Bridge Client: Connected")

                    # Subscribe
                    for topic in self._bridge.command_topics:
                        LOGGER.debug("MQTT Bridge Client: Subscribing to topic: %s", topic)
                        await client.subscribe(topic, qos=self._bridge.mqtt_qos)

                    # Register events ONCE
                    if not self._registered:
                        self._register_events()
                        self._registered = True

                    # Refresh state to publish current values on startup
                    self._refresh_state()

                    # MQTT Bridge Client is connected and running
                    if not self._ready_event.is_set():
                        self._ready_event.set()

                    # Run listener + publisher concurrently
                    await self._run_connected(client)

            except asyncio.CancelledError:
                LOGGER.info("MQTT Bridge Client: Shutting down ...")
                break

            except aiomqtt.MqttError as err:
                LOGGER.debug("MQTT Bridge Client: Connection error: %s", err)

            # Reconnect delay
            if not self._stop_event.is_set():
                LOGGER.debug("MQTT Bridge Client: Reconnecting in %s sec", self._bridge.mqtt_timeout)
                await asyncio.sleep(self._bridge.mqtt_timeout)

    async def _run_connected(self, client: aiomqtt.Client) -> None:
        listener = asyncio.create_task(self._listener(client))
        publisher = asyncio.create_task(self._publisher(client))

        LOGGER.info("MQTT Bridge Client: Running")

        done, pending = await asyncio.wait(
            [listener, publisher],
            return_when=asyncio.FIRST_EXCEPTION,
        )

        # Cancel remaining task
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Raise exception if any
        for task in done:
            if not task.cancelled():
                exc = task.exception()
                if exc:
                    raise exc

    async def _listener(self, client: aiomqtt.Client) -> None:
        try:
            async for message in client.messages:
                if message.topic.matches(self._bridge.automation_command_topic):
                    await self._handle_automation_command(message.payload.decode(errors="ignore"))

                if message.topic.matches(self._bridge.partition_command_topic):
                    await self._handle_partition_command(message.payload.decode(errors="ignore"))

                if message.topic.matches(f"{self._bridge.panel_topic}/command"):
                    await self._handle_panel_command(message.payload.decode(errors="ignore"))

        except aiomqtt.MqttError as err:
            if self._stop_event.is_set():
                return

            LOGGER.debug("MQTT Bridge Client: Listener error - %s", err)
            raise

    async def _publisher(self, client: aiomqtt.Client) -> None:
        try:
            while True:
                event = await self._queue.get()
                payload = json.dumps(event.data)

                id = event.data.get("id")
                if event.type == QolsysNotification.ZONE_UPDATE:
                    topic = f"{self._bridge.zone_topic}/{id}"
                elif event.type == QolsysNotification.PARTITION_UPDATE:
                    topic = f"{self._bridge.partition_topic}/{id}"
                elif event.type == QolsysNotification.AUTOMATION_UPDATE:
                    topic = f"{self._bridge.automation_topic}/{id}"
                elif event.type == QolsysNotification.PANEL_STATUS_UPDATE:
                    topic = f"{self._bridge.status_topic}"
                elif event.type == QolsysNotification.PANEL_SETTINGS_UPDATE:
                    topic = f"{self._bridge.settings_topic}"
                else:
                    continue

                await client.publish(topic, payload, qos=self._bridge.mqtt_qos, retain=True)

        except asyncio.CancelledError:
            raise

        except aiomqtt.MqttError as err:
            if self._stop_event.is_set():
                return

            LOGGER.debug("MQTT Bridge Client: Publisher error - %s", err)
            raise

    def _refresh_state(self) -> None:
        LOGGER.debug("MQTT Bridge Client: Refreshing state ...")
        for zone in self._bridge._controller.state.zones:
            self.handle_event(Event(QolsysNotification.ZONE_UPDATE, zone, zone.to_dict_event()))

        for partition in self._bridge._controller.state.partitions:
            self.handle_event(Event(QolsysNotification.PARTITION_UPDATE, partition, partition.to_dict_event()))

        for autdev in self._bridge._controller.state.automation_devices:
            self.handle_event(Event(QolsysNotification.AUTOMATION_UPDATE, autdev, autdev.to_dict_event()))

        # Panel sattus initial state update
        self.handle_event(
            Event(
                QolsysNotification.PANEL_STATUS_UPDATE,
                self._bridge._controller.panel,
                self._bridge._controller._to_event_dict(),
            )
        )

        # Panel settings initial state update
        self.handle_event(
            Event(
                QolsysNotification.PANEL_SETTINGS_UPDATE,
                self._bridge._controller.panel,
                self._bridge._controller.panel.to_event_dict(),
            )
        )

    def _register_events(self) -> None:
        LOGGER.debug("MQTT Bridge Client: Registering events ...")

        for zone in self._bridge._controller.state.zones:
            zone.register(QolsysNotification.ZONE_UPDATE, self.handle_event)

        for partition in self._bridge._controller.state.partitions:
            partition.register(QolsysNotification.PARTITION_UPDATE, self.handle_event)

        for autdev in self._bridge._controller.state.automation_devices:
            autdev.register(QolsysNotification.AUTOMATION_UPDATE, self.handle_event)

        self._bridge._controller.state.register(QolsysNotification.PANEL_STATUS_UPDATE, self.handle_event)
        self._bridge._controller.state.register(QolsysNotification.PANEL_SETTINGS_UPDATE, self.handle_event)

    async def _handle_automation_command(self, payload: str) -> None:
        # Decode JSON
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            LOGGER.error("MQTT Bridge Client: Invalid JSON payload: %s", payload)
            return

        valid_commands = [
            "light_on",
            "light_off",
            "light_level",
            "lock",
            "unlock",
            "cover_open",
            "cover_close",
            "cover_position",
            "siren_on",
            "siren_off",
            "valve_open",
            "valve_close",
            "valve_stop",
            "valve_position",
            "thermostat_mode",
            "thermostat_fan_mode",
            "thermostat_heat",
            "thermostat_cool",
        ]

        command: str = data.get("command")
        virtual_node_id: int = data.get("virtual_node_id")
        endpoint: int = data.get("endpoint")

        if command not in valid_commands:
            LOGGER.error("MQTT Bridge Client: Invalid command for automation device: %s", command)
            return

        if virtual_node_id is None:
            LOGGER.error("MQTT Bridge Client: Missing virtual_id in payload")
            return

        if endpoint is None:
            LOGGER.error("MQTT Bridge Client: Missing endpoint in payload")
            return

        automation_device = self._bridge._controller.state.automation_device(str(virtual_node_id))
        if automation_device is None:
            LOGGER.error("MQTT Bridge Client: Automation device not found for virtual_node_id: %s", virtual_node_id)
            return

        if command == "light_on":
            service = automation_device.service_get(LightService, endpoint)
            if not isinstance(service, LightService):
                LOGGER.error(
                    "MQTT Bridge Client: LightService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.turn_on()
            return

        if command == "light_off":
            service = automation_device.service_get(LightService, endpoint)
            if not isinstance(service, LightService):
                LOGGER.error(
                    "MQTT Bridge Client: LightService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.turn_off()
            return

        if command == "light_level":
            level = data.get("level")
            if level is None:
                LOGGER.error("MQTT Bridge Client: Missing level for light_level command")
                return

            service = automation_device.service_get(LightService, endpoint)
            if not isinstance(service, LightService):
                LOGGER.error(
                    "MQTT Bridge Client: LightService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            if not service.supports_level():
                LOGGER.error(
                    "MQTT Bridge Client: LightService does not support level for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.set_level(level)
            return

        if command == "lock":
            service = automation_device.service_get(LockService, endpoint)
            if not isinstance(service, LockService):
                LOGGER.error(
                    "MQTT Bridge Client: LockService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            if not service.supports_lock():
                LOGGER.error(
                    "MQTT Bridge Client: LockService does not support lock for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.lock()
            return

        if command == "unlock":
            service = automation_device.service_get(LockService, endpoint)
            if not isinstance(service, LockService):
                LOGGER.error(
                    "MQTT Bridge Client: LockService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            if not service.supports_lock():
                LOGGER.error(
                    "MQTT Bridge Client: LockService does not support lock for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.unlock()
            return

        if command == "cover_open":
            service = automation_device.service_get(CoverService, endpoint)
            if not isinstance(service, CoverService):
                LOGGER.error(
                    "MQTT Bridge Client: CoverService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            if not service.supports_open():
                LOGGER.error(
                    "MQTT Bridge Client: CoverService does not support open for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.open()
            return

        if command == "cover_close":
            service = automation_device.service_get(CoverService, endpoint)
            if not isinstance(service, CoverService):
                LOGGER.error(
                    "MQTT Bridge Client: CoverService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            if not service.supports_close():
                LOGGER.error(
                    "MQTT Bridge Client: CoverService does not support close for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.close()
            return

        if command == "cover_position":
            position = data.get("position")
            if position is None:
                LOGGER.error("MQTT Bridge Client: Missing position for cover_position command")
                return

            service = automation_device.service_get(CoverService, endpoint)
            if not isinstance(service, CoverService):
                LOGGER.error(
                    "MQTT Bridge Client: CoverService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            if not service.supports_position():
                LOGGER.error(
                    "MQTT Bridge Client: CoverService does not support position for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.set_current_position(position)
            return

        if command == "siren_on":
            service = automation_device.service_get(SirenService, endpoint)
            if not isinstance(service, SirenService):
                LOGGER.error(
                    "MQTT Bridge Client: SirenService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.turn_on()
            return

        if command == "siren_off":
            service = automation_device.service_get(SirenService, endpoint)
            if not isinstance(service, SirenService):
                LOGGER.error(
                    "MQTT Bridge Client: SirenService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.turn_off()
            return

        if command == "valve_open":
            service = automation_device.service_get(ValveService, endpoint)
            if not isinstance(service, ValveService):
                LOGGER.error(
                    "MQTT Bridge Client: ValveService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            if not service.supports_open():
                LOGGER.error(
                    "MQTT Bridge Client: ValveService does not support open for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.open()
            return

        if command == "valve_close":
            service = automation_device.service_get(ValveService, endpoint)
            if not isinstance(service, ValveService):
                LOGGER.error(
                    "MQTT Bridge Client: ValveService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            if not service.supports_close():
                LOGGER.error(
                    "MQTT Bridge Client: ValveService does not support close for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.close()
            return

        if command == "valve_stop":
            service = automation_device.service_get(ValveService, endpoint)
            if not isinstance(service, ValveService):
                LOGGER.error(
                    "MQTT Bridge Client: ValveService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            if not service.supports_stop():
                LOGGER.error(
                    "MQTT Bridge Client: ValveService does not support stop for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.stop()
            return

        if command == "valve_position":
            position = data.get("position")
            if position is None:
                LOGGER.error("MQTT Bridge Client: Missing position for valve_position command")
                return

            service = automation_device.service_get(ValveService, endpoint)
            if not isinstance(service, ValveService):
                LOGGER.error(
                    "MQTT Bridge Client: ValveService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            if not service.supports_position():
                LOGGER.error(
                    "MQTT Bridge Client: ValveService does not support position for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.set_position(position)
            return

        if command == "thermostat_mode":
            mode = data.get("mode")
            if mode is None:
                LOGGER.error("MQTT Bridge Client: Missing mode for thermostat_mode command")
                return

            valid_modes = [x.name for x in QolsysHvacMode]
            if mode not in valid_modes:
                LOGGER.error("MQTT Bridge Client: Invalid mode for thermostat_mode command: %s", mode)
                return

            service = automation_device.service_get(ThermostatService, endpoint)
            if not isinstance(service, ThermostatService):
                LOGGER.error(
                    "MQTT Bridge Client: ThermostatService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.set_hvac_mode(QolsysHvacMode[mode])
            return

        if command == "thermostat_fan_mode":
            fan_mode = data.get("fan_mode")
            if fan_mode is None:
                LOGGER.error("MQTT Bridge Client: Missing fan_mode for thermostat_fan_mode command")
                return

            valid_fan_modes = [x.name for x in QolsysFanMode]
            if fan_mode not in valid_fan_modes:
                LOGGER.error("MQTT Bridge Client: Invalid fan_mode for thermostat_fan_mode command: %s", fan_mode)
                return

            service = automation_device.service_get(ThermostatService, endpoint)
            if not isinstance(service, ThermostatService):
                LOGGER.error(
                    "MQTT Bridge Client: ThermostatService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return

            if not service.supports_fan_mode():
                LOGGER.error(
                    "MQTT Bridge Client: ThermostatService does not support fan_mode for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return

            await service.set_fan_mode(QolsysFanMode[fan_mode])
            return

        if command == "thermostat_heat":
            temperature = data.get("temperature")
            if temperature is None:
                LOGGER.error("MQTT Bridge Client: Missing temperature for thermostat_heat command")
                return

            service = automation_device.service_get(ThermostatService, endpoint)
            if isinstance(service, ThermostatService) and service.supports_target_temperature():
                await service.set_temperature(temperature, QolsysHvacMode.HEAT)
                return

        if command == "thermostat_cool":
            temperature = data.get("temperature")
            if temperature is None:
                LOGGER.error("MQTT Bridge Client: Missing temperature for thermostat_cool command")
                return

            service = automation_device.service_get(ThermostatService, endpoint)
            if not isinstance(service, ThermostatService):
                LOGGER.error(
                    "MQTT Bridge Client: ThermostatService not found for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            if not service.supports_target_temperature():
                LOGGER.error(
                    "MQTT Bridge Client: ThermostatService does not support target_temperature for virtual_node_id: %s, endpoint: %s",
                    virtual_node_id,
                    endpoint,
                )
                return
            await service.set_temperature(temperature, QolsysHvacMode.COOL)
            return

    async def _handle_partition_command(self, payload: str) -> None:
        # Decode JSON
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            LOGGER.error("MQTT Bridge Client: Invalid JSON payload: %s", payload)
            return

        command: str = data.get("command")
        partition_id: int = data.get("partition_id")
        user_code: str = data.get("user_code")
        exit_delay: bool = data.get("exit_delay", True)
        exit_sounds: bool = data.get("exit_sounds", True)
        instant_arm: bool = data.get("instant_arm", False)
        silent_disarm: bool = data.get("silent_disarm", False)

        valid_commands = [x.name for x in PartitionArmingType]
        valid_commands.append("DISARM")
        if command not in valid_commands:
            LOGGER.error("MQTT Bridge Client: Invalid command for partition command: %s", command)
            return

        if partition_id is None:
            LOGGER.error("MQTT Bridge Client: Missing partition_id in payload")
            return

        if command == "DISARM":
            await self._bridge._controller.command_disarm(str(partition_id), user_code, silent_disarm)
            return
        else:
            await self._bridge._controller.command_arm(
                str(partition_id),
                PartitionArmingType[command],
                user_code,
                exit_delay,
                exit_sounds,
                instant_arm,
            )

    async def _handle_panel_command(self, payload: str) -> None:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            LOGGER.error("MQTT Bridge Client: Invalid JSON payload: %s", payload)
            return

        command: str = data.get("command")

        if command == "execute_scene":
            scene_id = data.get("scene_id")
            if scene_id is None:
                LOGGER.error("MQTT Bridge Client: Missing scene_id in panel command payload")
                return
            await self._bridge._controller.command_panel_execute_scene(str(scene_id))
            return

        if command == "trigger_police":
            partition_id = data.get("partition_id")
            if partition_id is None:
                LOGGER.error("MQTT Bridge Client: Missing partition_id in panel command payload")
                return
            silent = bool(data.get("silent", False))
            await self._bridge._controller.command_panel_trigger_police(str(partition_id), silent)
            return

        if command == "trigger_auxilliary":
            partition_id = data.get("partition_id")
            if partition_id is None:
                LOGGER.error("MQTT Bridge Client: Missing partition_id in panel command payload")
                return
            silent = bool(data.get("silent", False))
            await self._bridge._controller.command_panel_trigger_auxilliary(str(partition_id), silent)
            return

        if command == "trigger_fire":
            partition_id = data.get("partition_id")
            if partition_id is None:
                LOGGER.error("MQTT Bridge Client: Missing partition_id in panel command payload")
                return
            await self._bridge._controller.command_panel_trigger_fire(str(partition_id))
            return

        if command == "speak":
            text = data.get("text")
            if not text:
                LOGGER.error("MQTT Bridge Client: Missing text in panel command payload")
                return
            await self._bridge._controller.command_panel_speak(str(text))
            return

        LOGGER.error("MQTT Bridge Client: Invalid command for panel command: %s", command)
