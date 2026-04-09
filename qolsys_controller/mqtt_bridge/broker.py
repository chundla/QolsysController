import asyncio
import hmac
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from amqtt.broker import Broker
from amqtt.plugins.authentication import BaseAuthPlugin

LOGGER = logging.getLogger(__name__)
logging.getLogger("transitions.core").setLevel(logging.ERROR)
logging.getLogger("amqtt").setLevel(logging.ERROR)
logging.getLogger("amqtt.core").setLevel(logging.ERROR)
logging.getLogger("amqtt.broker").setLevel(logging.ERROR)
logging.getLogger("amqtt.plugins").setLevel(logging.ERROR)


if TYPE_CHECKING:
    from qolsys_controller.controller import QolsysController


class AuthPlugin(BaseAuthPlugin):  # type: ignore[misc]
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.allowed_users: dict[str, str] = dict(getattr(self.config, "allowed_users", {}))

    def set_config(self, config: dict[str, Any]) -> None:
        raw_allowed_users = config.get("allowed_users", {})
        if not isinstance(raw_allowed_users, dict):
            self.allowed_users = {}
            return

        self.allowed_users = {
            str(username): str(password)
            for username, password in raw_allowed_users.items()
            if isinstance(username, str) and isinstance(password, str) and username and password
        }

        if hasattr(self, "config") and hasattr(self.config, "allowed_users"):
            self.config.allowed_users = dict(self.allowed_users)

    async def authenticate(self, *, session: Any) -> bool | None:
        username = getattr(session, "username", None)
        password = getattr(session, "password", None)
        if not username or not password:
            return False

        allowed_users = self.allowed_users or dict(getattr(self.config, "allowed_users", {}))
        expected_password = allowed_users.get(username)
        if expected_password is None:
            return False

        return hmac.compare_digest(password, expected_password)

    @dataclass
    class Config:
        allowed_users: dict[str, str] = field(default_factory=dict)


class MqttBridgeBroker:
    def __init__(self, controller: "QolsysController") -> None:
        self._controller = controller
        self._config: dict[str, Any] = self._build_config()
        self._broker: Broker | None = None
        self._is_running: bool = False
        self._broker_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> bool:
        if self._is_running:
            return True

        if self._broker_task and not self._broker_task.done():
            LOGGER.warning("MQTT Bridge Broker: Start requested while broker task is already running")
            return False

        LOGGER.info(
            "MQTT Bridge Broker: Starting: %s:%s ...",
            self._controller.settings.plugin_ip,
            self._controller.settings._mqtt_bridge_port,
        )

        startup_event = asyncio.Event()
        startup_result: dict[str, bool | Exception] = {"started": False}
        self._stop_event.clear()
        self._broker_task = asyncio.create_task(self._run(startup_event, startup_result))

        await startup_event.wait()

        result = startup_result.get("started")
        if result is True:
            return True

        broker_error = startup_result.get("error")
        if isinstance(broker_error, Exception):
            LOGGER.error("MQTT Bridge Broker: Error Starting: %s", broker_error)

        if self._broker_task.done():
            try:
                await self._broker_task
            except Exception:
                pass

        self._broker_task = None
        return False

    async def _run(self, startup_event: asyncio.Event, startup_result: dict[str, bool | Exception]) -> None:
        try:
            await self._check_or_create_certificates()
            if self._broker is None:
                self._broker = self._create_broker()
            await self._broker.start()
            await self.wait_for_broker_start()

            self._is_running = True
            startup_result["started"] = True
            startup_event.set()

            # Wait forever until cancelled
            await self._stop_event.wait()

        except asyncio.CancelledError:
            pass

        except Exception as err:
            self._is_running = False
            startup_result["error"] = err
            startup_event.set()
            LOGGER.error("MQTT Bridge Broker: Runtime error: %s", err)
            raise

        finally:
            self._is_running = False

            if not startup_event.is_set():
                startup_result["started"] = False
                startup_event.set()

            try:
                if self._broker is not None:
                    await asyncio.wait_for(
                        asyncio.shield(self._broker.shutdown()),
                        timeout=5,
                    )

            except asyncio.TimeoutError:
                LOGGER.warning("MQTT Bridge Broker: Shutdown timed out")

            except Exception as err:
                LOGGER.debug("MQTT Bridge Broker: Error during shutdown: %s", err)

    async def wait_for_broker_start(self, timeout: int = 5) -> None:
        if self._broker is None:
            raise RuntimeError("MQTT Bridge Broker is not initialized")

        start_time = asyncio.get_event_loop().time()
        while self._broker.transitions.state != "started":
            if asyncio.get_event_loop().time() - start_time > timeout:
                raise TimeoutError("MQTT Bridge Broker did not start in time")
            await asyncio.sleep(0.05)
        LOGGER.info("MQTT Bridge Broker: Running")

    async def _check_or_create_certificates(self) -> None:
        LOGGER.debug("MQTT Bridge Broker: Validating certificates")
        await self._controller._pki.create_mqtt_bridge_certificates()

    def _create_broker(self) -> Broker:
        broker = Broker(self._config)
        return broker

    def _build_config(self) -> dict[str, Any]:
        listeners = {
            "default": {
                "type": "tcp",
                "bind": f"{self._controller.settings.plugin_ip}:{self._controller.settings.mqtt_bridge_port}",
                "ssl": True,
                "max_connections": self._controller.settings.mqtt_bridge_max_connections,
                "certfile": str(self._controller._pki.mqtt_bridge_cer_file_path),
                "keyfile": str(self._controller._pki.mqtt_bridge_key_file_path),
            }
        }

        plugins: dict[str, dict[str, Any]] = {}

        if self._controller.settings.mqtt_bridge_allow_anonymous:
            LOGGER.warning("MQTT Bridge Broker: Anonymous MQTT access enabled")
            plugins["amqtt.plugins.authentication.AnonymousAuthPlugin"] = {"allow_anonymous": True}
        else:
            allowed_users = self._build_allowed_users()
            if not allowed_users:
                raise ValueError(
                    "MQTT bridge authentication is enabled but no users were configured. "
                    "Set mqtt_bridge_username/mqtt_bridge_password, mqtt_bridge_allowed_users, "
                    "or explicitly set mqtt_bridge_allow_anonymous=true."
                )

            plugins["qolsys_controller.mqtt_bridge.broker.AuthPlugin"] = {
                "allowed_users": allowed_users,
            }

        return {"listeners": listeners, "plugins": plugins}

    def _build_allowed_users(self) -> dict[str, str]:
        allowed_users = {
            username: password
            for username, password in self._controller.settings.mqtt_bridge_allowed_users.items()
            if isinstance(username, str) and isinstance(password, str) and username and password
        }

        username = self._controller.settings.mqtt_bridge_username.strip()
        password = self._controller.settings.mqtt_bridge_password

        if username and password:
            allowed_users[username] = password

        return allowed_users

    async def shutdown(self) -> None:
        LOGGER.info("MQTT Bridge Broker: Shutting down ...")
        if self._broker_task:
            self._stop_event.set()
            await self._broker_task
            self._broker_task = None
            LOGGER.info("MQTT Bridge Broker: Shutdown complete")
