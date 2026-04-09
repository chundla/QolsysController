#!/usr/bin/env python3
import argparse
import asyncio
import contextlib
import json
import logging
import os
import signal
import socket
import ssl
import sys
from dataclasses import dataclass

from qolsys_controller.controller import QolsysController as QolsysControllerImpl
from qolsys_controller.errors import QolsysMqttError, QolsysSqlError, QolsysSslError


@dataclass
class ControllerConfig:
    panel_ip: str
    panel_mac: str
    random_mac: str
    config_dir: str
    plugin_ip: str | None
    auto_discover_pki: bool
    pairing_resume: bool
    mqtt_bridge: bool
    mqtt_bridge_allow_anonymous: bool
    mqtt_bridge_username: str
    mqtt_bridge_password: str
    mqtt_bridge_allowed_users: dict[str, str]
    start_pairing: bool
    check_user_code_on_arm: bool
    check_user_code_on_disarm: bool
    log_mqtt_messages: bool


def _detect_local_ip() -> str:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip: str = sock.getsockname()[0]
        sock.close()
        return ip
    except Exception:
        return "127.0.0.1"


def load_config(path: str) -> ControllerConfig:
    with open(path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)
        raw_mqtt_bridge_allowed_users = raw.get("mqtt_bridge_allowed_users", {})
        if not isinstance(raw_mqtt_bridge_allowed_users, dict):
            raw_mqtt_bridge_allowed_users = {}

        return ControllerConfig(
            panel_ip=raw["panel_ip"],
            panel_mac=raw["panel_mac"],
            random_mac=raw["random_mac"],
            config_dir=raw.get("config_dir", "/var/lib/qolsys-bridge"),
            plugin_ip=raw.get("plugin_ip"),
            auto_discover_pki=bool(raw.get("auto_discover_pki", False)),
            pairing_resume=bool(raw.get("resume_pairing", False)),
            start_pairing=bool(raw.get("start_pairing", False)),
            check_user_code_on_arm=bool(raw.get("check_user_code_on_arm", False)),
            check_user_code_on_disarm=bool(raw.get("check_user_code_on_disarm", False)),
            log_mqtt_messages=bool(raw.get("log_mqtt_messages", False)),
            mqtt_bridge=bool(raw.get("mqtt_bridge", False)),
            mqtt_bridge_allow_anonymous=bool(raw.get("mqtt_bridge_allow_anonymous", False)),
            mqtt_bridge_username=str(raw.get("mqtt_bridge_username", "")),
            mqtt_bridge_password=str(raw.get("mqtt_bridge_password", "")),
            mqtt_bridge_allowed_users={
                str(username): str(password)
                for username, password in raw_mqtt_bridge_allowed_users.items()
                if isinstance(username, str) and isinstance(password, str)
            },
        )


class QolsysController:
    def __init__(self, config: ControllerConfig, log: logging.Logger) -> None:
        self.config = config
        self.log = log
        self.controller = QolsysControllerImpl()

    async def start(self) -> None:
        os.makedirs(self.config.config_dir, exist_ok=True)

        settings = self.controller.settings
        settings.config_directory = self.config.config_dir
        settings.plugin_ip = self.config.plugin_ip or _detect_local_ip()
        settings.panel_ip = self.config.panel_ip
        settings.panel_mac = self.config.panel_mac
        settings.random_mac = self.config.random_mac
        settings.log_mqtt_messages = self.config.log_mqtt_messages
        settings.auto_discover_pki = self.config.auto_discover_pki
        settings.check_user_code_on_arm = self.config.check_user_code_on_arm
        settings.check_user_code_on_disarm = self.config.check_user_code_on_disarm
        settings.pairing_resume = self.config.pairing_resume
        settings._mqtt_bridge_enabled = self.config.mqtt_bridge
        settings._mqtt_bridge_allow_anonymous = self.config.mqtt_bridge_allow_anonymous
        settings._mqtt_bridge_username = self.config.mqtt_bridge_username
        settings._mqtt_bridge_password = self.config.mqtt_bridge_password
        settings._mqtt_bridge_allowed_users = self.config.mqtt_bridge_allowed_users

        configured = await self.controller.config(start_pairing=self.config.start_pairing)
        if not configured:
            raise RuntimeError("Failed to configure qolsys-controller (pairing/config issue)")

        try:
            await self.controller.start_operation()

        except QolsysMqttError:
            raise RuntimeError("Failed to start qolsys-controller due to MQTT error. Check logs for details.")

        except (QolsysSslError, ssl.SSLError):
            raise RuntimeError("Failed to start qolsys-controller due to SSL error. Check logs for details.")

        except QolsysSqlError:
            raise RuntimeError("Failed to start qolsys-controller due to SQL error. Check logs for details.")

        if not self.controller.connected:
            self.log.error("qolsys-controller not ready for operation")

    async def stop(self) -> None:
        await self.controller.stop_operation()


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="qolsys-controller")
    parser.add_argument("--config", default="config.json", help="Path to qolsys-controller config.json")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)
    log = logging.getLogger("qolsys-controller")

    config = load_config(args.config)
    bridge = QolsysController(config, log)

    stop_event = asyncio.Event()

    def _handle_signal() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _handle_signal)

    await bridge.start()
    await stop_event.wait()
    await bridge.stop()


# Change to the "Selector" event loop if platform is Windows
if sys.platform.lower() == "win32" or os.name.lower() == "nt":
    from asyncio import (  # type: ignore[attr-defined]
        WindowsSelectorEventLoopPolicy,
        set_event_loop_policy,
    )

    set_event_loop_policy(WindowsSelectorEventLoopPolicy())

asyncio.run(main())
