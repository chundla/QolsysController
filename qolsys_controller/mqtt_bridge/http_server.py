from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from qolsys_controller.controller import QolsysController

LOGGER = logging.getLogger(__name__)


class MqttBridgeHttpServer:
    _REQUEST_READ_TIMEOUT_SECONDS = 2

    def __init__(self, controller: 'QolsysController') -> None:
        self._controller = controller
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> bool:
        if self._server is not None:
            return True

        host = '127.0.0.1'
        port = self._controller.settings.mqtt_bridge_http_port
        self._server = await asyncio.start_server(self._handle_client, host, port)
        LOGGER.info('MQTT Bridge HTTP server listening on %s:%s', host, port)
        return True

    async def shutdown(self) -> None:
        if self._server is None:
            return

        self._server.close()
        await self._server.wait_closed()
        self._server = None
        LOGGER.info('MQTT Bridge HTTP server stopped')

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            raw = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), timeout=self._REQUEST_READ_TIMEOUT_SECONDS)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            writer.close()
            await writer.wait_closed()
            return
        except Exception:
            writer.close()
            await writer.wait_closed()
            return

        try:
            request_line = raw.split(b'\r\n', 1)[0].decode('utf-8', errors='ignore')
            method, target, _version = request_line.split(' ', 2)
        except ValueError:
            await self._send_text(writer, 400, 'Bad Request')
            return

        path = target.split('?', 1)[0]

        if method != 'GET':
            await self._send_text(writer, 405, 'Method Not Allowed')
            return

        if path == '/health':
            payload = {
                'connected': self._controller.connected,
                'paired': self._controller.connected,
                'ca_ready': await self._has_ca(),
                'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            }
            await self._send_json(writer, 200, payload)
            return

        if path == '/mqtt-bridge/ca':
            await self._send_ca(writer)
            return

        await self._send_text(writer, 404, 'Not Found')

    async def _has_ca(self) -> bool:
        return await asyncio.to_thread(self._controller._pki.mqtt_bridge_ca_cer_file_path.exists)

    async def _send_ca(self, writer: asyncio.StreamWriter) -> None:
        if not await self._has_ca():
            await self._send_text(writer, 404, 'CA not found')
            return

        try:
            payload = await asyncio.to_thread(self._controller._pki.mqtt_bridge_ca_cer_file_path.read_bytes)
        except Exception as err:
            LOGGER.error('MQTT Bridge HTTP server failed to read CA: %s', err)
            await self._send_text(writer, 500, 'Internal Server Error')
            return

        headers = {
            'Content-Type': 'application/x-pem-file',
            'Content-Length': str(len(payload)),
            'Connection': 'close',
        }
        await self._send_response(writer, 200, headers, payload)

    async def _send_json(self, writer: asyncio.StreamWriter, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload).encode('utf-8')
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Content-Length': str(len(body)),
            'Connection': 'close',
        }
        await self._send_response(writer, status, headers, body)

    async def _send_text(self, writer: asyncio.StreamWriter, status: int, message: str) -> None:
        body = message.encode('utf-8')
        headers = {
            'Content-Type': 'text/plain; charset=utf-8',
            'Content-Length': str(len(body)),
            'Connection': 'close',
        }
        await self._send_response(writer, status, headers, body)

    async def _send_response(self, writer: asyncio.StreamWriter, status: int, headers: dict[str, str], body: bytes) -> None:
        reason = {
            200: 'OK',
            400: 'Bad Request',
            404: 'Not Found',
            405: 'Method Not Allowed',
            500: 'Internal Server Error',
        }.get(status, 'OK')

        response = [f'HTTP/1.1 {status} {reason}\r\n']
        for key, value in headers.items():
            response.append(f'{key}: {value}\r\n')
        response.append('\r\n')
        writer.write(''.join(response).encode('utf-8') + body)
        await writer.drain()
        writer.close()
        await writer.wait_closed()
