import socket

from zeroconf import ServiceInfo
from zeroconf.asyncio import AsyncZeroconf


class QolsysMDNS:
    def __init__(self, ip: str, port: int, external_zero_conf: AsyncZeroconf | None = None) -> None:
        # Add possible external zeroconf instance provided by Home Assistant by example
        # If no external instance is provided, create our own
        if external_zero_conf:
            self.azc = external_zero_conf
        else:
            self.azc = AsyncZeroconf()

        instance_name = f"NsdPairService-{ip.replace('.', '-')}-{port}._http._tcp.local."
        self.mdns_info = ServiceInfo(
            "_http._tcp.local.",
            instance_name,
            addresses=[socket.inet_aton(ip)],
            port=port,
        )

    async def start_mdns(self) -> None:
        await self.azc.async_register_service(self.mdns_info)

    async def stop_mdns(self) -> None:
        await self.azc.async_unregister_service(self.mdns_info)
