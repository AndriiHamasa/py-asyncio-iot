import asyncio
import random
import string
from typing import Protocol

from app.iot.message import Message, MessageType


def generate_id(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_uppercase, k=length))


# Protocol is very similar to ABC, but uses duck typing
# so devices should not inherit for it (if it walks like a duck, and quacks like a duck, it's a duck)
class Device(Protocol):
    async def connect(self) -> None:
        ...  # Ellipsis - similar to "pass", but sometimes has different meaning

    async def disconnect(self) -> None:
        ...

    async def send_message(self, message_type: MessageType, data: str) -> None:
        ...


class IOTService:
    def __init__(self) -> None:
        self.devices: dict[str, Device] = {}

    async def register_device(self, device: Device) -> str:
        await device.connect()
        device_id = generate_id()
        self.devices[device_id] = device
        return device_id

    async def unregister_device(self, device_id: str) -> None:
        await self.devices[device_id].disconnect()
        del self.devices[device_id]

    def get_device(self, device_id: str) -> Device:
        return self.devices[device_id]

    async def run_program(self, program: list[Message]) -> None:
        print("=====RUNNING PROGRAM======")
        tasks = {}
        for msg in program:
            if msg.device_id not in tasks:
                tasks[msg.device_id] = []
            tasks[msg.device_id].append(msg)

        await asyncio.gather(
            *(self.run_device_program(device_id, device_msgs) for
              device_id, device_msgs in tasks.items()))
        print("=====END OF PROGRAM======")

    async def run_device_program(
            self, device_id: str,
            messages: list[Message]
    ) -> None:
        for msg in messages:
            await self.send_msg(msg)

    async def send_msg(self, msg: Message) -> None:
        await self.devices[msg.device_id].send_message(msg.msg_type, msg.data)
