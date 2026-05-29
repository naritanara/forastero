# Copyright 2023, Peter Birch, mailto:peter@lightlogic.co.uk
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import dataclasses
from collections.abc import AsyncGenerator
from random import Random
from typing import TYPE_CHECKING, Generic, TypeVar

import cocotb
from cocotb.handle import SimHandleBase
from cocotb.triggers import RisingEdge

from forastero.event import Event

from .component import Component, _Io
from .transaction import BaseTransaction

if TYPE_CHECKING:
    from .bench import BaseBench

_Transaction = TypeVar("_Transaction", bound=BaseTransaction)


class MonitorEvent(Event, Generic[_Transaction]):
    pass


@dataclasses.dataclass()
class CaptureEvent(MonitorEvent[_Transaction]):
    transaction: _Transaction


@dataclasses.dataclass()
class MonitorStatistics:
    captured: int = 0


class BaseMonitor(Component[MonitorEvent[_Transaction], _Io]):
    """
    Component for sampling transactions from an interface matching the
    implementation's signalling protocol.

    :param tb:      Handle to the testbench
    :param io:      Handle to the BaseIO interface
    :param clk:     Clock signal to use when driving/sampling the interface
    :param rst:     Reset signal to use when driving/sampling the interface
    :param random:  Random number generator to use (optional)
    :param name:    Unique name for this component instance (optional)
    """

    def __init__(
        self,
        tb: "BaseBench",
        io: _Io,
        clk: SimHandleBase,
        rst: SimHandleBase,
        random: Random | None = None,
        name: str | None = None,
        blocking: bool = True,
    ) -> None:
        super().__init__(tb, io, clk, rst, random, name, blocking)
        self.stats = MonitorStatistics()
        cocotb.start_soon(self._monitor_loop())

    async def _monitor_loop(self) -> None:
        """Main loop for monitoring transactions on the interface"""
        await self.tb.ready()
        await RisingEdge(self.clk)
        self._ready.set()

        async for tx in self.monitor():
            self.stats.captured += 1
            self.publish(CaptureEvent(tx))

    def monitor(self) -> AsyncGenerator[_Transaction, None]:
        """
        Placeholder monitor, this should be overridden by a child class to match
        the signalling protocol of the interface's implementation.
        """
        raise NotImplementedError("monitor is not implemented on BaseMonitor")
