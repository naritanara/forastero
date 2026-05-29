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
from collections.abc import Iterable, Iterator
from random import Random
from typing import TYPE_CHECKING, Generic, TypeVar, overload

import cocotb
from cocotb.handle import SimHandleBase
from cocotb.triggers import Event as CocotbEvent
from cocotb.triggers import RisingEdge
from cocotb.utils import get_sim_time

from .component import Component, _Io
from .event import Event
from .queue import Queue
from .transaction import BaseTransaction

if TYPE_CHECKING:
    from .bench import BaseBench


TX = TypeVar("TX", bound=BaseTransaction)


class DriverEvent(Event, Generic[TX]):
    pass


@dataclasses.dataclass()
class EnqueueEvent(DriverEvent[TX]):
    transactions: Iterable[TX]


@dataclasses.dataclass()
class PreDriveEvent(DriverEvent[TX]):
    transaction: TX


@dataclasses.dataclass()
class PostDriveEvent(DriverEvent[TX]):
    transaction: TX


@dataclasses.dataclass()
class DriverStatistics:
    dequeued: int = 0


class _WithDriverEvent(Generic[TX]):
    event: tuple[type[DriverEvent[TX]], CocotbEvent] | None = None

    def set_event_if_eq(self, event: type[DriverEvent[TX]]):
        if (evt := self.event) and evt[0] is event:
            evt[1].set()

    def get_cocotb_event(self) -> CocotbEvent | None:
        return self.event[1] if self.event else None


@dataclasses.dataclass()
class _TransactionWithEvent(_WithDriverEvent[TX]):
    transaction: TX


@dataclasses.dataclass()
class _EnqueuedTransaction(_WithDriverEvent[TX]):
    transactions: Iterator[TX]


class BaseDriver(
    Component[DriverEvent[TX], _Io],
):
    """
    Component for driving transactions onto an interface matching the
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
        self.stats = DriverStatistics()
        self._queue: Queue[_EnqueuedTransaction[TX]] = Queue()
        cocotb.start_soon(self._driver_loop())

    @property
    def busy(self) -> bool:
        """Busy when either locked or the queue has outstanding entries"""
        return not self._queue.empty and super().busy

    @property
    def queued(self) -> int:
        """Return how many entries are queued up"""
        return self._queue.level

    @overload
    def enqueue(
        self,
        transaction: TX | Iterable[TX],
        wait_for: None = None,
    ) -> None: ...

    @overload
    def enqueue(
        self,
        transaction: TX | Iterable[TX],
        wait_for: type[DriverEvent[TX]],
    ) -> CocotbEvent: ...

    def enqueue(
        self,
        transaction: TX | Iterable[TX],
        wait_for: type[DriverEvent[TX]] | None = None,
    ) -> CocotbEvent | None:
        """
        Queue up a transaction to be driven onto the interface

        :param transaction: Transaction to queue, must inherit from BaseTransaction,
                            or be an iterable which yields BaseTransaction
        :param wait_for:    When defined, this will return an event that can be
                            monitored for a given transaction event occurring
        """
        # Handle a pure transaction
        if isinstance(transaction, BaseTransaction):
            transaction = [transaction]
        # Bail if not a transaction or iterable
        elif not isinstance(transaction, Iterable):
            raise TypeError(
                f"Transaction objects should inherit from BaseTransaction or be "
                f"an iterable unlike {transaction}"
            )

        # Wrap the transaction in an _EnqueuedTransaction
        tx = _EnqueuedTransaction(iter(transaction))

        # Does this transaction/iterable need an event?
        if wait_for is not None:
            tx.event = wait_for, CocotbEvent()
        # Queue up the transaction with no delay
        self._queue.push(tx)
        # Notify any enqueue subscribers
        self.publish(EnqueueEvent(transaction))
        # Immediately set event if waiting for enqueue
        tx.set_event_if_eq(EnqueueEvent)
        # Return the cocotb Event
        return tx.get_cocotb_event()

    async def _get_from_queue(self) -> _TransactionWithEvent[TX]:
        """
        Fetch next item from the queue.
        Process iterables and add events to yielded BaseTransaction
        """
        while True:
            await self._queue.wait_for_not_empty()
            obj = self._queue.peek()
            if True:
                # yield from iterable, append events (if any) and pop from queue when exhausted
                while True:
                    next_item = next(obj.transactions, StopIteration())
                    if isinstance(next_item, StopIteration):
                        # Remove the exhausted EnqueuedTransaction from the queue
                        await self._queue.pop()
                        # After popping, break to outer loop to process the new front item
                        break
                    elif not isinstance(next_item, BaseTransaction):
                        raise TypeError(
                            "Transaction objects should inherit from BaseTransaction",
                            f" unlike {next_item}",
                        )
                    # If event is set, attach to the transaction
                    ret = _TransactionWithEvent[TX](next_item)
                    ret.event = obj.event
                    return ret

    async def _driver_loop(self) -> None:
        """Main loop for driving transactions onto the interface"""
        await self.tb.ready()
        await RisingEdge(self.clk)
        self._ready.set()
        while True:
            # Pickup next event to drive
            tx_with_evt = await self._get_from_queue()
            obj = tx_with_evt.transaction
            # Wait until reset is deasserted
            while self.rst.value == self.tb.rst_active_value:
                await RisingEdge(self.clk)
            # Lock out the driver (prevents shutdown mid-stimulus)
            await self.lock()
            # Set the timestamp where the transaction was about to be driven
            obj.timestamp = int(get_sim_time("ns"))
            # Notify any pre-drive subscribers
            self.publish(PreDriveEvent(obj))
            tx_with_evt.set_event_if_eq(PreDriveEvent)
            # Drive the transaction
            await self.drive(obj)
            self.stats.dequeued += 1
            # Notify any post-drive subscribers
            self.publish(PostDriveEvent(obj))
            tx_with_evt.set_event_if_eq(PostDriveEvent)
            # Release the lock
            self.release()

    async def drive(self, obj: TX) -> None:
        """
        Placeholder driver, this should be overridden by a child class to match
        the signalling protocol of the interface's implementation.

        :param obj: The transaction to drive onto the interface
        """
        del obj
        raise NotImplementedError("drive is not implemented on BaseDriver")
