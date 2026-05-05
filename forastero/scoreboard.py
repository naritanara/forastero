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

import logging
from asyncio import Lock
from collections import defaultdict
from collections.abc import Callable, Iterable, Sized
from enum import Enum, auto
from itertools import chain
from logging import Logger
from typing import TYPE_CHECKING, Generic, TypeVar, overload

import cocotb
from cocotb.triggers import First, RisingEdge, Timer
from cocotb.utils import get_sim_time

if TYPE_CHECKING:
    from forastero.bench import BaseBench

from .monitor import BaseMonitor, MonitorEvent
from .queue import Queue
from .transaction import BaseTransaction


class ChannelTimeoutError(AssertionError):
    pass


class WindowResidenceError(AssertionError):
    pass


class DrainPolicy(Enum):
    """Specify the draining behaviour for a scoreboard channel"""

    MON_AND_REF = auto()
    """Block until both monitor and reference queues are empty"""
    MON_ONLY = auto()
    """Block until only the monitor queue is empty"""
    REF_ONLY = auto()
    """Block until only the reference queue is empty"""
    NON_BLOCKING = auto()
    """Do not wait for either queue to drain"""


T = TypeVar("T")
TX = TypeVar("TX", bound=BaseTransaction)
TX_FILTERED = TypeVar("TX_FILTERED", bound=BaseTransaction, infer_variance=True, default=TX)

TransactionFilter = Callable[[BaseMonitor[TX], MonitorEvent, TX], TX_FILTERED]
ChannelComparisonCallback = Callable[["Channel", T, T], None]


class Channel(Generic[TX, TX_FILTERED]):
    """
    A channel gathers transactions from a monitor and a reference model of some
    form. When both queues contain an entry, the top-most entry is popped from
    both queues and tested for equivalence. Any mismatches are reported to the
    scoreboard.

    :param name:         Name of this scoreboard channel
    :param monitor:      Handle to the monitor capturing traffic from the DUT
    :param log:          Handle to the scoreboard log
    :param filter_fn:    Function to filter or modify captured transactions
    :param timeout_ns:   Optional timeout to allow for a object sat at the front
                         of the monitor queue to remain unmatched (in nanoseconds,
                         a value of None disables the timeout mechanism)
    :param polling_ns:   How frequently to poll to check for unmatched items stuck
                         in the monitor queue in nanoseconds (defaults to 100 ns)
    :param match_window: Where precise ordering of expected transactions is not
                         known, a positive integer matching window can be used
                         to match any of the next N transactions in the reference
                         queue (where N is set by match_window)
    :param drain_policy: Specify how the handle should handle draining and which
                         queues should be considered when blocking testcase
                         shutdown
    """

    def __init__(
        self,
        name: str,
        monitor: BaseMonitor[TX],
        log: Logger,
        filter_fn: TransactionFilter[TX, TX_FILTERED] = lambda _mon, evt, obj: obj,
        timeout_ns: int | None = None,
        polling_ns: int = 100,
        drain_policy: DrainPolicy = DrainPolicy.MON_AND_REF,
        match_window: int = 1,
    ) -> None:
        self.name = name
        self.monitor = monitor
        self.log = log
        self.filter_fn = filter_fn
        self.timeout_ns = timeout_ns
        self.polling_ns = polling_ns
        self.drain_policy = drain_policy
        assert self.drain_policy in list(
            DrainPolicy
        ), f"Unsupported draining mode {self.drain_policy}"
        self.match_window = match_window or 1
        assert (
            isinstance(self.match_window, int) and self.match_window > 0
        ), "Channel matching window must be a positive integer"
        self._q_mon = Queue[TX_FILTERED]()
        self._q_ref = Queue[TX_FILTERED]()
        self._lock = Lock()
        self._matched = 0
        self._mismatched = 0
        self._residence = defaultdict(lambda: 0)

        def _sample(mon: BaseMonitor[TX], evt: MonitorEvent, obj: TX) -> None:
            if mon is self.monitor and evt is MonitorEvent.CAPTURE:
                # If a filter function was provided, apply it
                obj_filtered = self.filter_fn(mon, evt, obj)
                # A filter can drop the transaction, so test for None
                if obj_filtered is not None:
                    self.push_monitor(obj_filtered)

        self.monitor.subscribe(MonitorEvent.CAPTURE, _sample)
        cocotb.start_soon(self._polling())

    @property
    def monitor_depth(self) -> int:
        """Number of captured packets queued up by the DUT monitor"""
        return self._q_mon.level

    @property
    def reference_depth(self) -> int:
        """Number of expected packets queued up by the reference model"""
        return self._q_ref.level

    @property
    def matched(self) -> int:
        """Number of matched packets between monitor and reference model"""
        return self._matched

    @property
    def mismatched(self) -> int:
        """Number of mismatched packets between monitor and reference model"""
        return self._mismatched

    @property
    def total(self) -> int:
        """Total number of packets matched/mismatched"""
        return self._matched + self._mismatched

    def push_monitor(self, *transactions: TX_FILTERED) -> None:
        """
        Push one or more captured transactions into the monitor's queue.

        :param *transactions: Captured transactions
        """
        for transaction in transactions:
            assert isinstance(transaction, BaseTransaction)
            self._q_mon.push(transaction)

    @overload
    def push_reference(self, *transactions: TX_FILTERED) -> None: ...

    if TYPE_CHECKING:

        @overload
        def push_reference(self, queue: str, *transactions: TX_FILTERED) -> None: ...

    def push_reference(self, queue: str | TX_FILTERED, *transactions: TX_FILTERED) -> None:
        """
        Push one or more captured transactions into the reference (model) queue.

        :param *transactions: Captured transactions
        """
        if isinstance(queue, str):
            raise ValueError("Named queues are only supported by FunnelChannel")

        for transaction in chain([queue], transactions):
            assert isinstance(transaction, BaseTransaction)
            self._q_ref.push(transaction)

    async def _dequeue(self) -> tuple[TX_FILTERED, TX_FILTERED]:
        """
        Dequeue the top-most transaction from both the monitor and reference
        queues.

        :returns: Tuple of the monitor transaction and reference transaction
        """
        # Wait for monitor to capture a transaction
        await self._q_mon.wait_for_not_empty()
        # When a monitor transaction arrives, lock out the drain procedure
        await self._lock.acquire()
        # If matching window is 1, wait for the first reference transaction
        if self.match_window == 1:
            next_ref = await self._q_ref.pop()
        # If matching window > 1,
        else:
            # Peek at the captured transaction and search the next N transactions
            peek_mon = self._q_mon[0]
            next_ref = None
            while next_ref is None:
                # Search within the match window
                for idx, peek_ref in enumerate(self._q_ref[: self.match_window]):
                    # Track how many cycles this item has been present in the matching window
                    self._residence[id(peek_ref)] += 1
                    # Check if this item has been in the matching window too long?
                    if self._residence[id(peek_ref)] > self.match_window:
                        raise WindowResidenceError(
                            f"Item has been present in the matching window of scoreboard "
                            f"channel {self.name} for longer than the window length of "
                            f"{self.match_window}: {peek_ref}"
                        )
                    # If the reference and monitor transactions match...
                    if peek_ref == peek_mon:
                        # ...pop the reference transaction and...
                        next_ref = await self._q_ref.pop(idx)
                        # ...stop tracking its residence time
                        del self._residence[id(peek_ref)]
                        break
                # If no match found...
                else:
                    # ...if queue length less than the match window, wait for a push
                    if len(self._q_ref) < self.match_window:
                        await self._q_ref.on_push_event.wait()
                    # ...otherwise pop the zeroeth entry
                    else:
                        next_ref = await self._q_ref.pop(0)
        # Pop the front of the monitor queue
        next_mon = await self._q_mon.pop()
        # Return monitor-reference pair (don't release lock yet)
        return next_mon, next_ref

    async def _polling(self) -> None:
        """
        Polling loop that checks for items getting stuck in the channel's
        monitor queue
        """
        while True:
            # Wait for polling delay
            await Timer(self.polling_ns, "ns")
            # Check for object at the front of the monitor queue
            if (
                (self.timeout_ns is not None)
                and (self._q_mon.level > 0)
                and ((age := (get_sim_time("ns") - self._q_mon.peek().timestamp)) > self.timeout_ns)
            ):
                self.log.error(
                    f"Object at the front of the of the {self.name} monitor "
                    f"queue has been stalled for {age} ns which exceeds the "
                    f"configured timeout of {self.timeout_ns} ns"
                )
                self.report()
                raise ChannelTimeoutError(f"Channel {self.name} timed out")

    async def loop(
        self,
        mismatch: ChannelComparisonCallback[TX_FILTERED],
        match: ChannelComparisonCallback[TX_FILTERED] | None = None,
    ) -> None:
        """
        Continuously dequeue pairs of transactions from the monitor and reference
        queues and report mismatches to the scoreboard via the callback.

        :param mismatch: Callback to execute whenever a mismatch occurs, providing
                         arguments of the channel (self), the monitor object, and
                         the reference object
        """
        while True:
            # Wait for a monitor-reference pair to be dequeued
            mon, ref = await self._dequeue()
            if mon != ref:
                self._mismatched += 1
                mismatch(self, mon, ref)
            else:
                self._matched += 1
                if match is not None:
                    match(self, mon, ref)
            # Release the lock after the comparison completes
            self._lock.release()

    async def drain(self) -> None:
        """Block until the channel's monitor and reference queues empty"""
        # Start draining
        match self.drain_policy:
            case DrainPolicy.MON_AND_REF:
                while self.monitor_depth > 0 or self.reference_depth > 0:
                    await RisingEdge(self.monitor.clk)
            case DrainPolicy.MON_ONLY:
                while self.monitor_depth > 0:
                    await RisingEdge(self.monitor.clk)
            case DrainPolicy.REF_ONLY:
                while self.reference_depth > 0:
                    await RisingEdge(self.monitor.clk)
        # Helper messages to flag if reference or monitor queues contain entries
        if self.reference_depth > 0:
            self.log.warning(
                f"Reference queue of scoreboard channel {self.name} still contains "
                f"{self.reference_depth} items but is ignored by the draining mode"
            )
        if self.monitor_depth > 0:
            self.log.warning(
                f"Monitor queue of scoreboard channel {self.name} still contains "
                f"{self.monitor_depth} items but is ignored by the draining mode"
            )
        # Wait for the lock to ensure a comparison is not still underway
        await self._lock.acquire()
        # Release the lock (in case we call drain multiple times)
        self._lock.release()

    def report(self) -> None:
        """
        Report the status of this channel detailing number of entries in the
        monitor and reference queues, and the queued transactions at the head
        of those queues
        """
        # Report matches/mismatches
        self.log.info(
            f"Channel {self.name} paired {self.total} transactions of which "
            f"{self.mismatched} mismatches were detected"
        )
        # Report outstanding transactions
        mon_depth = self.monitor_depth
        ref_depth = self.reference_depth
        if mon_depth > 0 or ref_depth > 0:
            self.log.error(
                f"Channel {self.name} has {mon_depth} entries captured from "
                f"monitor and {ref_depth} entries queued from reference model"
            )
        if mon_depth > 0:
            self.log.info(f"Packet at head of {self.name}'s monitor queue:")
            self.log.info(self._q_mon.peek().tabulate())
        if ref_depth > 0:
            self.log.info(f"Packet at head of {self.name}'s reference queue:")
            self.log.info(self._q_ref.peek().tabulate())


class FunnelChannel(Channel[TX, TX_FILTERED]):
    """
    An extended scoreboard channel where the order of data exiting the monitor
    is not strictly defined, often due to the hardware interleaving different
    streams. Reference data can be pushed into one or more named queues and when
    monitor packets arrive any queue head is valid.

    :param name:         Name of this scoreboard channel
    :param monitor:      Handle to the monitor capturing traffic from the DUT
    :param log:          Handle to the scoreboard log
    :param filter_fn:    Function to filter or modify captured transactions
    :param ref_queues:   List of reference queue names
    :param timeout_ns:   Optional timeout to allow for a object sat at the front
                         of the monitor queue to remain unmatched (in nanoseconds,
                         a value of None disables the timeout mechanism)
    :param polling_ns:   How frequently to poll to check for unmatched items stuck
                         in the monitor queue in nanoseconds (defaults to 100 ns)
    :param drain_policy: Specify how the handle should handle draining and which
                         queues should be considered when blocking testcase
                         shutdown
    """

    def __init__(
        self,
        name: str,
        monitor: BaseMonitor[TX],
        log: Logger,
        filter_fn: TransactionFilter[TX, TX_FILTERED],
        ref_queues: Iterable[str],
        timeout_ns: int | None = None,
        polling_ns: int = 100,
        drain_policy: DrainPolicy = DrainPolicy.MON_AND_REF,
    ) -> None:
        super().__init__(
            name,
            monitor,
            log,
            filter_fn,
            timeout_ns=timeout_ns,
            polling_ns=polling_ns,
            drain_policy=drain_policy,
        )
        self._q_ref = {x: Queue[TX_FILTERED]() for x in ref_queues}

    @property
    def reference_depth(self) -> int:
        return sum(x.level for x in self._q_ref.values())

    if TYPE_CHECKING:

        @overload
        def push_reference(self, *transactions: TX_FILTERED) -> None: ...

    @overload
    def push_reference(self, queue: str, *transactions: TX_FILTERED) -> None: ...

    def push_reference(self, queue: str | TX_FILTERED, *transactions: TX_FILTERED) -> None:
        """
        Push one or more captured transactions into a given reference (model)
        queue.

        :param queue:         Name of the queue to push into
        :param *transactions: Captured transactions
        """
        if isinstance(queue, BaseTransaction):
            raise ValueError("Nameless queues are not supported by FunnelChannel")
        assert isinstance(queue, str) and queue in self._q_ref
        for transaction in transactions:
            assert isinstance(transaction, BaseTransaction)
            self._q_ref[queue].push(transaction)

    async def _dequeue(self) -> tuple[TX_FILTERED, TX_FILTERED]:
        """
        Dequeue the top-most transaction from both the monitor and reference
        queues, searching through the reference queues for a matching object to
        the monitor's captured transaction.

        :returns: Tuple of the monitor transaction and reference transaction
        """
        # Wait for monitor to capture a transaction
        await self._q_mon.wait_for_not_empty()
        # Once a monitor transaction arrives, lock out the drain procedure
        await self._lock.acquire()
        # Peek at the front of all of the queues
        while True:
            next_mon = self._q_mon.peek()
            any_empty = False
            queue = None
            for queue in self._q_ref.values():
                any_empty = any_empty or (queue.level == 0)
                if queue.level > 0 and queue.peek() == next_mon:
                    await self._q_mon.pop()
                    next_ref = await queue.pop()
                    return next_mon, next_ref
            if queue is None:
                raise RuntimeError("No reference queue")
            # If all queues contain objects but none matched, this is a mismatch!
            # NOTE: This will just pop the final queue in order to report miscompare
            if not any_empty:
                await self._q_mon.pop()
                next_ref = await queue.pop()
                return next_mon, next_ref
            # Wait for a reference object to be pushed to any queue
            await First(*(x.on_push_event.wait() for x in self._q_ref.values()))

    def report(self) -> None:
        """
        Report the status of this channel detailing number of entries in the
        monitor and reference queues, and the queued transactions at the head
        of those queues
        """
        # Report matches/mismatches
        self.log.info(
            f"Channel {self.name} paired {self.total} transactions of which "
            f"{self.mismatched} mismatches were detected"
        )
        # Report outstanding transactions
        mon_depth = self.monitor_depth
        ref_depth = self.reference_depth
        if mon_depth > 0 or ref_depth > 0:
            self.log.error(
                f"Channel {self.name} has {mon_depth} entries captured from "
                f"monitor and {ref_depth} entries queued from reference model"
            )
        if mon_depth > 0:
            self.log.info(f"Packet at head of {self.name}'s monitor queue:")
            self.log.info(self._q_mon.peek().tabulate())
        for key, queue in self._q_ref.items():
            if queue.level > 0:
                self.log.info(f"Packet at head of {self.name}'s reference queue '{key}':")
                self.log.info(queue.peek().tabulate())


class MiscompareError(Exception, Generic[TX, TX_FILTERED]):
    """
    Raises a miscomparison as an exception with associated data.

    :param channel:   Channel that the miscompare occurred on
    :param monitor:   Transaction captured by the monitor
    :param reference: Expected transaction queued by the model
    """

    def __init__(
        self, channel: Channel[TX, TX_FILTERED], monitor: TX_FILTERED, reference: TX_FILTERED
    ) -> None:
        super().__init__()
        self.channel = channel
        self.monitor = monitor
        self.reference = reference


T = TypeVar("T")


class SizedIterable(Iterable[T], Sized):
    pass


class Scoreboard:
    """
    Scoreboard for comparing captured and reference transactions. The scoreboard
    is divided into different 'channels', one per monitor, that handle the
    comparison of captured and reference objects and reporting of any differences.

    :param tb:         Reference to the testbench
    :param fail_fast:  Stop the test as soon as a mismatch is reported (default: False)
    :param postmortem: Enter debug on a mismatch (default: False)
    """

    def __init__(
        self,
        tb: "BaseBench",
        fail_fast: bool = False,
        postmortem: bool = False,
    ):
        self.fail_fast = fail_fast
        self.postmortem = postmortem
        self._mismatches = []
        self.tb = tb
        self.log = self.tb.fork_log("scoreboard")
        self.channels: dict[str, Channel] = {}

    def attach(
        self,
        monitor: BaseMonitor[TX],
        filter_fn: TransactionFilter[TX, TX_FILTERED] = lambda mon, evt, obj: obj,
        queues: SizedIterable[str] | None = None,
        timeout_ns: int | None = None,
        polling_ns: int = 100,
        drain_policy: DrainPolicy = DrainPolicy.MON_AND_REF,
        match_window: int = 1,
    ) -> None:
        """
        Attach a monitor to the scoreboard, creating and scheduling a new
        channel in the process.

        :param monitor:      The monitor to attach
        :param queues:       List of reference queue names, this causes a funnel
                             type scoreboard channel to be used
        :param filter_fn:    A filter function that can either drop or modify items
                             recorded by the monitor prior to scoreboarding
        :param timeout_ns:   Optional timeout to allow for a object sat at the front
                             of the monitor queue to remain unmatched (in nanoseconds,
                             a value of None disables the timeout mechanism)
        :param polling_ns:   How frequently to poll to check for unmatched items stuck
                             in the monitor queue in nanoseconds (defaults to 100 ns)
        :param drain_policy: Specify how the handle should handle draining and
                             which queues should be considered when blocking
                             testcase shutdown
        :param match_window: Where precise ordering of expected transactions is not
                             known, a positive integer matching window can be used
                             to match any of the next N transactions in the reference
                             queue (where N is set by match_window)
        """
        assert monitor.name not in self.channels, f"Monitor known for '{monitor.name}'"
        if isinstance(queues, Iterable) and len(queues) > 0:
            channel = FunnelChannel(
                monitor.name,
                monitor,
                self.tb.fork_log("scoreboard", "channel", monitor.name),
                filter_fn,
                queues,
                timeout_ns=timeout_ns,
                polling_ns=polling_ns,
                drain_policy=drain_policy,
            )
        else:
            channel = Channel(
                monitor.name,
                monitor,
                self.tb.fork_log("scoreboard", "channel", monitor.name),
                filter_fn,
                timeout_ns=timeout_ns,
                polling_ns=polling_ns,
                drain_policy=drain_policy,
                match_window=match_window,
            )
        self.channels[channel.name] = channel
        if self.log.getEffectiveLevel() <= logging.DEBUG:
            cocotb.start_soon(channel.loop(self._mismatch, self._match))
        else:
            cocotb.start_soon(channel.loop(self._mismatch))

    async def drain(self) -> None:
        """Block until all chains of the scoreboard have been drained"""
        for name, channel in self.channels.items():
            self.log.info(f"Draining scoreboard channel '{name}'...")
            await channel.drain()
            self.log.info(f"Drained scoreboard channel '{name}'")

    def _mismatch(
        self, channel: Channel[TX, TX_FILTERED], monitor: TX_FILTERED, reference: TX_FILTERED
    ) -> None:
        """
        Callback whenever a channel detects a mismatch between captured and
        reference objects. If fail_fast is enabled, then an assertion will fire.

        :param channel:   The channel reporting the mismatch
        :param monitor:   The transaction captured by a monitor
        :param reference: The reference transaction produced by a model
        """
        self.log.error(
            f"Mismatch on channel {channel.monitor.name} for transaction index "
            f"{channel.total-1}"
        )
        self.log.info(monitor.tabulate(reference))
        self._mismatches.append((channel, monitor, reference))
        if self.fail_fast:
            if self.postmortem:
                self.log.warning(
                    "Entering fast-fail postmortem - the 'monitor' variable gives "
                    "the captured transaction, 'reference' the reference "
                    "transaction, and 'channel' gives access to the scoreboard "
                    "channel"
                )
                breakpoint()
            raise MiscompareError(channel, monitor, reference)

    def _match(
        self, channel: Channel[TX, TX_FILTERED], monitor: TX_FILTERED, reference: TX_FILTERED
    ) -> None:
        """
        Callback whenever a channel detects a match between captured and
        reference objects, only called if monitor attached with DEBUG verbosity.

        :param channel:   The channel reporting the mismatch
        :param monitor:   The transaction captured by a monitor
        :param reference: The reference transaction produced by a model
        """
        self.log.debug(
            f"Match on channel {channel.monitor.name} for transaction index {channel.total-1}"
        )
        self.log.debug(monitor.tabulate(reference))

    @property
    def result(self) -> bool:
        """If no mismatches recorded return True, else return False"""
        return len(self._mismatches) == 0
