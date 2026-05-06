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

import asyncio
from collections import defaultdict
from collections.abc import Coroutine
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, Literal, Protocol, Self, TypeVar, cast, overload

import cocotb
from cocotb.triggers import Event, Trigger

_Event = TypeVar("_Event", infer_variance=True, bound=Enum)
_Payload = TypeVar("_Payload", infer_variance=True)
_EventEmitter = TypeVar("_EventEmitter", infer_variance=True)


class DataEvent(Event, Generic[_Payload]):
    def __init__(self, *args, **kwds):
        super().__init__(*args, **kwds)

    @overload
    def set(self, data: _Payload):
        ...

    if TYPE_CHECKING:
        @overload
        def set(self, data: object | None = None):
            ...

    def set(self, data: Any = None):
        super().set()
        self.payload = cast(_Payload, data)

    @property
    def data(self) -> _Payload:
        """Alias for the event payload."""
        return self.payload

    @data.setter
    def data(self, new_data: _Payload):
        self.payload = new_data

class EventHandler(Protocol, Generic[_EventEmitter, _Event, _Payload]):
    def __call__(
        self, emitter: _EventEmitter, event: _Event, obj: _Payload, /
    ) -> None | Coroutine[Trigger, None, None]: ...


class EventEmitter(Generic[_Event, _Payload]):
    """Core support for publishing events and subscribing to them"""

    _handlers: defaultdict[_Event | Literal["*"], list[EventHandler[Self, _Event, _Payload]]]
    _ready: DataEvent[_Payload]
    _waiting: defaultdict[_Event, list[DataEvent[_Payload]]]

    def __init__(self) -> None:
        self._handlers = defaultdict(list)
        self._ready = DataEvent()
        self._waiting = defaultdict(list)

    def subscribe(self, event: _Event, callback: EventHandler[Self, _Event, _Payload]) -> None:
        """
        Subscribe to an event being published by this component.

        :param event:    Enumerated event
        :param callback: Method to call when the event occurs, this must accept
                         arguments of component, event type, and an associated
                         object
        """
        if not isinstance(event, Enum):
            raise TypeError(f"Event should inherit from Enum, unlike {event}")
        self._handlers[event].append(callback)

    def subscribe_all(self, callback: EventHandler[Self, _Event, _Payload]) -> None:
        """
        Subscribe to all events published by this component.

        :param callback: Method to call when the event occurs, this must accept
                         arguments of component, event type, and an associated
                         object
        """
        self._handlers["*"].append(callback)

    def unsubscribe_all(self, event: _Event | None = None) -> None:
        """
        De-register all subscribers for a given event (when event is not None),
        or all subscribers from all events (when event is None).

        :param event: Optional enumerated event to unsubscribe, or None to
                      unsubscribe all subscribers for all events
        """
        if event is None:
            self._handlers.clear()
            self._waiting.clear()
        elif not isinstance(event, Enum):
            raise TypeError(f"Event should inherit from Enum, unlike {event}")
        else:
            self._handlers[event].clear()
            self._waiting[event].clear()

    def publish(self, event: _Event, obj: _Payload) -> None:
        """
        Publish an event and deliver it to any registered subscribers.

        :param event: Enumerated event
        :param obj:   Object associated to the event
        """
        # Call direct handlers
        for handler in self._handlers["*"] + self._handlers[event]:
            call = handler(self, event, obj)
            if asyncio.iscoroutine(call):
                cocotb.start_soon(call)
        # Trigger pending events
        pending = self._waiting[event][:]
        self._waiting[event].clear()
        for evt in pending:
            evt.set(data=obj)

    def _get_wait_event(self, event: _Event) -> DataEvent[_Payload]:
        evt = DataEvent()
        self._waiting[event].append(evt)
        return evt

    async def wait_for(self, event: _Event) -> _Payload:
        """
        Wait for a specific enumerated event to occur and return the data that
        was associated to it.

        :param event: Enumerated event trigger
        :returns:     Data associated with the event
        """
        evt = self._get_wait_event(event)
        await evt.wait()
        return evt.payload
