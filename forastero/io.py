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
from enum import Enum, IntEnum, auto
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    Protocol,
    dataclass_transform,
    overload,
    runtime_checkable,
)

from cocotb.handle import HierarchyObject, SimHandleBase

SignalValue = bool | int


class IORole(IntEnum):
    """Role that a particular bus is performing (determines signal suffix)"""

    INITIATOR = 0
    RESPONDER = 1

    @staticmethod
    def opposite(value: "IORole") -> "IORole":
        return {IORole.INITIATOR: IORole.RESPONDER, IORole.RESPONDER: IORole.INITIATOR}[value]


class SignalWrapper:
    """
    Some simulators supported by cocotb give visibility into the fields of a
    struct (e.g. Cadence Xcelium), while others only expose it as a packed
    bitvector. Forastero is built to assume the signal is a packed bitvector,
    and this wrapper class normalises the behaviour.

    :param hier: The signal hierarchy object
    """

    def __init__(self, hier: HierarchyObject | SimHandleBase) -> None:
        self._hier = hier

        # Recursively discover all components (in case of a nested struct)
        def _expand(level):
            if isinstance(level, HierarchyObject):
                for comp in level:
                    yield from _expand(comp)
            else:
                yield level

        all_components = list(_expand(self._hier))
        # Figure out how the bit fields pack into the bit vector, precalculating
        # the MSB, LSB, and mask to save compute later
        self._packing = []
        self._width = 0
        for comp in all_components[::-1]:
            # cocotb 2.X support
            if getattr(comp, "range", None) is not None:
                if comp.range.direction == "downto":
                    c_msb, c_lsb = comp.range.left, comp.range.right
                else:
                    c_msb, c_lsb = comp.range.right, comp.range.left
            # cocotb 1.X support
            elif getattr(comp, "_range", None) is not None:
                c_msb, c_lsb = comp._range
            # Fallback to the length of the component if no range is available
            else:
                c_msb, c_lsb = len(comp) - 1, 0
            rel_msb, rel_lsb = self._width + c_msb, self._width + c_lsb
            width = len(comp)
            self._packing.append(((rel_lsb, rel_msb, (1 << width) - 1), comp))
            self._width += width

    @property
    def value(self) -> int:
        value = 0
        for (lsb, _, _), comp in self._packing:
            value |= int(comp.value) << lsb
        return value

    @value.setter
    def value(self, value: int) -> None:
        for (lsb, _, mask), comp in self._packing:
            comp.value = (value >> lsb) & mask

    @property
    def _range(self) -> tuple[int, int]:
        return 0, self._width - 1

    def __len__(self) -> int:
        return self._width


@runtime_checkable
class IOStyle(Protocol):
    def __call__(
        self, bus: str | None, component: str, role_bus: IORole, role_component: IORole, /
    ) -> str: ...


def io_prefix_style(bus: str | None, component: str, role_bus: IORole, role_comp: IORole) -> str:
    """
    Style signal names as i/o_(<BUS>_)<COMPONENT> for example i_dma_awaddr and
    o_dma_awready.

    :param bus:       Name of the bus instance or None if not required
    :param component: Name of component signal withim the bus
    :param role_bus:  Interface role of the entire bus instance
    :param role_comp: Interface role of the component within the bus
    :returns:         Complete string name
    """
    mapping = {
        (IORole.INITIATOR, IORole.INITIATOR): "o",
        (IORole.INITIATOR, IORole.RESPONDER): "i",
        (IORole.RESPONDER, IORole.INITIATOR): "i",
        (IORole.RESPONDER, IORole.RESPONDER): "o",
    }
    full_name = f"{mapping[role_bus, role_comp]}"
    if bus is not None:
        full_name += f"_{bus}"
    return f"{full_name}_{component}"


def io_suffix_style(bus: str | None, component: str, role_bus: IORole, role_comp: IORole) -> str:
    """
    Style signal names as (<BUS>_)<COMPONENT>_i/o for example dma_awaddr_i and
    dma_awready_o.

    :param bus:       Name of the bus instance or None if not required
    :param component: Name of component signal withim the bus
    :param role_bus:  Interface role of the entire bus instance
    :param role_comp: Interface role of the component within the bus
    :returns:         Complete string name
    """
    mapping = {
        (IORole.INITIATOR, IORole.INITIATOR): "o",
        (IORole.INITIATOR, IORole.RESPONDER): "i",
        (IORole.RESPONDER, IORole.INITIATOR): "i",
        (IORole.RESPONDER, IORole.RESPONDER): "o",
    }
    full_name = f"{bus}_" if bus is not None else ""
    return f"{full_name}{component}_{mapping[role_bus, role_comp]}"


def io_plain_style(bus: str | None, component: str, role_bus: IORole, role_comp: IORole) -> str:
    """
    Style signal names as (<BUS>_)<COMPONENT> for example dma_awaddr and
    dma_awready.

    :param bus:       Name of the bus instance or None if not required
    :param component: Name of component signal withim the bus
    :param role_bus:  Interface role of the entire bus instance
    :param role_comp: Interface role of the component within the bus
    :returns:         Complete string name
    """
    del role_bus
    del role_comp
    if bus is None:
        return component
    else:
        return f"{bus}_{component}"


def _private(**kwargs) -> Any:
    pass


class _SignalMarker(Enum):
    INITIATOR = auto()
    RESPONDER = auto()


def initiator(init: Literal[False] = False) -> Any:
    return _SignalMarker.INITIATOR


def responder(init: Literal[False] = False) -> Any:
    return _SignalMarker.RESPONDER


@dataclass_transform(
    eq_default=False,
    field_specifiers=(
        _private,
        initiator,
        responder,
    ),
)
class BaseIOMeta(type):
    def __new__(cls, *args, **kwargs):
        cls = super().__new__(cls, *args, **kwargs)

        # Find initiator and responder signals
        cls._init_sigs = list[str]()
        cls._resp_sigs = list[str]()
        for attr in dir(cls):
            value = getattr(cls, attr)
            if value is _SignalMarker.INITIATOR:
                cls._init_sigs.append(attr)
                delattr(cls, attr)
            elif value is _SignalMarker.RESPONDER:
                cls._resp_sigs.append(attr)
                delattr(cls, attr)
        cls._sigs = cls._init_sigs + cls._resp_sigs
        return cls


class BaseIO(metaclass=BaseIOMeta):
    """
    Wraps a collection of different signals into a single interface that can be
    used by drivers and monitors to interact with the design.

    :param dut:       Pointer to the DUT boundary
    :param name:      Name of the signal - acts as a prefix
    :param role:      Role of this signal on the DUT boundary
    :param init_sigs: Signals driven by the initiator
    :param resp_sigs: Signals driven by the responder
    :param io_style:  Optionally override the default I/O naming style
    """

    _init_sigs: ClassVar[list[str]]
    _resp_sigs: ClassVar[list[str]]
    _sigs: ClassVar[list[str]]

    DEFAULT_IO_STYLE: ClassVar[IOStyle] = io_prefix_style

    _dut: HierarchyObject = _private(init=True, alias="dut")
    _name: str | None = _private(init=True, alias="name")
    _role: IORole = _private(init=True, alias="role")
    _io_style: IOStyle | None = _private(init=True, default=None, alias="io_style")
    _defaults: dict[str, SignalValue | None] = _private(init=False)
    __initiators: dict[str, SignalWrapper] = _private(init=False)
    __responders: dict[str, SignalWrapper] = _private(init=False)

    def __init__(
        self,
        dut: HierarchyObject,
        name: str | None,
        role: IORole,
        io_style: IOStyle | None = None,
    ) -> None:
        # Sanity checks
        assert role in IORole, f"Role {role} is not recognised"
        assert isinstance(io_style, IOStyle | None), "IO style does not fit the interface"
        # Hold onto attributes
        self._dut = dut
        self._name = name
        self._role = role
        self._defaults = dict[str, SignalValue | None]()
        # If no IO style provided, adopt the default
        io_style = io_style or BaseIO.DEFAULT_IO_STYLE
        # Pickup all initiator and response signals wrapping each inside a
        # SignalWrapper to normalise its behaviour across simulators
        self.__initiators = dict[str, SignalWrapper]()
        self.__responders = dict[str, SignalWrapper]()
        for comp in self._init_sigs:
            sig = io_style(self._name, comp, self._role, IORole.INITIATOR)
            if not hasattr(self._dut, sig):
                logging.getLogger("tb").getChild(f"io.{type(self).__name__.lower()}").info(
                    f"{type(self).__name__}: Did not find I/O component {sig} on {dut}"
                )
                continue
            sig_ptr = SignalWrapper(getattr(self._dut, sig))
            self.__initiators[comp] = sig_ptr
        for comp in self._resp_sigs:
            sig = io_style(self._name, comp, self._role, IORole.RESPONDER)
            if not hasattr(self._dut, sig):
                logging.getLogger("tb").getChild(f"io.{type(self).__name__.lower()}").info(
                    f"{type(self).__name__}: Did not find I/O component {sig} on {dut}"
                )
                continue
            sig_ptr = SignalWrapper(getattr(self._dut, sig))
            self.__responders[comp] = sig_ptr

    @property
    def role(self) -> IORole:
        return self._role

    @property
    def dut(self) -> HierarchyObject:
        return self._dut

    def initialise(self, role: IORole) -> None:
        """Initialise signals according to the active role"""
        for sig in (self.__initiators if role == IORole.INITIATOR else self.__responders).values():
            sig.value = 0

    def set_default(self, comp: str, value: SignalValue | None) -> None:
        """
        Set the default value to be returned for a signal if it is not available.

        :param comp:  Component name
        :param value: Value to return
        """
        self._defaults[comp] = value

    def has(self, comp: str) -> bool:
        """
        Test whether a particular signal has been resolved inside the interface.

        :param comp: Name of the component
        :returns:    True if exists, False otherwise
        """
        return (comp in self.__initiators) or (comp in self.__responders)

    def get_signal(self, comp: str) -> SignalWrapper | None:
        if comp in self.__initiators:
            return self.__initiators[comp]
        elif comp in self.__responders:
            return self.__responders[comp]
        else:
            return None

    @overload
    def get(self, comp: str, default: SignalValue) -> SignalValue: ...
    @overload
    def get(self, comp: str) -> SignalValue | None: ...
    def get(self, comp: str, default: SignalValue | None = None) -> SignalValue | None:
        """
        Get the current value of a particular signal.

        :param comp:    Name of the component
        :param default: Default value if the signal is not resolved
        :returns:       The resolved value, otherwise the default
        """
        if signal := self.get_signal(comp):
            raw = int(signal.value)
            return (raw == 1) if len(signal) == 1 else raw
        else:
            return self._defaults.get(comp, None) if default is None else default

    def set(self, comp: str, value: SignalValue) -> None:
        """
        Set the value of a particular signal if it exists.

        :param comp:  Name of the component
        :param value: Value to set
        """
        if signal := self.get_signal(comp):
            signal.value = value

    # NOTE: The type checker acts as if there is arbitrary attribute access if
    #       these methods are visible
    if not TYPE_CHECKING:

        def __getattr__(self, name: str) -> Any:
            if name in self._sigs:
                return self.get(name)
            else:
                raise AttributeError(f"Class {self.__class__.__name__} has no attribute {name}")

        def __setattr__(self, name: str, value: Any):
            if name in self._sigs:
                self.set(name, value)
            elif hasattr(self, name):
                super().__setattr__(name, value)
            else:
                raise AttributeError(f"Class {self.__class__.__name__} has no attribute {name}")

    def width(self, comp: str) -> int:
        """
        Return the width of a particular signal.

        :param comp: Name of the component
        :returns:    The bit width if resolved, else 0
        """
        if signal := self.get_signal(comp):
            return max(signal._range) - min(signal._range) + 1
        else:
            return 0
