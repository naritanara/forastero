# Support for cocotb 2.X
try:
    from cocotb.logging import (  # pyright: ignore[reportMissingImports]
        SimLogFormatter,
        SimTimeContextFilter,
    )
    from cocotb.triggers import SimTimeoutError  # pyright: ignore[reportMissingImports]
# Fallback for cocotb 1.X
except ImportError:
    from cocotb.log import (  # pyright: ignore[reportMissingImports]
        SimLogFormatter,
        SimTimeContextFilter,
    )
    from cocotb.result import SimTimeoutError  # pyright: ignore[reportMissingImports]

from . import typing

__all__ = [
    "SimLogFormatter",
    "SimTimeContextFilter",
    "SimTimeoutError",
    "typing",
]
