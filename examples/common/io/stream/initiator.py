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

from cocotb.triggers import RisingEdge

from forastero.driver import BaseDriver

from .io import StreamIO
from .transaction import StreamTransaction


class StreamInitiator(BaseDriver[StreamTransaction, StreamIO]):
    async def drive(self, obj: StreamTransaction) -> None:
        self.io.data = obj.data
        self.io.valid = True
        while True:
            await RisingEdge(self.clk)
            if self.io.ready:
                break
        self.io.valid = False
