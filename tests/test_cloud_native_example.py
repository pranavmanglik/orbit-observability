# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Verify the local cloud-native example across secrets and all telemetry signals."""

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from examples.cloud_native import verify_example


@pytest.mark.asyncio
async def test_cloud_native_example_resolves_secret_and_correlates_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use immediate fake exporters so this contract test needs no worker-pool threads."""

    async def run_inline(function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return function(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", run_inline)
    await verify_example()
