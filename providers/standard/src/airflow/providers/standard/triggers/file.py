# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import asyncio
import datetime
import fnmatch
import hashlib
import json
import logging
import os
from collections.abc import AsyncIterator, Hashable
from glob import glob
from typing import Any

import anyio

from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.triggers.base import BaseEventTrigger, BaseTrigger, TriggerEvent
else:
    from airflow.triggers.base import (  # type: ignore
        BaseTrigger,
        BaseTrigger as BaseEventTrigger,
        TriggerEvent,
    )

log = logging.getLogger(__name__)

_WATERMARK_KEY_PREFIX = "directory_file_watermark"


class FileTrigger(BaseTrigger):
    """
    A trigger that fires exactly once after it finds the requested file or folder.

    :param filepath: File or folder name (relative to the base path set within the connection), can
        be a glob.
    :param recursive: when set to ``True``, enables recursive directory matching behavior of
        ``**`` in glob filepath parameter. Defaults to ``False``.
    :param poke_interval: Time that the job should wait in between each try
    """

    def __init__(
        self,
        filepath: str,
        recursive: bool = False,
        poke_interval: float = 5.0,
        **kwargs,
    ):
        super().__init__()
        self.filepath = filepath
        self.recursive = recursive
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize FileTrigger arguments and classpath."""
        return (
            "airflow.providers.standard.triggers.file.FileTrigger",
            {
                "filepath": self.filepath,
                "recursive": self.recursive,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the relevant files are found."""
        while True:
            for path in glob(self.filepath, recursive=self.recursive):
                if await anyio.Path(path).is_file():
                    mod_time_f = (await anyio.Path(path).stat()).st_mtime
                    mod_time = datetime.datetime.fromtimestamp(mod_time_f).strftime("%Y%m%d%H%M%S")
                    self.log.info("Found File %s last modified: %s", path, mod_time)
                    yield TriggerEvent(True)
                    return
                for _, _, files in await anyio.to_thread.run_sync(lambda p: list(os.walk(p)), path):
                    if files:
                        yield TriggerEvent(True)
                        return
            await asyncio.sleep(self.poke_interval)


class FileDeleteTrigger(BaseEventTrigger):
    """
    A trigger that fires exactly once after it finds the requested file and then delete the file.

    The difference between ``FileTrigger`` and ``FileDeleteTrigger`` is ``FileDeleteTrigger`` can only find a
    specific file.

    :param filepath: File (relative to the base path set within the connection).
    :param poke_interval: Time that the job should wait in between each try
    """

    def __init__(
        self,
        filepath: str,
        poke_interval: float = 5.0,
        **kwargs,
    ):
        super().__init__()
        self.filepath = filepath
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize FileDeleteTrigger arguments and classpath."""
        return (
            "airflow.providers.standard.triggers.file.FileDeleteTrigger",
            {
                "filepath": self.filepath,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the relevant file is found."""
        while True:
            filepath = anyio.Path(self.filepath)
            if await filepath.is_file():
                mod_time_f = (await filepath.stat()).st_mtime
                mod_time = datetime.datetime.fromtimestamp(mod_time_f).strftime("%Y%m%d%H%M%S")
                self.log.info("Found file %s last modified: %s", self.filepath, mod_time)
                await filepath.unlink()
                self.log.info("File %s has been deleted", self.filepath)
                yield TriggerEvent(True)
                return
            await asyncio.sleep(self.poke_interval)


class DirectoryFileDeleteTrigger(BaseEventTrigger):
    """
    Fire once when ``filename`` appears in ``directory``, then delete it.

    Functionally equivalent to ``FileDeleteTrigger`` for a single file, but
    sibling triggers that point at the same ``directory`` and ``poke_interval``
    share a single underlying directory scan in the triggerer; each instance
    only fires for its own ``filename``. This is useful when many assets are
    driven by per-flag-file events landing in a shared inbox directory.

    :param directory: Directory to scan.
    :param filename: File name (without directory) whose appearance fires this
        trigger. The matched file is deleted before the event is yielded.
    :param poke_interval: Time to wait between scans.
    """

    def __init__(self, *, directory: str, filename: str, poke_interval: float = 5.0) -> None:
        super().__init__()
        self.directory = directory
        self.filename = filename
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize DirectoryFileDeleteTrigger arguments and classpath."""
        return (
            "airflow.providers.standard.triggers.file.DirectoryFileDeleteTrigger",
            {
                "directory": self.directory,
                "filename": self.filename,
                "poke_interval": self.poke_interval,
            },
        )

    def shared_stream_key(self) -> Hashable | None:
        """All triggers on the same directory + cadence share one scan."""
        # Use realpath so trivial path variants all resolve to the same canonical
        # path: trailing slashes (``/tmp/flags`` vs ``/tmp/flags/``), relative vs
        # absolute paths (``./flags`` vs ``/tmp/flags``), and symlinks vs their
        # targets all key to the same group instead of running N independent scans.
        return ("directory-scan", os.path.realpath(self.directory), self.poke_interval)

    @classmethod
    async def open_shared_stream(cls, kwargs: dict[str, Any]) -> AsyncIterator[Any]:
        """
        Drive one directory-listing loop and broadcast each snapshot.

        Missing directories yield an empty snapshot so subscribers keep
        polling for the file to appear. Configuration-class failures
        (``PermissionError``, ``NotADirectoryError``, ``IsADirectoryError``)
        propagate — these are almost always permanent (wrong mount, wrong
        mode, path points at a file), so silently retrying just hides the
        misconfiguration from the operator; surfacing them as a
        ``_PollFailure`` makes the trigger visibly fail in the UI, where it
        can be diagnosed and restarted after the operator corrects the
        config. Other ``OSError`` subclasses (transient I/O blips, NFS
        hiccups, etc.) are logged at warning and the snapshot is skipped for
        this cadence, since those may self-heal.
        """
        directory = anyio.Path(kwargs["directory"])
        poke_interval: float = kwargs["poke_interval"]
        while True:
            try:
                names = {p.name async for p in directory.iterdir()}
            except FileNotFoundError:
                names = set()
            except (PermissionError, NotADirectoryError, IsADirectoryError):
                raise
            except OSError:
                log.warning(
                    "Failed to list %s; retrying after %ss",
                    directory,
                    poke_interval,
                    exc_info=True,
                )
                await asyncio.sleep(poke_interval)
                continue
            yield {"directory": str(directory), "names": names}
            await asyncio.sleep(poke_interval)

    async def filter_shared_stream(self, shared_stream: AsyncIterator[Any]) -> AsyncIterator[TriggerEvent]:
        """Fire once for this instance's own filename and delete the file."""
        async for snapshot in shared_stream:
            if self.filename not in snapshot["names"]:
                continue
            filepath = anyio.Path(snapshot["directory"]) / self.filename
            try:
                await filepath.unlink()
            except FileNotFoundError:
                # Lost a race with a sibling, or the file disappeared between
                # snapshot and unlink. Wait for the next scan.
                continue
            self.log.info("File %s has been deleted", filepath)
            yield TriggerEvent({"filepath": str(filepath)})
            return

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Standalone fallback when the shared-stream manager is unavailable.

        Mirrors the shared path so the trigger remains usable in unit tests
        and on Airflow versions without the manager wired in. It does not
        deduplicate I/O — that requires the triggerer to drive the shared
        stream.
        """
        kwargs = self.serialize()[1]
        async for event in self.filter_shared_stream(type(self).open_shared_stream(kwargs)):
            yield event


class DirectoryFileWatermarkTrigger(BaseEventTrigger):
    """
    Fire when files in ``directory`` are newer than the stored watermark.

    Unlike ``DirectoryFileDeleteTrigger``, matched files are left in place. The trigger keeps a watermark
    (the newest modification time seen, plus the names of the files that share it) in the asset state
    store, and fires once per scan with every matching file whose modification time is past the
    watermark. Files that are modified after they fired make the trigger fire again. Files written with
    an older modification time than the watermark (for example ``cp -p`` or ``rsync -t``) are not seen.

    The first scan, when there is no watermark yet, fires for every matching file already in the
    directory. Delivery is at-least-once: the watermark is saved after the event is handed to the
    triggerer, so a triggerer failure in between fires the same files again.

    Requires Airflow 3.3+ and an ``AssetWatcher`` on exactly one asset, since the watermark lives in that
    asset's state store.

    :param directory: Directory to scan. Subdirectories are not scanned.
    :param pattern: ``fnmatch`` pattern that file names must match.
    :param poke_interval: Time to wait between scans.
    """

    def __init__(
        self,
        *,
        directory: str,
        pattern: str = "*",
        poke_interval: float = 5.0,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.directory = directory
        self.pattern = pattern
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize DirectoryFileWatermarkTrigger arguments and classpath."""
        return (
            "airflow.providers.standard.triggers.file.DirectoryFileWatermarkTrigger",
            {
                "directory": self.directory,
                "pattern": self.pattern,
                "poke_interval": self.poke_interval,
            },
        )

    @property
    def _watermark_key(self) -> str:
        # Several watchers on one asset may point at different directories, so each gets its own key.
        identity = json.dumps(
            {"directory": os.path.realpath(self.directory), "pattern": self.pattern},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return f"{_WATERMARK_KEY_PREFIX}:{hashlib.sha256(identity).hexdigest()}"

    def _get_store(self) -> Any:
        store = getattr(self, "asset_state_store", None)
        if store is None:
            raise RuntimeError(
                "DirectoryFileWatermarkTrigger needs the asset state store, which requires Airflow 3.3+ "
                "and an AssetWatcher on exactly one asset"
            )
        return store

    async def _load_watermark(self, store: Any) -> tuple[int, set[str]] | None:
        try:
            # aget/aset landed in Airflow 3.3.2; 3.3.0 and 3.3.1 only expose the blocking API.
            if hasattr(store, "aget"):
                value = await store.aget(self._watermark_key)
            else:
                value = await asyncio.to_thread(store.get, self._watermark_key)
        except ValueError as e:
            raise RuntimeError(
                "DirectoryFileWatermarkTrigger must be used in an AssetWatcher on exactly one asset"
            ) from e
        if value is None:
            return None
        if (
            not isinstance(value, dict)
            or not isinstance(value.get("mtime_ns"), int)
            or not isinstance(value.get("names"), list)
        ):
            self.log.warning("Stored watermark for %s is invalid; scanning from the start", self.directory)
            return None
        return value["mtime_ns"], set(value["names"])

    async def _save_watermark(self, store: Any, mtime_ns: int, names: set[str]) -> None:
        value = {"mtime_ns": mtime_ns, "names": sorted(names)}
        if hasattr(store, "aset"):
            await store.aset(self._watermark_key, value)
        else:
            await asyncio.to_thread(store.set, self._watermark_key, value)

    async def _scan(self) -> dict[str, int]:
        """Return the modification time in nanoseconds of every matching file in the directory."""
        files: dict[str, int] = {}
        try:
            async for path in anyio.Path(self.directory).iterdir():
                if not fnmatch.fnmatch(path.name, self.pattern):
                    continue
                try:
                    if not await path.is_file():
                        continue
                    files[path.name] = (await path.stat()).st_mtime_ns
                except FileNotFoundError:
                    continue
        except FileNotFoundError:
            pass
        return files

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Scan the directory forever and fire for files past the watermark."""
        store = self._get_store()
        watermark = await self._load_watermark(store)
        while True:
            files = await self._scan()
            if watermark is None:
                new_names = set(files)
            else:
                wm_mtime_ns, wm_names = watermark
                new_names = {
                    name
                    for name, mtime_ns in files.items()
                    if mtime_ns > wm_mtime_ns or (mtime_ns == wm_mtime_ns and name not in wm_names)
                }
            if new_names:
                newest = max(files[name] for name in new_names)
                names_at_newest = {name for name in new_names if files[name] == newest}
                if watermark is not None and newest == watermark[0]:
                    names_at_newest |= watermark[1]
                self.log.info("Found %d new file(s) in %s", len(new_names), self.directory)
                yield TriggerEvent(
                    {
                        "directory": self.directory,
                        "filepaths": [os.path.join(self.directory, name) for name in sorted(new_names)],
                    }
                )
                watermark = (newest, names_at_newest)
                await self._save_watermark(store, *watermark)
            await asyncio.sleep(self.poke_interval)
