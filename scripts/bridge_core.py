#!/usr/bin/env python3

"""
bridge_core.py

Transport-agnostic bridge logic: config loading, field filtering, binary
codec, and the transmit queue/manager.  No ROS 2 imports and no hardware
imports — usable standalone or in tests without a device.

Packet envelope (little-endian):
    [2 bytes] sequence number  (added by TxManager / stripped by CommsDevice._process_received)
    [1 byte]  bridge_id (unsigned int, 0-255)
    [4 bytes] payload length
    [N bytes] payload  (binary-packed field values, schema known from bridge.yaml)

Payload encoding — field values are packed in the order they appear in the
bridge.yaml 'fields' list (or message-definition order if 'fields' is omitted).
No field names are transmitted; both sides reconstruct them from the shared
config.  Per-field wire format:

    primitive scalar    struct.pack('<fmt', value)          fixed size
    string              uint16 length + UTF-8 bytes         variable
    fixed array [N]     struct.pack('<N*fmt', *values)      fixed size
    dynamic array []    uint16 count + count*fmt bytes      variable

TODO: evaluate msgpack or CDR once field schema is fully locked down.
"""

import json
import logging
import os
import re
import struct
import threading
import time
import yaml
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


# ---------------------------------------------------------------------------
# Sequence number helpers
# ---------------------------------------------------------------------------

SEQ_MAX = 0xFFFF


def seq_next(current: int) -> int:
    return (current + 1) & SEQ_MAX


def seq_distance(a: int, b: int) -> int:
    """Signed distance from sequence number a to b, accounting for wrap."""
    d = (b - a) & SEQ_MAX
    return d if d <= SEQ_MAX // 2 else d - (SEQ_MAX + 1)


def wrap_with_seq(seq: int, raw_packet: bytes) -> bytes:
    """Prepend a 2-byte sequence number to a packet."""
    return struct.pack("<H", seq) + raw_packet


def strip_seq(framed: bytes) -> tuple[int, bytes]:
    """Split a framed packet back into (seq, raw_packet)."""
    seq = struct.unpack_from("<H", framed, 0)[0]
    return seq, framed[2:]


# ---------------------------------------------------------------------------
# Per-bridge transmit queue
# ---------------------------------------------------------------------------

@dataclass
class _QueuedPacket:
    seq: int
    data: bytes          # already framed with seq number
    attempts: int = 0
    enqueue_time: float = field(default_factory=time.monotonic)


class TxQueue:
    """
    A single bridge's transmit queue.

    Policy
    ------
    - Fixed-depth deque: when full, the *oldest* packet is dropped (keep latest).
    - Reliability mode:
        best_effort  - fire and forget; packet is removed after one send attempt.
        reliable     - packet stays in a pending-ack slot until ACK received or
                       max_retries exceeded; retried after ack_timeout seconds.
    - Thread-safe: all methods may be called from the ROS callback thread
      while the drain loop runs in the manager thread.
    """

    def __init__(
        self,
        bridge_id: int,
        address: Optional[str],        # XBee 64-bit address string, or None = broadcast
        priority: int,                 # lower = higher priority
        reliability: str,              # "best_effort" | "reliable"
        queue_depth: int,
        max_retries: int = 3,
        ack_timeout: float = 0.5,
    ):
        self.bridge_id: int = bridge_id
        self.address     = address
        self.priority    = priority
        self.reliability = reliability
        self.queue_depth = queue_depth
        self.max_retries = max_retries
        self.ack_timeout = ack_timeout

        self._lock   = threading.Lock()
        self._queue: deque[_QueuedPacket] = deque()
        self._seq    = 0

        # For reliable mode: the one packet waiting for an ACK.
        self._pending_ack: Optional[_QueuedPacket] = None
        self._pending_ack_time: float = 0.0

        # Stats
        self.stat_enqueued  = 0
        self.stat_dropped   = 0
        self.stat_sent      = 0
        self.stat_retried   = 0
        self.stat_failed    = 0

    def enqueue(self, raw_packet: bytes) -> None:
        """Add a packet to the queue, dropping the oldest if full."""
        with self._lock:
            self._seq = seq_next(self._seq)
            pkt = _QueuedPacket(seq=self._seq, data=wrap_with_seq(self._seq, raw_packet))
            if len(self._queue) >= self.queue_depth:
                self._queue.popleft()
                self.stat_dropped += 1
            self._queue.append(pkt)
            self.stat_enqueued += 1

    def acknowledge(self, seq: int) -> None:
        """
        Called when the receiver sends back an ACK for sequence number seq.
        Clears the pending-ack slot so the next packet can be sent.
        """
        with self._lock:
            if self._pending_ack and self._pending_ack.seq == seq:
                self._pending_ack = None

    def peek_next(self) -> Optional[_QueuedPacket]:
        """
        Return the next packet to transmit without removing it.

        For reliable mode: if a packet is pending ACK and hasn't timed out,
        returns None (nothing new to send yet).  If it has timed out,
        returns it again for retry.

        For best_effort: returns the front of the queue (or None).
        """
        with self._lock:
            if self.reliability == "reliable":
                if self._pending_ack is not None:
                    if time.monotonic() - self._pending_ack_time < self.ack_timeout:
                        return None  # not time to retry yet
                    # Timed out - retry or give up
                    if self._pending_ack.attempts >= self.max_retries:
                        self._pending_ack = None
                        self.stat_failed += 1
                        return self._queue[0] if self._queue else None
                    self._pending_ack.attempts += 1
                    self._pending_ack_time = time.monotonic()
                    self.stat_retried += 1
                    return self._pending_ack
                return self._queue[0] if self._queue else None
            else:
                return self._queue[0] if self._queue else None

    def pop_sent(self, pkt: _QueuedPacket) -> None:
        """
        Mark a packet as sent.  For best_effort, removes it.
        For reliable, moves it to the pending-ack slot.
        """
        with self._lock:
            if self._queue and self._queue[0].seq == pkt.seq:
                self._queue.popleft()
            if self.reliability == "reliable" and self._pending_ack is None:
                self._pending_ack = pkt
                self._pending_ack.attempts = 1
                self._pending_ack_time = time.monotonic()
            self.stat_sent += 1

    def has_pending(self) -> bool:
        with self._lock:
            return bool(self._queue) or self._pending_ack is not None


# ---------------------------------------------------------------------------
# Transmit manager
# ---------------------------------------------------------------------------

class TxManager:
    """
    Manages per-bridge queues and drives the transmit loop.

    Priority scheduling: bridges are grouped into priority tiers (lower number
    = higher priority).  The highest-priority non-empty tier is fully drained
    before lower tiers are touched.  Within a tier, queues are round-robined.

    The device passed in must implement:
        device.send(address: Optional[str], framed_data: bytes) -> bool
        device.set_receive_callback(fn: Callable[[int, int, bytes], None]) -> None
    """

    def __init__(self, device, logger=None, drain_interval: float = 0.01):
        self._device         = device
        self._log            = logger or logging.getLogger(__name__)
        self._drain_interval = drain_interval

        self._queues: dict[int, TxQueue] = {}
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def register_bridge(
        self,
        bridge_id: int,
        address: Optional[str]  = None,
        priority: int           = 5,
        reliability: str        = "best_effort",
        queue_depth: int        = 10,
        max_retries: int        = 3,
        ack_timeout: float      = 0.5,
    ) -> None:
        """
        Register a bridge with the manager.

        bridge_id   - unique unsigned int identifier (0-255).
        address     - XBee 64-bit hex string for unicast, or None for broadcast.
        priority    - integer tier; lower = higher priority.  Default 5.
        reliability - "best_effort" or "reliable".
        queue_depth - max queued packets; oldest dropped when exceeded.
        """
        if not (0 <= bridge_id <= 255):
            raise ValueError(f"bridge_id {bridge_id} out of range (must be 0-255).")
        if reliability == "reliable" and address is None:
            raise ValueError(
                f"Bridge {bridge_id}: reliability='reliable' requires a unicast address. "
                "Broadcast cannot receive ACKs — use reliability='best_effort' or set an address."
            )
        if bridge_id in self._queues:
            self._log.warning(f"TxManager: re-registering bridge '{bridge_id}'.")
        self._queues[bridge_id] = TxQueue(
            bridge_id=bridge_id, address=address, priority=priority,
            reliability=reliability, queue_depth=queue_depth,
            max_retries=max_retries, ack_timeout=ack_timeout,
        )
        self._log.info(
            f"Registered bridge {bridge_id} priority={priority} "
            f"reliability={reliability} depth={queue_depth}"
        )

    def start(self) -> None:
        """Start the background drain thread."""
        self._running = True
        self._thread  = threading.Thread(target=self._drain_loop, name="radio_tx_drain", daemon=True)
        self._thread.start()
        self._log.debug("TxManager drain thread started.")

    def stop(self) -> None:
        """Signal the drain thread to stop and wait for it."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
        self._log.info("TxManager stopped.")

    def enqueue(self, bridge_id: int, raw_packet: bytes) -> None:
        """
        Add raw_packet to the named bridge's queue.

        raw_packet should be the bytes produced by BridgeCore.pack_packet().
        The manager prepends the sequence number before transmitting.
        """
        q = self._queues.get(bridge_id)
        if q is None:
            self._log.warning(f"enqueue: unknown bridge_id {bridge_id}. Was register_bridge() called?")
            return
        q.enqueue(raw_packet)

    def ack_received(self, bridge_id: int, seq: int) -> None:
        """
        Notify the manager that the receiver ACKed sequence number seq
        for the given bridge.  Call this from the radio receive callback.
        """
        q = self._queues.get(bridge_id)
        if q:
            q.acknowledge(seq)

    def on_receive(
        self, bridge_id: int, seq: int, src_hw_addr: Optional[str] = None
    ) -> None:
        """
        Called for every inbound DATA packet.  Sends an ACK only for
        reliable bridges — best_effort bridges do not ACK.
        """
        q = self._queues.get(bridge_id)
        if q and q.reliability == "reliable":
            self._device._send_ack(src_hw_addr, bridge_id, seq)

    def get_stats(self) -> dict:
        """Return per-bridge TX queue statistics as a dict keyed by bridge_id."""
        result = {}
        for bridge_id, q in self._queues.items():
            with q._lock:
                queue_depth = len(q._queue)
            result[bridge_id] = {
                "priority":    q.priority,
                "reliability": q.reliability,
                "enqueued":    q.stat_enqueued,
                "dropped":     q.stat_dropped,
                "sent":        q.stat_sent,
                "retried":     q.stat_retried,
                "failed":      q.stat_failed,
                "queue_depth": queue_depth,
            }
        return result

    def log_stats(self) -> None:
        """Dump per-bridge queue statistics to the logger."""
        for bridge_id, s in self.get_stats().items():
            self._log.info(
                f"  bridge[{bridge_id}] priority={s['priority']} reliability={s['reliability']} "
                f"enqueued={s['enqueued']} dropped={s['dropped']} "
                f"sent={s['sent']} retried={s['retried']} failed={s['failed']} "
                f"queue_depth={s['queue_depth']}"
            )

    def _drain_loop(self) -> None:
        while self._running:
            self._drain_once()
            time.sleep(self._drain_interval)

    def _drain_once(self) -> None:
        """One full scheduling pass across all bridges."""
        # TODO understand this logic. This is a bit complex. 

        tiers: dict[int, list[TxQueue]] = {}
        for q in self._queues.values():
            tiers.setdefault(q.priority, []).append(q)

        for priority in sorted(tiers.keys()):
            tier_queues = tiers[priority]
            drained = True
            while drained:
                drained = False
                for q in tier_queues:
                    pkt = q.peek_next()
                    if pkt is None:
                        continue
                    success = self._device.send(q.address, pkt.data)
                    if success:
                        q.pop_sent(pkt)
                        drained = True
                        self._log.debug(
                            f"[{q.bridge_id}] sent seq={pkt.seq} "
                            f"({len(pkt.data)} bytes) priority={q.priority}"
                        )
                    elif q.reliability == "best_effort":
                        # Drop on failure — best_effort never retries
                        q.pop_sent(pkt)
                        self._log.warning(f"[{q.bridge_id}] send failed, dropping seq={pkt.seq} (best_effort)")
                    else:
                        # reliable — leave in queue, retry on next drain
                        self._log.warning(f"[{q.bridge_id}] send failed seq={pkt.seq}, will retry")
            if any(q.has_pending() for q in tier_queues):
                break


# ---------------------------------------------------------------------------
# Binary codec helpers
# ---------------------------------------------------------------------------

_PRIM_FMT: dict[str, str] = {
    'boolean': '?', 'bool':  '?',
    'octet':   'B', 'byte':  'B', 'char': 'B',
    'float':   'f', 'float32': 'f',
    'double':  'd', 'float64': 'd',
    'int8':    'b', 'uint8':   'B',
    'int16':   'h', 'uint16':  'H',
    'int32':   'i', 'uint32':  'I',
    'int64':   'q', 'uint64':  'Q',
}

_ARRAY_FIXED_RE = re.compile(r'^(.+)\[(\d+)\]$')       # "double[9]"  → elem, count
_ARRAY_DYN_RE   = re.compile(r'^(.+)\[(?:<=\d+)?\]$')  # "uint8[]" or "uint8[<=256]"


@dataclass
class _FieldSpec:
    path:  tuple          # attribute path from root, e.g. ('header', 'stamp', 'sec')
    kind:  str            # 'prim' | 'string' | 'fixed_array' | 'dyn_array'
    fmt:   str            # struct format char for prim/array elements; '' for string
    count: int            # element count for fixed_array; 0 otherwise


# ---------------------------------------------------------------------------
# BridgeCore
# ---------------------------------------------------------------------------

class BridgeCore:
    """
    Stateless utilities for config loading, ROS message field filtering,
    and binary packet serialization.  All methods are static.

    Binary codec
    ------------
    Call build_codec(msg_type, field_tree, type_resolver) once at startup to
    get (encode_fn, decode_fn).

        encode_fn(msg)       -> bytes  (payload only, no envelope)
        decode_fn(raw_bytes) -> dict   (pass to apply_dict_to_message)

    Pass the result of rosidl_runtime_py.utilities.get_message as type_resolver
    so nested message types can be resolved.
    """

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------

    @staticmethod
    def load_config(path: str) -> dict:
        """Load and validate the YAML bridge configuration."""
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path, "r") as f:
            cfg = yaml.safe_load(f)
        if "topics" not in cfg or not isinstance(cfg["topics"], list):
            raise ValueError("Config must have a top-level 'topics' list.")
        for entry in cfg["topics"]:
            for required in ("input", "output", "type", "id"):
                if required not in entry:
                    raise ValueError(f"Each topic entry must have '{required}'. Got: {entry}")
            bridge_id = int(entry["id"])
            if not (0 <= bridge_id <= 255):
                raise ValueError(f"bridge_id {bridge_id} out of range (must be 0-255).")
        return cfg

    # ------------------------------------------------------------------
    # Field filtering  (used for 'local' mode)
    # ------------------------------------------------------------------

    @staticmethod
    def build_field_tree(allowed_fields: list) -> dict:
        """
        Convert a flat list of dot-notation field specs into a nested dict tree.
        None as a value means "copy this whole field, no further filtering".

        ["vector.x", "header.stamp.sec"] →
            {"vector": {"x": None}, "header": {"stamp": {"sec": None}}}
        """
        tree = {}
        for spec in allowed_fields:
            parts = spec.split(".")
            node = tree
            for i, part in enumerate(parts):
                if part not in node:
                    node[part] = None
                if i < len(parts) - 1:
                    if node[part] is None:
                        node[part] = {}
                    node = node[part]
        return tree

    @staticmethod
    def _apply_field_tree(src_msg: Any, dst_msg: Any, tree: dict) -> None:
        """Recursively copy fields from src_msg into dst_msg according to tree."""
        try:
            valid_fields = set(src_msg.get_fields_and_field_types().keys())
        except AttributeError:
            return
        for field_name, subtree in tree.items():
            if field_name not in valid_fields:
                continue
            src_value = getattr(src_msg, field_name)
            if subtree is None:
                setattr(dst_msg, field_name, src_value)
            else:
                dst_value = getattr(dst_msg, field_name)
                BridgeCore._apply_field_tree(src_value, dst_value, subtree)
                setattr(dst_msg, field_name, dst_value)

    @staticmethod
    def filter_message(src_msg: Any, field_tree: Optional[dict]) -> Any:
        """Return a copy of src_msg with only the fields in field_tree."""
        if not field_tree:
            return src_msg
        dst_msg = type(src_msg)()
        BridgeCore._apply_field_tree(src_msg, dst_msg, field_tree)
        return dst_msg

    # ------------------------------------------------------------------
    # Binary codec
    # ------------------------------------------------------------------

    @staticmethod
    def build_codec(
        msg_type,
        field_tree: Optional[dict],
        type_resolver: Optional[Callable] = None,
    ) -> tuple[Callable, Callable]:
        """
        Build (encode_fn, decode_fn) for msg_type with optional field filtering.

            encode_fn(msg: ROS2Message) -> bytes
            decode_fn(raw: bytes)       -> dict   (for apply_dict_to_message)

        type_resolver: callable(type_str) -> msg_class
            Pass rosidl_runtime_py.utilities.get_message so that nested types
            like 'geometry_msgs/msg/Vector3' can be resolved and recursed into.
            Without it, only flat messages with no nested types are supported.

        Field order is determined by the bridge.yaml 'fields' list (or the ROS2
        message definition order when fields is omitted).  Both sides MUST use
        the same config for the codec to be symmetric.
        """
        specs = BridgeCore._collect_specs(msg_type, field_tree, (), type_resolver)

        def encode(msg) -> bytes:
            parts = []
            for spec in specs:
                val = msg
                for attr in spec.path:
                    val = getattr(val, attr)
                parts.append(BridgeCore._pack_field(val, spec))
            return b''.join(parts)

        def decode(raw: bytes) -> dict:
            result: dict = {}
            offset = 0
            for spec in specs:
                val, offset = BridgeCore._unpack_field(raw, offset, spec)
                node = result
                for attr in spec.path[:-1]:
                    node = node.setdefault(attr, {})
                node[spec.path[-1]] = val
            return result

        return encode, decode

    @staticmethod
    def _collect_specs(
        msg_type,
        field_tree: Optional[dict],
        path: tuple,
        type_resolver: Optional[Callable],
    ) -> list:
        """Recursively walk msg_type and build a flat ordered list of _FieldSpec."""
        try:
            fields_and_types: dict = msg_type().get_fields_and_field_types()
        except Exception:
            return []

        if field_tree is None:
            keys = list(fields_and_types.keys())
        else:
            keys = [k for k in field_tree if k in fields_and_types]

        specs = []
        for field_name in keys:
            type_str = fields_and_types[field_name]
            subtree  = None if field_tree is None else field_tree.get(field_name)
            cur_path = path + (field_name,)

            spec = BridgeCore._classify_leaf(type_str, cur_path)
            if spec is not None:
                # Leaf field — subtree is ignored (field_tree selected the whole leaf)
                specs.append(spec)
            else:
                # Nested message type — recurse
                if type_resolver is None:
                    continue  # can't resolve without ROS2
                try:
                    nested_type = type_resolver(type_str)
                    specs.extend(
                        BridgeCore._collect_specs(nested_type, subtree, cur_path, type_resolver)
                    )
                except Exception:
                    pass  # unknown or unresolvable type, skip

        return specs

    @staticmethod
    def _classify_leaf(type_str: str, path: tuple) -> Optional[_FieldSpec]:
        """
        Return a _FieldSpec if type_str is a leaf (primitive, string, array of
        primitives).  Return None if it's a nested message type that needs recursion.
        """
        # Scalar primitive
        if type_str in _PRIM_FMT:
            return _FieldSpec(path=path, kind='prim', fmt=_PRIM_FMT[type_str], count=0)

        # String
        if type_str in ('string', 'wstring'):
            return _FieldSpec(path=path, kind='string', fmt='', count=0)

        # Fixed array: "double[9]", "float32[3]"
        m = _ARRAY_FIXED_RE.match(type_str)
        if m:
            elem, count = m.group(1).strip(), int(m.group(2))
            if elem in _PRIM_FMT:
                return _FieldSpec(path=path, kind='fixed_array', fmt=_PRIM_FMT[elem], count=count)
            return None  # array of nested types — not supported

        # Dynamic array: "uint8[]" or "uint8[<=256]"
        m = _ARRAY_DYN_RE.match(type_str)
        if m:
            elem = m.group(1).strip()
            if elem in _PRIM_FMT:
                return _FieldSpec(path=path, kind='dyn_array', fmt=_PRIM_FMT[elem], count=0)
            return None

        # Not a leaf — caller should recurse
        return None

    @staticmethod
    def _pack_field(val: Any, spec: _FieldSpec) -> bytes:
        if spec.kind == 'prim':
            return struct.pack('<' + spec.fmt, val)
        elif spec.kind == 'string':
            b = (val or '').encode('utf-8')
            return struct.pack('<H', len(b)) + b
        elif spec.kind == 'fixed_array':
            return struct.pack(f'<{spec.count}{spec.fmt}', *val)
        elif spec.kind == 'dyn_array':
            n = len(val)
            return struct.pack('<H', n) + (struct.pack(f'<{n}{spec.fmt}', *val) if n else b'')
        return b''

    @staticmethod
    def _unpack_field(raw: bytes, offset: int, spec: _FieldSpec) -> tuple:
        if spec.kind == 'prim':
            fmt = '<' + spec.fmt
            val = struct.unpack_from(fmt, raw, offset)[0]
            return val, offset + struct.calcsize(fmt)
        elif spec.kind == 'string':
            length = struct.unpack_from('<H', raw, offset)[0];  offset += 2
            val = raw[offset : offset + length].decode('utf-8')
            return val, offset + length
        elif spec.kind == 'fixed_array':
            fmt = f'<{spec.count}{spec.fmt}'
            vals = list(struct.unpack_from(fmt, raw, offset))
            return vals, offset + struct.calcsize(fmt)
        elif spec.kind == 'dyn_array':
            n = struct.unpack_from('<H', raw, offset)[0];  offset += 2
            if n:
                fmt = f'<{n}{spec.fmt}'
                vals = list(struct.unpack_from(fmt, raw, offset))
                return vals, offset + struct.calcsize(fmt)
            return [], offset
        return None, offset

    # ------------------------------------------------------------------
    # Packet envelope  (bridge_id + length prefix only — no field names)
    # ------------------------------------------------------------------

    @staticmethod
    def pack_packet(bridge_id: int, payload: bytes) -> bytes:
        """
        Wrap a binary payload in the bridge envelope.

        Layout (little-endian):
            [1 byte]  bridge_id
            [4 bytes] payload length
            [N bytes] payload  (from encode_fn)
        """
        return struct.pack("<B", bridge_id) + struct.pack("<I", len(payload)) + payload

    @staticmethod
    def unpack_packet(raw: bytes) -> tuple[int, bytes]:
        """
        Strip the bridge envelope.  Returns (bridge_id, payload_bytes).
        Pass payload_bytes to the bridge's decode_fn.
        """
        bridge_id = struct.unpack_from("<B", raw, 0)[0]
        pay_len   = struct.unpack_from("<I", raw, 1)[0]
        return bridge_id, raw[5 : 5 + pay_len]

    # ------------------------------------------------------------------
    # Message reconstruction  (used on the RX side after decode_fn)
    # ------------------------------------------------------------------

    @staticmethod
    def apply_dict_to_message(data: dict, dst_msg: Any) -> None:
        """
        Recursively write a plain dict (from decode_fn) back into a ROS 2
        message object.
        """
        try:
            valid = set(dst_msg.get_fields_and_field_types().keys())
        except AttributeError:
            return
        for k, v in data.items():
            if k not in valid:
                continue
            if isinstance(v, dict):
                nested = getattr(dst_msg, k)
                BridgeCore.apply_dict_to_message(v, nested)
                setattr(dst_msg, k, nested)
            else:
                setattr(dst_msg, k, v)
