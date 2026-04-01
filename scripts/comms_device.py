#!/usr/bin/env python3

"""
comms_device.py

Abstract base class for communication device wrappers.

Any transport (XBee radio, serial, UDP, WiFi, etc.) can be dropped into
TxManager by subclassing CommsDevice and implementing three abstract methods:
    open()      — open the hardware connection
    close()     — close the hardware connection
    _send_raw() — write framed bytes to the hardware

Full wire format (outermost → innermost):

    DATA packet:
        [1 byte]  packet_type = 0x01 # TODO make this just a bit?
        [1 byte]  src_device_id
        [2 bytes] seq
        [1 byte]  bridge_id
        [4 bytes] payload length
        [N bytes] payload

    ACK packet:
        [1 byte]  packet_type = 0x00
        [1 byte]  src_device_id
        [2 bytes] seq being ACKed
        [1 byte]  bridge_id being ACKed

The packet_type byte is the outermost field, read before checksum or
destination filtering, so the receive pipeline can route ACKs directly to
the queue manager without going through the data path.

Receive pipeline for DATA packets:

    _verify_checksum(inner)         → discard on mismatch
    _strip_framing(inner)           → extract (dest_addr, src_id, packet)
    _check_destination(dest_addr)   → discard if not addressed to this device
    strip_seq(packet)               → extract seq
    extract bridge_id from payload[0]
    _rx_callback(bridge_id, seq, payload, src_id)

Receive pipeline for ACK packets:

    parse (src_id, seq, bridge_id) from inner
    _ack_callback(bridge_id, seq, src_id)

Default implementations of all filtering hooks pass through everything,
which is correct for devices (like XBee) where the hardware already handles
checksums and destination routing.

TX pipelines:

    DATA — send(address, packet):
        _add_framing(address, packet) → [0x01][src_id] + packet
        _send_raw(address, framed)

    ACK — _send_ack(dest_hw_addr, bridge_id, seq):
        builds [0x00][src_id][seq(2)][bridge_id(1)]
        _send_raw(dest_hw_addr, ack_bytes)
"""

import logging
import struct
from abc import ABC, abstractmethod
from typing import Callable, Optional

from bridge_core import strip_seq

PACKET_DATA: int = 0x01
PACKET_ACK:  int = 0x00


class CommsDevice(ABC):
    """
    Abstract base class for all communication device wrappers.

    Subclass and implement open(), close(), and _send_raw().
    Override the filtering hooks only when the hardware does not already
    provide checksum verification and destination-address routing.
    """

    def __init__(
        self,
        logger=None,
        my_address: Optional[str] = None,
        device_id: int = 0,
    ):
        """
        Parameters
        ----------
        logger      : ROS 2 logger or standard logging.Logger.
        my_address  : This device's address on the medium.  Used by
                      _check_destination() in subclasses that share a broadcast
                      medium and must filter packets in software.
        device_id   : 1-byte unsigned int and not 0 (1-255) identifying this device.
                      Prepended to every outbound packet as the source address
                      so the receiver can identify where the data came from.
        """
        if not (0 < device_id <= 255):
            raise ValueError(f"device_id {device_id} out of range (must be 1-255).")

        self._log          = logger or logging.getLogger(__name__)
        self._rx_callback:  Optional[Callable[[int, int, bytes, int], None]] = None
        self._ack_callback: Optional[Callable[[int, int, int], None]] = None
        self.my_address    = my_address
        self.device_id     = device_id

    # ------------------------------------------------------------------
    # Abstract — must implement
    # ------------------------------------------------------------------

    @abstractmethod
    def open(self) -> bool:
        """Open the hardware connection.  Return True on success."""

    @abstractmethod
    def close(self) -> None:
        """Close the hardware connection."""

    @abstractmethod
    def _send_raw(self, address: Optional[str], data: bytes) -> bool:
        """
        Write data to the hardware, addressed to address (or broadcast if None).
        Return True on success.
        """

    # ------------------------------------------------------------------
    # Filtering hooks — override when hardware doesn't handle these
    # ------------------------------------------------------------------

    def _add_framing(self, address: Optional[str], packet: bytes) -> bytes:
        """
        Prepend [PACKET_DATA][src_device_id] before transmission.

        Override to add additional framing (e.g. destination address header,
        CRC) for devices that share a broadcast medium.  The packet_type byte
        must remain the outermost byte.
        """
        return struct.pack("<BB", PACKET_DATA, self.device_id) + packet

    def _strip_framing(self, inner: bytes) -> tuple[Optional[str], int, bytes]:
        """
        Strip the 1-byte source device_id from the inner packet (packet_type
        has already been removed by _process_received before this is called).
        Returns (dest_addr, src_id, packet).

        dest_addr is forwarded to _check_destination().  Return None when the
        hardware already filtered by destination and no check is needed.

        Override to parse additional framing (destination header, CRC).
        """
        src_id = struct.unpack_from("<B", inner, 0)[0]
        return None, src_id, inner[1:]

    def _verify_checksum(self, inner: bytes) -> bool:
        """
        Return False to discard a packet whose checksum does not match.
        Receives the packet after packet_type has been stripped.

        Default: True (hardware-verified, no software check needed).
        """
        return True

    def _check_destination(self, dest_addr: Optional[str]) -> bool:
        """
        Return False to discard a packet not addressed to this device.

        Compare dest_addr (extracted by _strip_framing) against self.my_address.

        Default: True (hardware already routed correctly, no filter needed).
        """
        return True

    # ------------------------------------------------------------------
    # Concrete pipeline — not intended to be overridden
    # ------------------------------------------------------------------

    def send(self, address: Optional[str], packet: bytes) -> bool:
        """
        Full DATA TX pipeline:
            1. _add_framing(address, packet)  → prepends [PACKET_DATA][src_id]
            2. _send_raw(address, framed)     → bool
        """
        framed = self._add_framing(address, packet)
        return self._send_raw(address, framed)

    def set_receive_callback(self, fn: Callable[[int, int, bytes, int, Optional[str]], None]) -> None:
        """
        Register the callback invoked for every valid inbound DATA packet.

            fn(bridge_id: int, seq: int, payload: bytes, src_id: int, src_hw_addr: Optional[str])

        src_id is the device_id of the sender, extracted from the framing.
        src_hw_addr is the hardware address of the sender (e.g. XBee 64-bit address),
        forwarded so the callback can decide whether to send an ACK.
        """
        self._rx_callback = fn

    def set_ack_callback(self, fn: Callable[[int, int, int], None]) -> None:
        """
        Register the callback invoked when an ACK packet is received.

            fn(bridge_id: int, seq: int, src_id: int)

        Wire this to TxManager.ack_received so the queue clears the pending
        packet when its ACK arrives.
        """
        self._ack_callback = fn

    def _send_ack(self, dest_hw_addr: Optional[str], bridge_id: int, seq: int) -> None:
        """
        Send an ACK packet back to the sender.

        Format: [PACKET_ACK=0x00][src_device_id][seq(2)][bridge_id(1)]

        dest_hw_addr is the hardware-level address of the device to ACK
        (e.g. XBee 64-bit address string).  Pass None to broadcast, though
        targeted ACKs are strongly preferred for reliable bridges.

        Override in subclasses that need additional framing (e.g. CRC) on
        outbound ACK packets.
        """
        ack_bytes = struct.pack("<BBHB", PACKET_ACK, self.device_id, seq, bridge_id)
        self._send_raw(dest_hw_addr, ack_bytes)

    def _process_received(self, raw: bytes, src_hw_addr: Optional[str] = None) -> None:
        """
        Entry point for bytes arriving from the hardware.

        Reads the packet_type byte first, then routes to the DATA pipeline or
        the ACK handler.  Call this from your hardware receive callback:

            def _hw_callback(self, message):
                self._process_received(message.data, sender_address)

        src_hw_addr: hardware-level address of the sender (e.g. XBee 64-bit
        address string).  Used to send targeted ACKs back for reliable bridges.
        """
        if len(raw) < 2:
            self._log.warning("Packet too short, discarding.")
            return

        packet_type = struct.unpack_from("<B", raw, 0)[0]
        inner       = raw[1:]

        if packet_type == PACKET_ACK:
            self._handle_ack(inner)
            return

        if packet_type != PACKET_DATA:
            self._log.warning(f"Unknown packet type {packet_type:#04x}, discarding.")
            return

        # --- DATA pipeline ---
        if not self._verify_checksum(inner):
            self._log.debug("Packet discarded: checksum mismatch")
            return

        dest_addr, src_id, packet = self._strip_framing(inner)

        if not self._check_destination(dest_addr):
            self._log.debug(f"Packet discarded: destination {dest_addr!r} not for us")
            return

        try:
            seq, after_seq = strip_seq(packet)
            bridge_id = struct.unpack_from("<B", after_seq, 0)[0]
            if self._rx_callback:
                self._rx_callback(bridge_id, seq, after_seq, src_id, src_hw_addr)
        except Exception as exc:
            self._log.error(f"RX DATA parse error: {exc}")

    def _handle_ack(self, inner: bytes) -> None:
        """
        Parse an inbound ACK packet and invoke the ack callback.

        ACK inner format (after packet_type is stripped):
            [1 byte]  src_device_id
            [2 bytes] seq being ACKed
            [1 byte]  bridge_id being ACKed
        """
        if len(inner) < 4:
            self._log.warning("ACK packet too short, discarding.")
            return
        try:
            src_id, seq, bridge_id = struct.unpack_from("<BHB", inner, 0)
            if self._ack_callback:
                self._ack_callback(bridge_id, seq, src_id)
        except Exception as exc:
            self._log.error(f"RX ACK parse error: {exc}")
