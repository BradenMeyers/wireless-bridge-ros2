#!/usr/bin/env python3

"""
radio_manager.py

XBee hardware wrapper.  Owns the serial port and nothing else.
All queue management and scheduling logic lives in bridge_core.TxManager.
All device abstraction lives in comms_device.CommsDevice.
"""

import logging
import struct
from typing import Optional

from comms_device import CommsDevice

# digi.xbee imports — guarded so the module can be imported in unit tests
# without hardware present.
try:
    from digi.xbee.devices import XBeeDevice, RemoteXBeeDevice, XBee64BitAddress
    from digi.xbee.exception import TransmitException
    _XBEE_AVAILABLE = True
except ImportError:
    _XBEE_AVAILABLE = False


class XBeeRadioDevice(CommsDevice):
    """
    CommsDevice implementation for digi XBee radios.

    The XBee hardware handles checksums (hardware CRC) and destination routing
    (unicast/broadcast at the RF layer), so none of the filtering hooks are
    overridden — all inbound packets that reach the callback are already valid
    and addressed to this device.
    """

    def __init__(self, port: str, baud: int, logger=None, device_id: int = 0):
        super().__init__(logger=logger, device_id=device_id)
        self._port   = port
        self._baud   = baud
        self._device = None

        if not _XBEE_AVAILABLE:
            self._log.warning(
                "digi.xbee not installed — XBeeRadioDevice running in stub mode."
            )

    # ------------------------------------------------------------------
    # CommsDevice abstract implementation
    # ------------------------------------------------------------------

    def open(self) -> bool:
        if not _XBEE_AVAILABLE:
            self._log.warning("XBee stub: open() called, no hardware.")
            return False
        try:
            self._device = XBeeDevice(self._port, self._baud)
            self._device.open()
            self._device.add_data_received_callback(self._hw_rx_callback)
            self._log.info(f"XBee opened on {self._port} @ {self._baud} baud.")
            return True
        except Exception as exc:
            self._log.error(f"XBee open failed: {exc}")
            return False

    def close(self) -> None:
        if self._device and self._device.is_open():
            self._device.close()
            self._log.info("XBee device closed.")

    def _send_raw(self, address: Optional[str], data: bytes) -> bool:
        if not _XBEE_AVAILABLE or self._device is None:
            # Stub: pretend it worked
            self._log.debug(
                f"XBee stub send {len(data)} bytes to {address or 'broadcast'}"
            )
            return True

        try:
            if address:
                remote = RemoteXBeeDevice(
                    self._device, XBee64BitAddress.from_hex_string(address)
                )
                self._device.send_data(remote, data)
            else:
                self._device.send_data_broadcast(data)
            return True
        except TransmitException as exc:
            if address:
                # Unicast failure is a real error — the remote didn't ACK at the RF layer.
                self._log.error(f"XBee TransmitException (unicast to {address}): {exc}")
                return False
            else:
                # Broadcast doesn't require receivers; a non-OK transmit status is
                # normal when no one is in range. Treat as fire-and-forget success.
                self._log.debug(f"XBee broadcast TransmitException (no receivers in range): {exc}")
                return True
        except Exception as exc:
            self._log.error(f"XBee send error: {exc}")
            return False

    # ------------------------------------------------------------------
    # Hardware receive callback
    # ------------------------------------------------------------------

    def _hw_rx_callback(self, xbee_message) -> None:
        """digi.xbee callback — forward raw bytes into the base-class pipeline."""
        try:
            src_hw_addr = str(xbee_message.remote_device.get_64bit_addr())
        except Exception:
            src_hw_addr = None
        self._process_received(xbee_message.data, src_hw_addr)
