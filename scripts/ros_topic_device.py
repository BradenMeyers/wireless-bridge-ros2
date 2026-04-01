#!/usr/bin/env python3

"""
ros_topic_device.py

CommsDevice implementation that uses ROS 2 topics instead of hardware.
Raw packet bytes are published on a TX topic and received from an RX topic
via std_msgs/UInt8MultiArray.

Usage — single node loopback (TX → RX same topic):
    sim_tx_topic: /radio_sim
    sim_rx_topic: /radio_sim

Usage — two nodes cross-wired (node A ↔ node B):
    Node A: sim_tx_topic=/a_to_b  sim_rx_topic=/b_to_a
    Node B: sim_tx_topic=/b_to_a  sim_rx_topic=/a_to_b
"""

from typing import Optional

from std_msgs.msg import UInt8MultiArray

from comms_device import CommsDevice


class RosTopicDevice(CommsDevice):
    """
    CommsDevice backed by ROS 2 topics instead of radio hardware.

    _send_raw() publishes bytes on the TX topic.
    Subscriber on the RX topic feeds bytes back into _process_received().

    Hardware addressing is not meaningful here — the address argument to
    _send_raw() is ignored (broadcast semantics).
    """

    def __init__(
        self,
        node,
        tx_topic: str,
        rx_topic: str,
        device_id: int = 1,
        logger=None,
    ):
        """
        Parameters
        ----------
        node      : rclpy Node — used to create publisher and subscriber.
        tx_topic  : Topic name for outbound raw bytes.
        rx_topic  : Topic name for inbound raw bytes.
        device_id : 1-byte source address prepended to every outbound packet.
        logger    : Logger; defaults to node.get_logger().
        """
        super().__init__(logger=logger or node.get_logger(), device_id=device_id)
        self._node      = node
        self._tx_topic  = tx_topic
        self._rx_topic  = rx_topic
        self._pub       = None
        self._sub       = None

    # ------------------------------------------------------------------
    # CommsDevice abstract implementation
    # ------------------------------------------------------------------

    def open(self) -> bool:
        self._pub = self._node.create_publisher(UInt8MultiArray, self._tx_topic, 10)
        self._sub = self._node.create_subscription(
            UInt8MultiArray, self._rx_topic, self._on_ros_msg, 10
        )
        self._log.info(
            f"RosTopicDevice: TX={self._tx_topic}  RX={self._rx_topic}"
        )
        return True

    def close(self) -> None:
        pass  # ROS node handles publisher/subscriber lifecycle

    def _send_raw(self, address: Optional[str], data: bytes) -> bool:
        if self._pub is None:
            self._log.warning("RosTopicDevice: send called before open().")
            return False
        msg = UInt8MultiArray()
        msg.data = list(data)
        self._pub.publish(msg)
        return True

    # ------------------------------------------------------------------
    # ROS subscriber callback
    # ------------------------------------------------------------------

    def _on_ros_msg(self, msg: UInt8MultiArray) -> None:
        """Feed inbound topic bytes into the base-class receive pipeline."""
        self._process_received(bytes(msg.data))
