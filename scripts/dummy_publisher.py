#!/usr/bin/env python3

import rclpy
from rclpy.node import Node
from geometry_msgs.msg import Vector3Stamped

class DummyPublisher(Node):
    def __init__(self):
        super().__init__('dummy_publisher')
        self.publisher_ = self.create_publisher(Vector3Stamped, '/vector', 10)
        self.timer = self.create_timer(0.1, self.timer_callback)

    def timer_callback(self):
        msg = Vector3Stamped()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.header.frame_id = 'base_link'
        msg.vector.x = 1.0
        msg.vector.y = 2.0
        msg.vector.z = 3.0
        self.publisher_.publish(msg)


def main(args=None):
    rclpy.init(args=args)
    node = DummyPublisher()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()