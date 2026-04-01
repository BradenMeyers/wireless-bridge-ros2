# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Run

```bash
# Build this package only
colcon build --packages-select base_station_coms

# Build and source
colcon build --packages-select base_station_coms && source install/setup.bash

# Run tests (ament_lint only — no unit tests currently)
colcon test --packages-select base_station_coms

# Launch the radio bridge
ros2 launch base_station_coms bridge_launch.py param_file:=/home/frostlab/config/base_station_params.yaml

# Launch the radio node directly
ros2 run base_station_coms base_station_radio.py
```

External config files are expected at:
- `/home/frostlab/config/base_station_params.yaml` — radio bridge YAML config
- `/home/frostlab/base_station/mission_control/params/fleet_params.yaml` — vehicle fleet config
- `/home/frostlab/base_station/mission_control/deploy_config.json` — WiFi vehicle IP mapping

## Architecture

This ROS 2 package manages communications between a ground base station and underwater AUVs (Autonomous Underwater Vehicles) over three independent channels:

### Communication Channels

1. **Acoustic Modem** (`src/base_station_modem.cpp`, `include/base_station_coms/coms_protocol.hpp`): Long-range underwater comms via Seatrac hardware. Uses a packed binary protocol with message IDs defined in `coms_protocol.hpp`. Provides ROS services: `modem_status_request`, `modem_e_kill`, `modem_e_surface`, `modem_init`.

2. **XBee Radio** (`scripts/base_station_radio.py`, `scripts/radio_manager.py`, `scripts/radio_bridge.py`): Mid-range RF over digi-xbee hardware. The radio stack is three layers:
   - `radio_bridge.py` — YAML-configured, type-agnostic ROS message serializer/deserializer. Operates in `local`, `radio_tx`, or `radio_rx` modes. Field filtering and binary packing is driven by the param YAML.
   - `radio_manager.py` — Transmission scheduler wrapping the XBee library. Per-bridge priority queuing (higher tier = higher priority), reliable vs best-effort delivery, sequence numbers, retries, and ACK tracking.
   - `base_station_radio.py` — Top-level ROS node: connection monitoring via periodic PINGs, chunked file transfer (64-byte chunks, base64 encoded), and JSON-based message protocol.

3. **WiFi** (`scripts/base_station_wifi.py`): For vehicles on the same local network. Uses system `ping` in a thread pool for connection monitoring. Provides services: `wifi_e_kill`, `wifi_init`, `wifi_load_mission`.

### Main Coordinator

`src/base_station_coms.cpp` is the top-level node. It holds service clients to all three channel nodes and fans out commands (emergency kill, status requests, mission loading) across whichever channels are active. It also runs periodic status polling.

`src/modem_pinger.cpp` is a standalone heartbeat node that sends periodic PINGs to vehicles and updates connection status.

`src/teleop_couguv_key.cpp` provides keyboard teleoperation, converting key inputs into fin/rudder/thruster commands sent over all active channels.

### Binary Protocol

`include/base_station_coms/coms_protocol.hpp` defines the packed structs and `COUG_MSG_ID` enum used on the acoustic modem channel. Message IDs: `VEHICLE_STATUS` (0x10), `REQUEST_STATUS` (0x11), `INIT` (0x20), `EMERGENCY_KILL` (0x40), `EMERGENCY_SURFACE` (0x50), and localization variants.

`include/base_station_coms/seatrac_enums.hpp` contains hardware-level enumerations for the Seatrac modem (message type codes, status flags, error codes).

### Custom Interface Dependencies

The package depends on three custom ROS 2 interface packages that must be built first:
- `seatrac_interfaces` — Acoustic modem message types
- `cougars_interfaces` — AUV state and command types
- `base_station_interfaces` — Base station service definitions
