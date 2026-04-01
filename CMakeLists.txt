cmake_minimum_required(VERSION 3.8)
project(wireless_bridge)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(std_msgs REQUIRED)
find_package(std_srvs REQUIRED)
find_package(nav_msgs REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(ament_cmake_python REQUIRED)



# include_directories(include)

# install(DIRECTORY include/
# DESTINATION include/${PROJECT_NAME})

install(PROGRAMS
  scripts/radio_bridge.py
  scripts/radio_manager.py
  scripts/bridge_core.py
  scripts/comms_device.py
  scripts/ros_topic_device.py
  scripts/dummy_publisher.py
  DESTINATION lib/${PROJECT_NAME}
)

#Install Launch file
install(DIRECTORY launch DESTINATION share/${PROJECT_NAME})

#Install Config dir
install(DIRECTORY config DESTINATION share/${PROJECT_NAME})

find_package(rosidl_default_generators REQUIRED)


ament_export_dependencies(rosidl_default_runtime)


install(TARGETS
DESTINATION lib/${PROJECT_NAME})

if(BUILD_TESTING)
find_package(ament_lint_auto REQUIRED)
set(ament_cmake_copyright_FOUND TRUE)
set(ament_cmake_cpplint_FOUND TRUE)
ament_lint_auto_find_test_dependencies()
endif()

ament_package()