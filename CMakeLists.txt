cmake_minimum_required(VERSION 3.8)
project(pointcloud_motion_deskew)

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(tf2 REQUIRED)
find_package(tf2_ros REQUIRED)
find_package(geometry_msgs REQUIRED)
find_package(tf2_geometry_msgs REQUIRED)

add_executable(pointcloud2_deskew_node src/pointcloud2_deskew.cpp)
ament_target_dependencies(pointcloud2_deskew_node
  rclcpp
  sensor_msgs
  tf2_ros
  tf2
  tf2_geometry_msgs
  geometry_msgs
)

install(TARGETS pointcloud2_deskew_node
    DESTINATION lib/${PROJECT_NAME}
)

ament_package()
