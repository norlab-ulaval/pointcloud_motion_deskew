cmake_minimum_required(VERSION 3.8)
project(pointcloud_motion_deskew)

set(CMAKE_BUILD_TYPE "Release")

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(tf2 REQUIRED)
find_package(tf2_ros REQUIRED)
find_package(geometry_msgs REQUIRED)
find_package(tf2_geometry_msgs REQUIRED)
find_package(rclcpp_components REQUIRED)

add_executable(pointcloud2_deskew_node src/pointcloud2_deskew.cpp)
ament_target_dependencies(pointcloud2_deskew_node
  rclcpp
  sensor_msgs
  tf2_ros
  tf2
  tf2_geometry_msgs
  geometry_msgs
)

add_library(pointcloud2_deskew_component SHARED src/pointcloud2_deskew_component.cpp)
ament_target_dependencies(pointcloud2_deskew_component
  rclcpp
  sensor_msgs
  tf2_ros
  tf2
  tf2_geometry_msgs
  geometry_msgs
  rclcpp_components
)
rclcpp_components_register_node(
  pointcloud2_deskew_component
  PLUGIN "PointcloudDeskewNode"
  EXECUTABLE pointcloud2_deskew_component_exec
)

install(TARGETS pointcloud2_deskew_node
    DESTINATION lib/${PROJECT_NAME}
)

ament_export_targets(export_pointcloud2_deskew_component)
install(TARGETS pointcloud2_deskew_component
        EXPORT export_pointcloud2_deskew_component
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin
)

install(DIRECTORY launch
    DESTINATION share/${PROJECT_NAME})

ament_package()
