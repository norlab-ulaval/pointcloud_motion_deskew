#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <sensor_msgs/point_cloud2_iterator.hpp>
#include <tf2_ros/transform_listener.h>
#include <tf2/utils.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.hpp>
#include <tf2_ros/buffer.h>
#include <geometry_msgs/msg/pose.hpp>
#include <geometry_msgs/msg/point.hpp>
#include <unordered_map>
#include <chrono>

class PointcloudDeskewNode: public rclcpp::Node
{
public:
    PointcloudDeskewNode() : Node("pointcloud2_deskew")
    {
        this->declare_parameter<std::string>("fixed_frame_for_lidar", "odom");
        this->declare_parameter<int>("expected_pointcloud_columns", 4000);
        this->declare_parameter<int>("round_to_n_nanoseconds", 50000);

        this->get_parameter("fixed_frame_for_lidar", fixed_frame_for_laser);
        this->get_parameter("expected_pointcloud_columns", expected_number_of_pcl_columns);
        this->get_parameter("round_to_n_nanoseconds", round_to_intervals_of_nanoseconds);

        pub = this->create_publisher<sensor_msgs::msg::PointCloud2>("output_point_cloud", 20);
        sub = this->create_subscription<sensor_msgs::msg::PointCloud2>("input_point_cloud", 20, std::bind(&PointcloudDeskewNode::cloud_callback, this, std::placeholders::_1));
        tfBuffer = std::unique_ptr<tf2_ros::Buffer>(new tf2_ros::Buffer(this->get_clock()));
        tfListener = std::unique_ptr<tf2_ros::TransformListener>(new tf2_ros::TransformListener(*tfBuffer));
    }
private:
    
    std::unique_ptr<tf2_ros::TransformListener> tfListener;
    std::unique_ptr<tf2_ros::Buffer> tfBuffer;
    std::string fixed_frame_for_laser = "odom";
    int expected_number_of_pcl_columns = 4000;
    int round_to_intervals_of_nanoseconds = 50000;

    rclcpp::Subscription<sensor_msgs::msg::PointCloud2>::SharedPtr sub;
    rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr pub;
    
    void cloud_callback (const sensor_msgs::msg::PointCloud2 &input)
    {
        // Create a container for the data.
        sensor_msgs::msg::PointCloud2 output;

        //Copy the data into a new message
        output = input;

        bool is_ouster_like = false;
        bool is_velodyne_like = false;
        bool is_hesai_like = false;

        //Detect what kind of time information we have for the points
        for (sensor_msgs::msg::PointField field : output.fields)
        {
            if(field.name == "t" && field.datatype == 6)
            {
                is_ouster_like = true;
            }
            else if(field.name == "time" && field.datatype == 7)
            {
                is_velodyne_like = true;
            }
            else if(field.name == "timestamp" && field.datatype == 8)
            {
                is_hesai_like = true;
            }
        }

        if(!(is_ouster_like || is_velodyne_like || is_hesai_like))
        {
            RCLCPP_WARN(this->get_logger(), "The input pointcloud does not contain 'time' or 't' or 'timestamp' field. Skipping.");
            return;
        }

        bool success = true;
        if(is_ouster_like) ouster_cloud_deskew(output, success);
        if(is_velodyne_like) velodyne_cloud_deskew(output, success);
        if(is_hesai_like) hesai_cloud_deskew(output, success);

        if(success)
        {
            pub->publish(output);
        }
        else{
            RCLCPP_WARN(this->get_logger(), "Deskewing failed, skipping.");
            return;
        }
    }

    // deskewing ouster-like sensors
    void ouster_cloud_deskew(sensor_msgs::msg::PointCloud2 &output, bool &success)
    {
        std::string time_field_name = "t";
        // Iterators over the pointcloud2 message
        sensor_msgs::PointCloud2Iterator<uint32_t> iter_t(output, time_field_name);

        // Dictionary to store transforms already looked up
        std::unordered_map<int32_t, geometry_msgs::msg::TransformStamped> tfs_cache;
        tfs_cache.reserve(expected_number_of_pcl_columns);

        uint32_t latest_time = 0;
        uint32_t current_point_time = 0;

        rclcpp::Time cloud_start_time(output.header.stamp);
        // Find the latest time
        for (;iter_t != iter_t.end(); ++iter_t)
        {
            if(*iter_t>latest_time) latest_time=*iter_t;
        }
        output.header.stamp = cloud_start_time + rclcpp::Duration(0, (latest_time/round_to_intervals_of_nanoseconds)*round_to_intervals_of_nanoseconds);

        //reset the iterators
        iter_t = sensor_msgs::PointCloud2Iterator<uint32_t>(output, time_field_name);
        sensor_msgs::PointCloud2Iterator<float> iter_xyz(output, "x");   // xyz are consecutive, y~iter_xzy[1], z~[2]

        //iterate over the pointcloud, lookup tfs and apply them
        for (;iter_t != iter_t.end(); ++iter_t, ++iter_xyz)
        {
            // RCLCPP_INFO(this->get_logger(), "timestamp iter_t: %d", *iter_t);
            current_point_time = (*iter_t/round_to_intervals_of_nanoseconds)*round_to_intervals_of_nanoseconds;
            // RCLCPP_INFO(this->get_logger(), "timestamp current_point: %d", current_point_time);
            geometry_msgs::msg::TransformStamped transform;
            tf2::Stamped<tf2::Transform> stampedTransform;

            if(tfs_cache.count(current_point_time) == 0)
            {
                rclcpp::Time laser_beam_time = cloud_start_time + rclcpp::Duration(0, current_point_time);
                
                // RCLCPP_INFO(this->get_logger(), "timestamp laser: %d", *iter_t);
                try{
                    transform = tfBuffer->lookupTransform(output.header.frame_id,
                                                          output.header.stamp,
                                                          output.header.frame_id,
                                                          laser_beam_time,
                                                          fixed_frame_for_laser,
                                                          rclcpp::Duration(0, 2.5e8));
                }
                catch(tf2::TransformException &ex){
                    RCLCPP_ERROR(this->get_logger(), "Pointcloud callback failed because: %s", ex.what());
                    success = false;
                    return;
                }
                tfs_cache[current_point_time] = transform;
            }
            else
            {
                transform = tfs_cache[current_point_time];
            }

            // transform the point
            tf2::convert(transform, stampedTransform);
            tf2::Vector3 transformed_point = stampedTransform * tf2::Vector3(iter_xyz[0], iter_xyz[1], iter_xyz[2]);
            iter_xyz[0] = transformed_point.x();
            iter_xyz[1] = transformed_point.y();
            iter_xyz[2] = transformed_point.z();
        }
    }

    // deskewing velodyne-like sensors
    void velodyne_cloud_deskew(sensor_msgs::msg::PointCloud2 &output, bool &success)
    {
        std::string time_field_name = "time";
        auto start = std::chrono::steady_clock::now();
        
        // Iterators over the pointcloud2 message
        sensor_msgs::PointCloud2Iterator<float> iter_t(output, time_field_name);

        // Dictionary to store transforms already looked up
        std::unordered_map<int32_t, geometry_msgs::msg::TransformStamped> tfs_cache;
        tfs_cache.reserve(expected_number_of_pcl_columns);

        uint32_t latest_time = 0;
        int32_t current_point_time = 0;

        rclcpp::Time cloud_start_time(output.header.stamp);
        // Find the latest time
        for (;iter_t != iter_t.end(); ++iter_t)
        {
            if(*iter_t>latest_time) latest_time=*iter_t;
        }
        output.header.stamp = cloud_start_time + rclcpp::Duration(0, ((int32_t)(latest_time * 1000000000)/round_to_intervals_of_nanoseconds)*round_to_intervals_of_nanoseconds);

        //reset the iterators
        iter_t = sensor_msgs::PointCloud2Iterator<float>(output, time_field_name);
        sensor_msgs::PointCloud2Iterator<float> iter_xyz(output, "x");   // xyz are consecutive, y~iter_xzy[1], z~[2]

        //iterate over the pointcloud, lookup tfs and apply them
        for (;iter_t != iter_t.end(); ++iter_t, ++iter_xyz)
        {
            current_point_time = (int32_t)(*iter_t * 1000000000); //convert to nanoseconds integer
            current_point_time = (current_point_time/round_to_intervals_of_nanoseconds)*round_to_intervals_of_nanoseconds;

            geometry_msgs::msg::TransformStamped transform;
            tf2::Stamped<tf2::Transform> stampedTransform;
            if(tfs_cache.count(current_point_time) == 0)
            {
                rclcpp::Time laser_beam_time = cloud_start_time;
                if (current_point_time < 0) {
                    laser_beam_time -= rclcpp::Duration(0, std::abs(current_point_time));
                } else {
                    laser_beam_time += rclcpp::Duration(0, current_point_time);
                }
                try{
                    transform = tfBuffer->lookupTransform(output.header.frame_id,
                                                          output.header.stamp,
                                                          output.header.frame_id,
                                                          laser_beam_time,
                                                          fixed_frame_for_laser,
                                                          rclcpp::Duration(0, 2.5e8));
                }
                catch(tf2::TransformException &ex){
                    RCLCPP_ERROR(this->get_logger(), "Pointcloud callback failed because: %s", ex.what());
                    success = false;
                    return;
                }
                tfs_cache[current_point_time] = transform;
            }
            else
            {
                transform = tfs_cache[current_point_time];
            }

            // transform the point
            tf2::convert(transform, stampedTransform);
            tf2::Vector3 transformed_point = stampedTransform * tf2::Vector3(iter_xyz[0], iter_xyz[1], iter_xyz[2]);
            iter_xyz[0] = transformed_point.x();
            iter_xyz[1] = transformed_point.y();
            iter_xyz[2] = transformed_point.z();
        }
    }

    // deskewing hesai-like sensors
    void hesai_cloud_deskew(sensor_msgs::msg::PointCloud2 &output, bool &success)
    {
        std::string time_field_name = "timestamp";
        auto start = std::chrono::steady_clock::now();
        
        // Iterators over the pointcloud2 message
        sensor_msgs::PointCloud2Iterator<double> iter_t(output, time_field_name);

        // Dictionary to store transforms already looked up
        std::unordered_map<int64_t, geometry_msgs::msg::TransformStamped> tfs_cache;
        tfs_cache.reserve(expected_number_of_pcl_columns);

        double latest_time = 0;
        int64_t current_point_time = 0;

        rclcpp::Time cloud_start_time(output.header.stamp);
        // Find the latest time
        for (;iter_t != iter_t.end(); ++iter_t)
        {
            if(*iter_t>latest_time) latest_time=*iter_t;
        }
        output.header.stamp = cloud_start_time + rclcpp::Duration(0, (latest_time/round_to_intervals_of_nanoseconds)*round_to_intervals_of_nanoseconds);

        //reset the iterators
        iter_t = sensor_msgs::PointCloud2Iterator<double>(output, time_field_name);
        sensor_msgs::PointCloud2Iterator<float> iter_xyz(output, "x");   // xyz are consecutive, y~iter_xzy[1], z~[2]

        //iterate over the pointcloud, lookup tfs and apply them
        for (;iter_t != iter_t.end(); ++iter_t, ++iter_xyz)
        {
            current_point_time = (*iter_t/round_to_intervals_of_nanoseconds)*round_to_intervals_of_nanoseconds;
            
            geometry_msgs::msg::TransformStamped transform;
            tf2::Stamped<tf2::Transform> stampedTransform;
            if(tfs_cache.count(current_point_time) == 0)
            {
                rclcpp::Time laser_beam_time = cloud_start_time + rclcpp::Duration(0, current_point_time);
                try{
                    transform = tfBuffer->lookupTransform(output.header.frame_id,
                                                          output.header.stamp,
                                                          output.header.frame_id,
                                                          laser_beam_time,
                                                          fixed_frame_for_laser,
                                                          rclcpp::Duration(0, 2.5e8));
                }
                catch(tf2::TransformException &ex){
                    RCLCPP_ERROR(this->get_logger(), "Pointcloud callback failed because: %s", ex.what());
                    success = false;
                    return;
                }
                tfs_cache[current_point_time] = transform;
            }
            else
            {
                transform = tfs_cache[current_point_time];
            }

            // transform the point
            tf2::convert(transform, stampedTransform);
            tf2::Vector3 transformed_point = stampedTransform * tf2::Vector3(iter_xyz[0], iter_xyz[1], iter_xyz[2]);
            iter_xyz[0] = transformed_point.x();
            iter_xyz[1] = transformed_point.y();
            iter_xyz[2] = transformed_point.z();
        }
    }
};

int main (int argc, char** argv)
{
    rclcpp::init(argc, argv);
    rclcpp::spin(std::make_shared<PointcloudDeskewNode>());
    rclcpp::shutdown();
    return 0;
}
