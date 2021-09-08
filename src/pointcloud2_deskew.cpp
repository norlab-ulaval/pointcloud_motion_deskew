#include <ros/ros.h>
#include <sensor_msgs/PointCloud2.h>
#include <sensor_msgs/point_cloud2_iterator.h>
#include <tf/transform_listener.h>
#include <unordered_map>
#include <chrono>
#include <pluginlib/class_list_macros.h>
#include "pointcloud_motion_deskew/pointcloud2_deskew.h"

PLUGINLIB_EXPORT_CLASS(PointCloud2Deskew, nodelet::Nodelet);

ros::Publisher pub;
ros::Subscriber sub;
boost::shared_ptr<tf::TransformListener> tf_ptr;
std::string fixed_frame_for_laser = "odom";  //TODO: Load as a parameter
std::string time_field_name = "t";
int expected_number_of_pcl_columns = 4000;
int round_to_intervals_of_nanoseconds = 50000;


void cloud_callback (const sensor_msgs::PointCloud2ConstPtr& input)
{

    //Measure callback duration
    auto start = std::chrono::steady_clock::now();

    // Create a container for the data.
    sensor_msgs::PointCloud2 output;

    // Copy the data into a new messages
    output = *input;

    // Iterators over the pointcloud2 message
    // TODO: Check that there actually is a "t" in the message!!! (try/catch)
    sensor_msgs::PointCloud2Iterator<uint32_t> iter_t(output, time_field_name);

    // Dictionary to store transforms already looked up
    std::unordered_map<uint32_t, tf::StampedTransform> tfs_cache;
    tfs_cache.reserve(expected_number_of_pcl_columns);

    uint32_t latest_time = 0;
    uint32_t current_point_time = 0;

    // Find the latest time
    for (; iter_t != iter_t.end(); ++iter_t)
    {
        if(*iter_t>latest_time) latest_time=*iter_t;
    }

    //wait for the latest transform
    ros::Time last_laser_beam_time = output.header.stamp + ros::Duration(0, latest_time);
    try{
        tf_ptr->waitForTransform (output.header.frame_id,
                                  output.header.stamp,
                                  output.header.frame_id,
                                  last_laser_beam_time,
                                  fixed_frame_for_laser,
                                  ros::Duration(0.25));

    }
    catch (tf::TransformException &ex){
        ROS_ERROR("Pointcloud callback failed because: %s",ex.what());
        return;
    }

    auto after_waitForTransform = std::chrono::steady_clock::now();

    //reset the iterators
    iter_t = sensor_msgs::PointCloud2Iterator<uint32_t>(output, time_field_name);
    sensor_msgs::PointCloud2Iterator<float> iter_xyz(output, "x");    // xyz are consecutive, y~iter_xzy[1], z~[2]

    // iterate over the pointcloud, lookup tfs and apply them
    for (; iter_t != iter_t.end(); ++iter_t, ++iter_xyz)
    {

        current_point_time = (*iter_t/round_to_intervals_of_nanoseconds)*round_to_intervals_of_nanoseconds;
        //std::cout << "Exact: " << *iter_t << "ns, rounded: " << current_point_time << "ns." << std::endl;

        tf::StampedTransform transform;
        if(tfs_cache.count(current_point_time) == 0) // we haven't looked for the transform for that time yet
        {
            ros::Time laser_beam_time = output.header.stamp + ros::Duration(0, current_point_time);
            try{
                tf_ptr->lookupTransform (output.header.frame_id,
                                         output.header.stamp,
                                         output.header.frame_id,
                                         laser_beam_time,
                                         fixed_frame_for_laser,
                                         transform);
            }
            catch (tf::TransformException ex){
                ROS_ERROR("Pointcloud callback failed because: %s",ex.what());
                return;
            }

            tfs_cache[current_point_time] = transform;
        }
        else // we already have that transform from a previous point
        {
            transform = tfs_cache[current_point_time];
        }

        //transform the point
        tf::Vector3 transformed_point = transform * tf::Vector3(iter_xyz[0], iter_xyz[1], iter_xyz[2]);
        iter_xyz[0] = transformed_point.x();
        iter_xyz[1] = transformed_point.y();
        iter_xyz[2] = transformed_point.z();

    }


    //Publish the data.
    pub.publish(output);


    auto end = std::chrono::steady_clock::now();
    const auto cb_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    if (cb_time_ms > 50)
    {
      const auto tf_time = std::chrono::duration_cast<std::chrono::milliseconds>(after_waitForTransform - start).count();
      ROS_WARN_STREAM_THROTTLE(1.0, "Elapsed callback time in milliseconds : " << cb_time_ms << " ms. ");
      ROS_WARN_STREAM_THROTTLE(1.0, "Elapsed callback time in waiting for transform: " << tf_time << " ms.");
      ROS_WARN_STREAM_THROTTLE(1.0, "Number of tf's fetched: " << tfs_cache.size());
    }
}

void PointCloud2Deskew::onInit()
{
    ros::NodeHandle nh = this->getNodeHandle();

    // Create a transform listener
    tf_ptr.reset(new tf::TransformListener);

    // Create a ROS publisher for the output point cloud
    pub = nh.advertise<sensor_msgs::PointCloud2> ("output_point_cloud", 20);

    // Create a ROS subscriber for the input point cloud
    sub = nh.subscribe ("input_point_cloud", 20, cloud_callback);
}
