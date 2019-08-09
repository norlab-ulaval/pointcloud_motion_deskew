#include <ros/ros.h>
#include <sensor_msgs/PointCloud2.h>
#include <sensor_msgs/point_cloud2_iterator.h>
//#include <pcl_conversions/pcl_conversions.h>
//#include <pcl/point_cloud.h>
//#include <pcl/point_types.h>
#include <tf/transform_listener.h>
#include <map>
#include <chrono>



ros::Publisher pub;
boost::shared_ptr<tf::TransformListener> tf_ptr;
std::string fixed_frame_for_laser = "odom";  //TODO: Load as a parameter


void cloud_callback (const sensor_msgs::PointCloud2ConstPtr& input)
{

    //Measure callback duration
    std::cout << "Beginning processing pcl...  ";
    auto start = std::chrono::steady_clock::now();

    // Create a container for the data.
    sensor_msgs::PointCloud2 output;

    // Copy the data into a new messages
    output = *input;

    // Iterators over the pointcloud2 message
    // TODO: Check that there actually is a "t" in the message!!! (try/catch)
    sensor_msgs::PointCloud2Iterator<uint32_t> iter_t(output, "t");
    sensor_msgs::PointCloud2Iterator<float> iter_xyz(output, "x");    // xyz are consecutive, y~iter_xzy[1], z~[2]

    // Dictionary to store transforms already looked up
    std::map<uint32_t, tf::StampedTransform> tfs_cache;


    // iterate over the pointcloud
    for (; iter_t != iter_t.end(); ++iter_t, ++iter_xyz)
    {
        tf::StampedTransform transform;

        if(tfs_cache.count(*iter_t) == 0) // we haven't looked for the transform for that time yet
        {
            ros::Time laser_beam_time = output.header.stamp + ros::Duration(0, *iter_t);
            try{
                tf_ptr->waitForTransform (output.header.frame_id,
                                          output.header.stamp,
                                          output.header.frame_id,
                                          laser_beam_time,
                                          fixed_frame_for_laser,
                                          ros::Duration(0.25));
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

            tfs_cache[*iter_t] = transform;
        }
        else // we already have that transform from a previous point
        {
            transform = tfs_cache[*iter_t];
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
    std::cout << "... done." << std::endl;
    std::cout << "Elapsed callback time in milliseconds : "
         << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
         << " ms" << std::endl;
    std::cout << "Number of tf's fetched: " << tfs_cache.size() << std::endl;

}

int
main (int argc, char** argv)
{
    // Initialize ROS
    ros::init (argc, argv, "pointcloud2_deskew");
    ros::NodeHandle nh;

    // Create a ROS subscriber for the input point cloud
    ros::Subscriber sub = nh.subscribe ("/laser_throttler/points", 20, cloud_callback);  //TODO: Load as a param or change to "input" for later topic renaming

    // Create a transform listener
    tf_ptr.reset(new tf::TransformListener);

    // Create a ROS publisher for the output point cloud
    pub = nh.advertise<sensor_msgs::PointCloud2> ("output", 20);

    // Spin
    ros::spin ();
}