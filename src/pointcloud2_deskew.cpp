#include <ros/ros.h>
#include <sensor_msgs/PointCloud2.h>
#include <sensor_msgs/point_cloud2_iterator.h>
#include <tf/transform_listener.h>
#include <unordered_map>
#include <chrono>

ros::Publisher pub;
boost::shared_ptr<tf::TransformListener> tf_ptr;
std::string fixed_frame_for_laser = "odom";
int expected_number_of_pcl_columns = 4000;
int round_to_intervals_of_nanoseconds = 50000;

void ouster_cloud_deskew(sensor_msgs::PointCloud2 &output, bool &success);
void velodyne_cloud_deskew(sensor_msgs::PointCloud2 &output, bool &success);
void hesai_cloud_deskew(sensor_msgs::PointCloud2 &output, bool &success);

void cloud_callback (const sensor_msgs::PointCloud2ConstPtr& input)
{

    //Measure callback duration
    std::cout << "Beginning processing pcl...  ";
    auto start = std::chrono::steady_clock::now();

    // Create a container for the data.
    sensor_msgs::PointCloud2 output;

    // Copy the data into a new messages
    output = *input;

    bool is_ouster_like = false;     // time field called "t", time positive nanoseconds in a uint32
    bool is_velodyne_like = false;     // time field called "time", time negative seconds in a float
    bool is_hesai_like = false;     // time field called "time", time negative seconds in a float

    // Detect what kind of time information we have for the points
    for (sensor_msgs::PointField field : output.fields)
    {
        if(field.name == "t" && field.datatype == 6)    // 6 is uint32
        {
            is_ouster_like = true;
            std::cout << "Pointcloud is the Ouster type." << std::endl;
            break;
        }
        if(field.name == "time" && field.datatype == 7)   // 7 is float 32
        {
            is_velodyne_like = true;
            std::cout << "Pointcloud is the Velodyne type." << std::endl;
            break;
        }
        if(field.name == "timestamp" && field.datatype == 8)   // 8 is float 64
        {
            is_hesai_like = true;
            std::cout << "Pointcloud Hilti is the Hesai type." << std::endl;
            break;
        }

    }

    if( !(is_ouster_like || is_velodyne_like || is_hesai_like))
    {
        ROS_WARN("The input point cloud does not contain 'time' or 't' or 'timestamp' field. Skipping.");
        return;
    }

    bool success = true;
    if (is_ouster_like) ouster_cloud_deskew(output, success);
    if (is_velodyne_like) velodyne_cloud_deskew(output, success);
    if (is_hesai_like) hesai_cloud_deskew(output, success);

    //Publish the data.
    if(success)
    {
        pub.publish(output);
    }
    else{
        ROS_WARN("Deskewing failed, skipping.");
        return;
    }

    auto end = std::chrono::steady_clock::now();
    std::cout << "... done." << std::endl;
    std::cout << "Elapsed callback time in milliseconds : "
         << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
         << " ms. " << std::endl;
}

// deskewing ouster-like sensors
void ouster_cloud_deskew(sensor_msgs::PointCloud2 &output, bool &success)
{
    std::string time_field_name = "t";
    auto start = std::chrono::steady_clock::now();

    // Iterators over the pointcloud2 message
    sensor_msgs::PointCloud2Iterator<uint32_t> iter_t(output, time_field_name);

    // Dictionary to store transforms already looked up
    std::unordered_map<int32_t, tf::StampedTransform> tfs_cache;   // Originally, I used uint32 as a key, but Velodyne came with negative
    tfs_cache.reserve(expected_number_of_pcl_columns);             // values. As long as the single scan takes shorter time than 2 seconds, we should fit.

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
        success = false;
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
                success = false;
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

    std::cout << "Elapsed callback time in waiting for transform: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(after_waitForTransform - start).count()
              << " ms." << std::endl;
    std::cout << "Number of tf's fetched: " << tfs_cache.size() << std::endl;
}

void velodyne_cloud_deskew(sensor_msgs::PointCloud2 &output, bool &success)
{
    std::string time_field_name = "time";
    auto start = std::chrono::steady_clock::now();

    // Iterators over the pointcloud2 message
    sensor_msgs::PointCloud2Iterator<float> iter_t(output, time_field_name);

    // Dictionary to store transforms already looked up
    std::unordered_map<int32_t, tf::StampedTransform> tfs_cache;   // Velodyne has negative time, so int32 instead of uint
    tfs_cache.reserve(expected_number_of_pcl_columns);

    float latest_time = 0;
    int32_t current_point_time = 0;

    // Find the latest time
    for (; iter_t != iter_t.end(); ++iter_t)
    {
        if(*iter_t>latest_time) latest_time=*iter_t;
    }

    //wait for the latest transform
    ros::Time last_laser_beam_time = output.header.stamp + ros::Duration(latest_time);
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
        success = false;
        return;
    }

    auto after_waitForTransform = std::chrono::steady_clock::now();

    //reset the iterators
    iter_t = sensor_msgs::PointCloud2Iterator<float>(output, time_field_name);
    sensor_msgs::PointCloud2Iterator<float> iter_xyz(output, "x");    // xyz are consecutive, y~iter_xzy[1], z~[2]

    // iterate over the pointcloud, lookup tfs and apply them
    for (; iter_t != iter_t.end(); ++iter_t, ++iter_xyz)
    {
        // convert to int and round
        current_point_time = (int32_t)(*iter_t * 1000000000);                        //convert to nanoseconds integer
        current_point_time = (current_point_time/round_to_intervals_of_nanoseconds)*round_to_intervals_of_nanoseconds;
        //std::cout << "Exact: " << *iter_t * 1000000000 << "ns, rounded: " << current_point_time << "ns." << std::endl;

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
                success = false;
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

    std::cout << "Elapsed callback time in waiting for transform: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(after_waitForTransform - start).count()
              << " ms." << std::endl;
    std::cout << "Number of tf's fetched: " << tfs_cache.size() << std::endl;
}

void hesai_cloud_deskew(sensor_msgs::PointCloud2 &output, bool &success)
{
    std::string time_field_name = "timestamp";
    auto start = std::chrono::steady_clock::now();

    // Iterators over the pointcloud2 message
    sensor_msgs::PointCloud2Iterator<double> iter_t(output, time_field_name);

    // Dictionary to store transforms already looked up
    std::unordered_map<int64_t, tf::StampedTransform> tfs_cache; 
    tfs_cache.reserve(expected_number_of_pcl_columns);

    double latest_time = 0;
    int64_t current_point_time = 0;

    // Find the latest time
    for (; iter_t != iter_t.end(); ++iter_t)
    {
        if(*iter_t>latest_time) latest_time=*iter_t;
    }

    //wait for the latest transform
    ros::Time last_laser_beam_time(latest_time);
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
        success = false;
        return;
    }

    auto after_waitForTransform = std::chrono::steady_clock::now();

    //reset the iterators
    iter_t = sensor_msgs::PointCloud2Iterator<double>(output, time_field_name);
    sensor_msgs::PointCloud2Iterator<float> iter_xyz(output, "x");    // xyz are consecutive, y~iter_xzy[1], z~[2]

    // iterate over the pointcloud, lookup tfs and apply them
    for (; iter_t != iter_t.end(); ++iter_t, ++iter_xyz)
    {
        tf::StampedTransform transform;
        if(tfs_cache.count(*iter_t) == 0) // we haven't looked for the transform for that time yet
        {
            ros::Time laser_beam_time(*iter_t);
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
                success = false;
                return;
            }

            tfs_cache[current_point_time] = transform;
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

    std::cout << "Elapsed callback time in waiting for transform: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(after_waitForTransform - start).count()
              << " ms." << std::endl;
    std::cout << "Number of tf's fetched: " << tfs_cache.size() << std::endl;
}


int main (int argc, char** argv)
{
    // Initialize ROS
    ros::init (argc, argv, "pointcloud2_deskew");
    ros::NodeHandle nh;
    ros::NodeHandle pnh("~");       // private node handle

    // Load Params
    pnh.param("fixed_frame_for_lidar", fixed_frame_for_laser, std::string("odom"));
    pnh.param("expected_pointcloud_columns", expected_number_of_pcl_columns, 4000);
    pnh.param("round_to_n_nanoseconds", round_to_intervals_of_nanoseconds, 50000);

    // Create a ROS subscriber for the input point cloud
    ros::Subscriber sub = nh.subscribe ("input_point_cloud", 20, cloud_callback);

    // Create a transform listener
    tf_ptr.reset(new tf::TransformListener);

    // Create a ROS publisher for the output point cloud
    pub = nh.advertise<sensor_msgs::PointCloud2> ("output_point_cloud", 20);

    // Spin
    ros::spin ();
}
