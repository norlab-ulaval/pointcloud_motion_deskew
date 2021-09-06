#ifndef POINTCLOUD_MOTION_DESKEW_POINTCLOUD2_DESKEW_H
#define POINTCLOUD_MOTION_DESKEW_POINTCLOUD2_DESKEW_H

#include <nodelet/nodelet.h>

class PointCloud2Deskew : public nodelet::Nodelet
{
public:
    virtual void onInit();
};

#endif
