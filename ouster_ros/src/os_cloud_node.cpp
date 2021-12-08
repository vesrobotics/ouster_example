/**
 * @file
 * @brief Example node to publish point clouds and imu topics
 */

#include <ros/console.h>
#include <ros/ros.h>
#include <ros/service.h>
#include <sensor_msgs/Imu.h>
#include <sensor_msgs/PointCloud2.h>
#include <tf2_ros/static_transform_broadcaster.h>
#include <pcl/point_types.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <pcl_conversions/pcl_conversions.h>

#include "ouster/lidar_scan.h"
#include "ouster/types.h"
#include "ouster_ros/OSConfigSrv.h"
#include "ouster_ros/PacketMsg.h"
#include "ouster_ros/ros.h"

using PacketMsg = ouster_ros::PacketMsg;
using Cloud = ouster_ros::Cloud;
using Point = ouster_ros::Point;
namespace sensor = ouster::sensor;

struct PacketHeader{
    uint64_t ts;
    uint16_t mId, fId;
    uint32_t encCount;
};

struct ChannelDataBlock{
    static const size_t BLOCK_SIZE = sizeof(uint8_t)*12;
    uint32_t range;
    uint8_t calRef;
    uint16_t sigPhot;
    uint16_t infrared;
};

PacketHeader getPacketHeader(const PacketMsg& _pkt){
    auto ptr = _pkt.buf.data();
    PacketHeader header;
    
    memcpy(&header.ts,          ptr, sizeof(uint64_t)); ptr += sizeof(uint64_t);
    memcpy(&header.mId,         ptr, sizeof(uint16_t)); ptr += sizeof(uint16_t);
    memcpy(&header.fId,         ptr, sizeof(uint16_t)); ptr += sizeof(uint16_t);
    memcpy(&header.encCount,    ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);

    return header;
}

const uint8_t* blockPointer(const PacketMsg &_pkt, size_t _idx){
    return _pkt.buf.data()+sizeof(PacketHeader)+ChannelDataBlock::BLOCK_SIZE*_idx;
}

ChannelDataBlock parseBlock(const PacketMsg &_pkt, size_t _idx){
    auto ptr = blockPointer(_pkt, _idx);
    ChannelDataBlock data;

    memcpy(&data.range,     ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
    memcpy(&data.calRef,    ptr, sizeof(uint8_t));  ptr += sizeof(uint8_t)*2;   // Skip 8 bits unused
    memcpy(&data.sigPhot,   ptr, sizeof(uint16_t)); ptr += sizeof(uint16_t);
    memcpy(&data.infrared,  ptr, sizeof(uint16_t)); ptr += sizeof(uint16_t)*2;   // Skip 16 bits unused

    return data;
}

Cloud packetToCloud(const PacketMsg &_pkt, sensor::sensor_info _info){
    
    PacketHeader header = getPacketHeader(_pkt);
    auto pf = sensor::get_format(_info);

    printf("--> %ld, %d, %d, %d\n", header.ts, header.mId, header.fId, header.encCount);

    // N Channel data blocks
    double n = _info.lidar_origin_to_beam_origin_mm;
    const double azimuth_radians = M_PI * 2.0 / 1024;

    Cloud cloud;
    int counter =0;
    cloud.resize(pf.columns_per_packet*pf.pixels_per_column);
    for(int v = 0; v<pf.columns_per_packet;v++){
        for(int u = 0; u < pf.pixels_per_column; u++){
            // Just read range and ignore the rest
            auto dataBlock = parseBlock(_pkt, u);

            double encoder = 2.0 * M_PI - ((header.mId+v) * azimuth_radians);
            double azimuth = -_info.beam_azimuth_angles[u] * M_PI / 180.0;
            double altitude = _info.beam_altitude_angles[u] * M_PI / 180.0;

            double x = (dataBlock.range-n)*cos(encoder+azimuth)*cos(altitude)+n*cos(encoder);
            double y = (dataBlock.range-n)*sin(encoder+azimuth)*cos(altitude)+n*sin(encoder);
            double z = (dataBlock.range-n)*sin(altitude);
            cloud.at(counter) = Point{{{  
                    static_cast<float>(x/1000.0f), 
                    static_cast<float>(y/1000.0f),
                    static_cast<float>(z/1000.0f), 1.0f}},
                static_cast<float>(dataBlock.sigPhot),
                static_cast<uint32_t>(header.ts),
                static_cast<uint16_t>(dataBlock.calRef),
                static_cast<uint8_t>(u),
                static_cast<uint16_t>(dataBlock.infrared),
                static_cast<uint32_t>(dataBlock.range)
            };

            counter++;
            // printf("%d, %d, %f, %f, %f, %f, %f, %f",i,j, encoder, azimuth, altitude, x, y, z);

        }
        // getchar();
    }
    return cloud;
}

int main(int argc, char** argv) {
    ros::init(argc, argv, "os_cloud_node");
    ros::NodeHandle nh("~");

    auto tf_prefix = nh.param("tf_prefix", std::string{});
    if (!tf_prefix.empty() && tf_prefix.back() != '/') tf_prefix.append("/");
    auto sensor_frame = tf_prefix + "os_sensor";
    auto imu_frame = tf_prefix + "os_imu";
    auto lidar_frame = tf_prefix + "os_lidar";

    ouster_ros::OSConfigSrv cfg{};
    auto client = nh.serviceClient<ouster_ros::OSConfigSrv>("os_config");
    client.waitForExistence();
    if (!client.call(cfg)) {
        ROS_ERROR("Calling config service failed");
        return EXIT_FAILURE;
    }

    auto info = sensor::parse_metadata(cfg.response.metadata);
    uint32_t H = info.format.pixels_per_column;
    uint32_t W = info.format.columns_per_frame;

    auto pf = sensor::get_format(info);

    auto lidar_pub = nh.advertise<sensor_msgs::PointCloud2>("points", 10);
    auto packet_pub = nh.advertise<sensor_msgs::PointCloud2>("points_packet", 100);
    auto imu_pub = nh.advertise<sensor_msgs::Imu>("imu", 100);

    auto xyz_lut = ouster::make_xyz_lut(info);

    Cloud cloud{W, H};
    ouster::LidarScan ls{W, H};

    ouster::ScanBatcher batch(W, pf);

    auto lidar_handler = [&](const PacketMsg& pm) mutable {
        if (batch(pm.buf.data(), ls)) {
            auto h = std::find_if(
                ls.headers.begin(), ls.headers.end(), [](const auto& h) {
                    return h.timestamp != std::chrono::nanoseconds{0};
                });
            if (h != ls.headers.end()) {
                scan_to_cloud(xyz_lut, h->timestamp, ls, cloud);
                lidar_pub.publish(ouster_ros::cloud_to_cloud_msg(
                    cloud, h->timestamp, sensor_frame));
            }
        }
    };

    auto imu_handler = [&](const PacketMsg& p) {
        imu_pub.publish(ouster_ros::packet_to_imu_msg(p, imu_frame, pf));
    };

    auto lidar_packet_sub = nh.subscribe<PacketMsg, const PacketMsg&>(
        "lidar_packets", 2048, lidar_handler);


    auto imu_packet_sub = nh.subscribe<PacketMsg, const PacketMsg&>(
        "imu_packets", 100, imu_handler);

    // publish transforms
    tf2_ros::StaticTransformBroadcaster tf_bcast{};

    tf_bcast.sendTransform(ouster_ros::transform_to_tf_msg(
        info.imu_to_sensor_transform, sensor_frame, imu_frame));

    tf_bcast.sendTransform(ouster_ros::transform_to_tf_msg(
        info.lidar_to_sensor_transform, sensor_frame, lidar_frame));


    auto lidar_packet_sub2 = nh.subscribe<PacketMsg, const PacketMsg&>(
        "lidar_packets", 2048, [&](const PacketMsg& pm){
            Cloud cloud = packetToCloud(pm, info);
            // getchar();
            sensor_msgs::PointCloud2 msg;
            pcl::toROSMsg(cloud, msg);
            msg.header.frame_id = sensor_frame;
            msg.header.stamp.fromNSec(ls.headers.begin()->timestamp.count());

            packet_pub.publish(msg);
        });

    ros::spin();

    return EXIT_SUCCESS;
}
