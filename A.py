import datetime
import logging
from typing import Dict

from service.draw_recog_instance.reproj_point_type_enum import *
from service.draw_recog_instance.CamUndistortion import *
from autohdmap_micro_services.proto.generated import track_pb2

import struct
import json
import os
import requests
import time
import signal
import tarfile
import hdfs
from fastapi import Response
import cv2
import zlib



class RecognitionFormatHeader:
    def __init__(self):
        self.det_model_version = 0
        self.seg_model_version = 0
        self.sign_model_version = 0
        self.pole_model_version = 0
        self.ground_model_version = 0
        self.camera_model_version = 0
        self.lamp_model_version = 0
        self.reserved = [0] * 3
        self.struct_version = 0
        self.segcontour_coordinate_vector_offset = 0
        self.segcontour_coordinate_vector_size = 0
        self.segcontour_basicinfo_vector_offset = 0
        self.segcontour_basicinfo_vector_size = 0
        self.segcontour_extrainfo_vector_offset = 0
        self.segcontour_extrainfo_vector_size = 0
        self.size_in_all = 0
        self.image_width = 0
        self.image_height = 0
        self.padding = [0] * 84

    @staticmethod
    def parse_from_file(file_path):
        header = RecognitionFormatHeader()
        with open(file_path, 'rb') as f:
            data = f.read(struct.calcsize('7B 3B H 7I 2H 84x'))
            values = struct.unpack('7B 3B H 7I 2H 84x', data)
            header.det_model_version, header.seg_model_version, header.sign_model_version, \
                header.pole_model_version, header.ground_model_version, header.camera_model_version, \
                header.lamp_model_version, reserved1, reserved2, reserved3, header.struct_version, \
                header.segcontour_coordinate_vector_offset, header.segcontour_coordinate_vector_size, \
                header.segcontour_basicinfo_vector_offset, header.segcontour_basicinfo_vector_size, \
                header.segcontour_extrainfo_vector_offset, header.segcontour_extrainfo_vector_size, \
                header.size_in_all, header.image_width, header.image_height = values
            header.reserved = [reserved1, reserved2, reserved3]
            header.padding = values[-84:]
        return header


class SegContourCoordinate:
    def __init__(self, x, y):
        self.x = x
        self.y = y


class SegContourBasicInfo:
    def __init__(self, recognition_source, r, g, b, reproj_type, reserved, confidence, segcontour_coordinate_offset,
                 segcontour_coordinate_size, segcontour_extrainfo_offset, segcontour_extrainfo_size):
        self.recognition_source = recognition_source
        self.r = r
        self.g = g
        self.b = b
        self.reproj_type = reproj_type
        self.reserved = reserved
        self.confidence = confidence
        self.segcontour_coordinate_offset = segcontour_coordinate_offset
        self.segcontour_coordinate_size = segcontour_coordinate_size
        self.segcontour_extrainfo_offset = segcontour_extrainfo_offset
        self.segcontour_extrainfo_size = segcontour_extrainfo_size


class SegContourExtraInfo:
    def __init__(self, type, subtype, shape, confidence, cont):
        self.type = type
        self.subtype = subtype
        self.shape = shape
        self.confidence = confidence
        self.cont = cont


class SegContour:
    def __init__(self):
        self.segcontour_coordinates = []
        self.segcontour_basicinfo = None
        self.segcontour_extrainfo = None

    @staticmethod
    def from_data(basic_info, coordinates, extra_infos):
        segcontour = SegContour()
        segcontour.segcontour_basicinfo = basic_info
        segcontour.segcontour_coordinates = [np.array([coord.x, coord.y]) for coord in coordinates[
                                                                                       basic_info.segcontour_coordinate_offset // sizeof(
                                                                                           SegContourCoordinate): basic_info.segcontour_coordinate_offset // sizeof(
                                                                                           SegContourCoordinate) + basic_info.segcontour_coordinate_size // sizeof(
                                                                                           SegContourCoordinate)]]
        if basic_info.segcontour_extrainfo_size > 0:
            extra_info_idx = basic_info.segcontour_extrainfo_offset // sizeof(SegContourExtraInfo)
            segcontour.segcontour_extrainfo = extra_infos[extra_info_idx]
        return segcontour


def read_recognition_format_file(input_file):
    segcontours = list()
    header = RecognitionFormatHeader.parse_from_file(input_file)

    with open(input_file, 'rb') as f:
        # Read contour_coordinates data
        f.seek(header.segcontour_coordinate_vector_offset)
        segcontour_coordinates = []
        for _ in range(header.segcontour_coordinate_vector_size // 4):  # 4 bytes per coordinate (2 int16)
            x, y = struct.unpack('2H', f.read(4))
            segcontour_coordinates.append(SegContourCoordinate(x, y))

        # Read contour_basicinfos data
        f.seek(header.segcontour_basicinfo_vector_offset)
        segcontour_basicinfos = []
        for _ in range(header.segcontour_basicinfo_vector_size // 28):  # 28 bytes per basic info
            recognition_source, r, g, b, reproj_type, reserved, confidence, segcontour_coordinate_offset, segcontour_coordinate_size, segcontour_extrainfo_offset, segcontour_extrainfo_size = struct.unpack(
                '4B 2H f 4I', f.read(28))
            segcontour_basicinfos.append(
                SegContourBasicInfo(recognition_source, r, g, b, reproj_type, reserved, confidence,
                                    segcontour_coordinate_offset, segcontour_coordinate_size,
                                    segcontour_extrainfo_offset, segcontour_extrainfo_size))

        # Read extra_infos data
        f.seek(header.segcontour_extrainfo_vector_offset)
        segcontour_extrainfos = []
        for _ in range(header.segcontour_extrainfo_vector_size // 20):  # 20 bytes per extra info
            type, subtype, shape, confidence = struct.unpack('3H f 8x', f.read(20))  # cont 疑似有对齐问题
            cont = ''
            segcontour_extrainfos.append(SegContourExtraInfo(type, subtype, shape, confidence, cont))

    for basic_info in segcontour_basicinfos:
        segcontour = SegContour()
        segcontour.segcontour_basicinfo = basic_info
        segcontour.segcontour_coordinates = [segcontour_coordinates[basic_info.segcontour_coordinate_offset // 4 + i]
                                             for i in range(basic_info.segcontour_coordinate_size // 4)]

        if basic_info.segcontour_extrainfo_size > 0:
            extra_info_idx = basic_info.segcontour_extrainfo_offset // 20
            segcontour.segcontour_extrainfo = segcontour_extrainfos[extra_info_idx]
        else:
            segcontour.segcontour_extrainfo = None

        segcontours.append(segcontour)

    return header, segcontours


def draw_segcontour_instance(frame, segcontours, alpha=0.5):
    for segcontour in segcontours:
        segcontour_coordinates = segcontour.segcontour_coordinates
        segcontour_basicinfo = segcontour.segcontour_basicinfo
        segcontour_extrainfo = segcontour.segcontour_extrainfo
        reproj_type = int_to_reproj_point_type(segcontour_basicinfo.reproj_type)

        # Assuming GetBoundingBoxOfGivenCoords and GetColorFromReprojType are defined elsewhere
        box_min, box_max = get_bounding_box_of_given_coords(segcontour_coordinates)
        pt1_left_top = (int(box_min[0]), int(box_min[1]))
        pt2_bottom_right = (int(box_max[0]), int(box_max[1]))

        if segcontour_basicinfo.r == 0 and segcontour_basicinfo.g == 0 and segcontour_basicinfo.b == 0:
            color = get_color_from_reproj_type(reproj_type)
        else:
            color = np.array([segcontour_basicinfo.r, segcontour_basicinfo.g, segcontour_basicinfo.b])
        instance_color = (int(color[2]), int(color[1]), int(color[0]))

        if len(segcontour_coordinates) > 0:
            points = [(int(coord.x), int(coord.y)) for coord in segcontour_coordinates]

            # Draw polygon with transparency so later shapes don't fully occlude earlier ones
            overlay = frame.copy()
            cv2.fillPoly(frame, [np.array(points, dtype=np.int32)], color=instance_color, lineType=cv2.LINE_8)
            cv2.addWeighted(overlay, 1 - alpha, frame, alpha, 0, frame)

            # Display info
            display_info = "reproj_type: " + str(segcontour_basicinfo.reproj_type)
            text_rect_pt1 = (int(box_min[0]), int(box_min[1]) - 20)  # Adjust the y-coordinate for text position
            text_pt1 = (int(box_min[0]), int(box_min[1]) - 3)
            cv2.putText(frame, display_info, text_pt1, cv2.FONT_HERSHEY_SIMPLEX, 0.5, instance_color, 1, cv2.LINE_AA)

    # # Save the output image
    # cv2.imwrite(output_file, frame)
    _, png_data = cv2.imencode('.png', frame)
    return png_data


# Helper functions (assuming these are defined elsewhere)
def get_bounding_box_of_given_coords(coords):
    # Implement the logic to get the bounding box of given coordinates
    # This is a placeholder function
    min_x = min(coord.x for coord in coords)
    min_y = min(coord.y for coord in coords)
    max_x = max(coord.x for coord in coords)
    max_y = max(coord.y for coord in coords)
    return np.array([min_x, min_y]), np.array([max_x, max_y])


def get_color_from_reproj_type(reproj_type):
    return recog_color_vector.get(reproj_type, (0, 0, 0))


def sizeof(struct_type):
    return struct.calcsize(struct_type.format)






if __name__ == "__main__":
    crt_time_dat_file_path = ""
    output_file_path = ""
    header, segcontours = read_recognition_format_file(crt_time_dat_file_path)
    frame = np.zeros((header.image_height, header.image_width, 3), dtype=np.uint8)
    png_data = draw_segcontour_instance(frame, segcontours)
    encode_img = cv2.imdecode(png_data, cv2.IMREAD_UNCHANGED)
    cv2.imwrite(output_file_path, encode_img)