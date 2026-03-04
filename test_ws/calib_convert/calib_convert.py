#!/usr/bin/env python3
"""Convert calib.json to calib.yaml (OpenCV YAML format).

Reads calib.json and generates calib_output.yaml in OpenCV YAML format,
using calib.yaml as the reference template structure.
Fields not present in calib.json are filled with zeros.
"""

import json
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_INPUT = os.path.join(SCRIPT_DIR, 'calib.json')
YAML_OUTPUT = os.path.join(SCRIPT_DIR, 'calib_output.yaml')

# JSON projection_type -> YAML model_type mapping.
# Empirically derived from the two files: cameras labelled 'cylinder' in the
# JSON (wide-angle surround cameras) are calibrated with the PINHOLE model in
# the YAML, while cameras labelled 'pinhole' (narrower side cameras) are
# actually fisheye lenses calibrated with the KANNALA_BRANDT model.
PROJECTION_TO_MODEL = {
    'cylinder': 'PINHOLE',
    'pinhole': 'KANNALA_BRANDT',
}

# Sensor list derived from calib.yaml template: (sensor_name, default_model_type)
# model_type None means lidar (valid: false, no intrinsic parameters)
SENSOR_TEMPLATE = [
    ('camera_backward',               'PINHOLE'),
    ('camera_backward_redundant',     'PINHOLE'),
    ('camera_forward_far',            'PINHOLE'),
    ('camera_forward_wide',           'PINHOLE'),
    ('camera_forward_wide_redundant', 'PINHOLE'),
    ('camera_left_front',             'KANNALA_BRANDT'),
    ('camera_left_rear',              'KANNALA_BRANDT'),
    ('camera_right_front',            'KANNALA_BRANDT'),
    ('camera_right_rear',             'KANNALA_BRANDT'),
    ('camera_sur_front',              'KANNALA_BRANDT'),
    ('camera_sur_left',               'KANNALA_BRANDT'),
    ('camera_sur_rear',               'KANNALA_BRANDT'),
    ('camera_sur_right',              'KANNALA_BRANDT'),
    ('lidar_primary_front',           None),
    ('lidar_primary_left',            None),
    ('lidar_primary_rear',            None),
    ('lidar_primary_right',           None),
    ('lidar_short_front',             None),
    ('lidar_short_left',              None),
    ('lidar_short_rear',              None),
    ('lidar_short_right',             None),
]

ZERO_4X4 = [
    [0., 0., 0., 0.],
    [0., 0., 0., 0.],
    [0., 0., 0., 0.],
    [0., 0., 0., 0.],
]


def fmt_float(val):
    """Format a float for OpenCV YAML output (scientific notation or compact zero/one)."""
    if val == 0.0:
        return '0.'
    if val == 1.0:
        return '1.'
    return f'{val:.16e}'


def format_extrinsic_data(matrix_4x4):
    """Format a 4x4 matrix as an OpenCV YAML data array (one row per line)."""
    flat = [v for row in matrix_4x4 for v in row]
    vals = [fmt_float(v) for v in flat]

    prefix = '      data: [ '
    indent = '          '
    lines = []
    for i in range(0, 16, 4):
        chunk = vals[i:i + 4]
        if i + 4 < 16:
            lines.append(', '.join(chunk) + ',')
        else:
            lines.append(', '.join(chunk) + ' ]')

    result = prefix + lines[0]
    for line in lines[1:]:
        result += '\n' + indent + line
    return result


def write_sensor_block(out, name, model_type, cam_data):
    """Write one sensor block to the output YAML file."""
    out.write(f'{name}:\n')

    if model_type is None:
        # Lidar: no intrinsic parameters
        out.write('   Intrinsic_param:\n')
        out.write('      valid: false\n')
    else:
        # Camera: extract or zero-fill intrinsic parameters
        if cam_data:
            K = cam_data['camera_intrinsics']['K']
            fx, fy = K[0][0], K[1][1]
            cx, cy = K[0][2], K[1][2]
            width = cam_data['width']
            height = cam_data['height']
        else:
            fx = fy = cx = cy = 0.0
            width = height = 0

        out.write('   Intrinsic_param:\n')
        out.write('      valid: true\n')
        out.write(f'      model_type: {model_type}\n')
        out.write(f'      camera_name: {name}\n')
        out.write(f'      image_width: {width}\n')
        out.write(f'      image_height: {height}\n')

        if model_type == 'PINHOLE':
            out.write('      distortion_parameters:\n')
            out.write(f'         k1: {fmt_float(0.0)}\n')
            out.write(f'         k2: {fmt_float(0.0)}\n')
            out.write(f'         p1: {fmt_float(0.0)}\n')
            out.write(f'         p2: {fmt_float(0.0)}\n')
            out.write(f'         k3: {fmt_float(0.0)}\n')
            out.write(f'         k4: {fmt_float(0.0)}\n')
            out.write(f'         k5: {fmt_float(0.0)}\n')
            out.write(f'         k6: {fmt_float(0.0)}\n')
            out.write('      projection_parameters:\n')
            out.write(f'         fx: {fmt_float(fx)}\n')
            out.write(f'         fy: {fmt_float(fy)}\n')
            out.write(f'         cx: {fmt_float(cx)}\n')
            out.write(f'         cy: {fmt_float(cy)}\n')
        elif model_type == 'KANNALA_BRANDT':
            out.write('      projection_parameters:\n')
            out.write(f'         k1: {fmt_float(0.0)}\n')
            out.write(f'         k2: {fmt_float(0.0)}\n')
            out.write(f'         k3: {fmt_float(0.0)}\n')
            out.write(f'         k4: {fmt_float(0.0)}\n')
            out.write(f'         fx: {fmt_float(fx)}\n')
            out.write(f'         fy: {fmt_float(fy)}\n')
            out.write(f'         cx: {fmt_float(cx)}\n')
            out.write(f'         cy: {fmt_float(cy)}\n')

    # Extrinsic parameter (all zeros for sensors not in JSON)
    matrix = cam_data['camera2vcs'] if cam_data and 'camera2vcs' in cam_data else ZERO_4X4

    out.write('   Extrinsic_param: !!opencv-matrix\n')
    out.write('      rows: 4\n')
    out.write('      cols: 4\n')
    out.write('      dt: d\n')
    out.write(format_extrinsic_data(matrix) + '\n')
    out.write('#\n')
    out.write('#\n')


def main():
    with open(JSON_INPUT, 'r', encoding='utf-8') as f:
        calib_json = json.load(f)

    template_names = {name for name, _ in SENSOR_TEMPLATE}

    with open(YAML_OUTPUT, 'w', encoding='utf-8') as out:
        # YAML header
        out.write('%YAML:1.0\n')
        out.write('---\n')

        # calib_meta section (not in JSON, fill with empty strings)
        out.write('calib_meta:\n')
        out.write('   vehicle_id: ""\n')
        out.write('   calib_version: ""\n')
        out.write('   calib_details: ""\n')
        out.write('   calib_date: ""\n')
        out.write('#\n')
        out.write('#\n')
        out.write('reference_at_body: 1\n')
        out.write('#\n')
        out.write('#\n')

        # Write sensors from template (in template order)
        for sensor_name, default_model_type in SENSOR_TEMPLATE:
            cam_data = calib_json.get(sensor_name)

            if cam_data:
                proj_type = cam_data.get('projection_type', '')
                model_type = PROJECTION_TO_MODEL.get(proj_type, default_model_type)
            else:
                model_type = default_model_type

            write_sensor_block(out, sensor_name, model_type, cam_data)

        # Write any extra sensors in JSON that are not in the template
        for sensor_name, cam_data in calib_json.items():
            if sensor_name not in template_names:
                proj_type = cam_data.get('projection_type', '')
                model_type = PROJECTION_TO_MODEL.get(proj_type, 'PINHOLE')
                write_sensor_block(out, sensor_name, model_type, cam_data)

    print(f'Output written to: {YAML_OUTPUT}')


if __name__ == '__main__':
    main()
