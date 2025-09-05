import os, sys
import json
import pandas as pd
import subprocess
from helpers_conf import *
import tempfile
import zipfile
import er_aws


def create_geojson_image(features, pid, id, **kwargs):
    geo_json = {'type': 'FeatureCollection', 'features': features}
    layer_name = kwargs.get('name')
    json_file = f'tmp-{pid}.json'
    with open(json_file, 'w') as f:
        f.write(json.dumps(geo_json, indent=4))
    if (os.path.exists(json_file)):
        out_img = f'{id}.png'
        if layer_name:
            subprocess.call([
                rscript, 'img_from_geojson.R', '--geojson-file', json_file,
                '--out-img', out_img, '--layer-name', layer_name
            ])
        else:
            subprocess.call([
                rscript, 'img_from_geojson.R', '--geojson-file', json_file,
                '--out-img', out_img
            ])
        print(f'Created {out_img}')
        # os.remove(json_file)


def create_circle_geom_image(features, id, **kwargs):
    lats = []
    lons = []
    radii = []
    layer_name = kwargs.get('name')
    for f in features:
        lats.append(f['CenterLat'])
        lons.append(f['CenterLon'])
        radii.append(f['Radii'])
    csv_file = f'{id}.csv'
    pd.DataFrame.from_dict({
        'lats': lats,
        'lons': lons,
        'radii': radii
    }).to_csv(csv_file, index=False)
    if (os.path.exists(csv_file)):
        out_img = f'{id}.png'
        if layer_name:
            subprocess.call([
                rscript, 'img_from_geojson.R', '--csv-file', csv_file,
                '--out-img', out_img, '--layer-name', layer_name
            ])
        else:
            subprocess.call([
                rscript, 'img_from_geojson.R', '--csv-file', csv_file,
                '--out-img', out_img
            ])
        print(f'Created {out_img}')
        # os.remove(csv_file)


def get_event_shape(ed_dict, id, **kwargs):
    shape_type = ed_dict['SHAPE_TYPE']
    print(f'Shape Type: {shape_type}')
    if (shape_type.endswith('JSON')):
        features = ed_dict['SHAPE_JSON']['features']
        circle_geom = False
        try:
            for f in features:
                if f['properties']['UserShapeType'] == 'Circle':
                    circle_geom = True
                    break
        except KeyError:
            pass

        pid = os.getpid()

        if circle_geom:
            create_circle_geom_image(features, id, **kwargs)
        else:
            create_geojson_image(features, pid, id, **kwargs)
    elif shape_type == 'TIFF':
        big_tiff = ed_dict['big_tiff_flag']
        if big_tiff == 'Y':
            print("Bigtiff event")
            return
        with open(f'{id}.tif', 'wb') as f:
            f.write(ed_dict['SHAPE'])
        # tiff_file_name = ed_dict['tiff_file_name'].strip()
        # if len(tiff_file_name) == 0:
        #     print('no binary file')
        # else:
        #     thumbnail = ed_dict['SHAPE_THUMBNAIL']
        #     tiff_file = ed_dict['SHAPE']
        #     with open(tiff_file_name, 'wb') as f:
        #         f.write(tiff_file)

        #     with open('skg.png', 'wb') as f:
        #         f.write(thumbnail)
        # tiff_prefix = os.path.splitext(tiff_file_name)[0]
        # zip_name = f'{tiff_prefix}.7z'
        # zip_path = os.path.join(tempfile.gettempdir(), zip_name)
        # bucket = os.environ['SMALL_EVENT_S3_BUCKET']
        # er_aws.download_file(bucket, zip_name, out=zip_path)
