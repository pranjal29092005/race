import os, sys, struct, json
import pandas as pd
import geopandas as gpd
import argparse
import pathlib
from shapely.geometry import Polygon, Point
import numpy as np
import math
from pyproj import Transformer, crs
import rasterio as rio

parser = argparse.ArgumentParser(prog='Get Data at lat/lon from a bigtiff binary')
parser.add_argument('--lat', dest='lat', required=True, type=float)
parser.add_argument('--lon', dest='lon', required=True, type=float)
argv = parser.parse_args()

cur_dir = os.getcwd()
meta_file = list(pathlib.Path(cur_dir).glob('*.meta'))
json_file = list(pathlib.Path(cur_dir).glob('*.json'))

if meta_file:
    meta_file = meta_file[0]
else:
    meta_file = None

if json_file:
    json_file = json_file[0]
else:
    json_file = None

if meta_file is None or json_file is None:
    print('Meta or json file is none')
    sys.exit(-1)

x_coord, y_coord = (argv.lon, argv.lat)
bigtiff_crs = None
bigtiff_code = 'EPSG:4326'

with open(json_file, 'r') as f:
    tmp = json.load(f)
    conf = tmp.get('Configuration')
    if conf is not None:
        bigtiff_code = conf.get('Code', 'EPSG:4326')

        bigtiff_crs = crs.CRS.from_string(bigtiff_code)
        src_crs = crs.CRS.from_string('EPSG:4326')
        transformer = Transformer.from_crs(src_crs, bigtiff_crs, always_xy=True)
        x_coord, y_coord = transformer.transform(argv.lon, argv.lat)

test_point = Point(x_coord, y_coord)
print(test_point)


def get_tile_for_lat_lon(meta):
    for idx, row in meta.iterrows():
        if (row['geometry'].contains(test_point)):
            return row['name']
    return None


def get_data(fn):
    with open(fn, 'rb') as f:
        xs, ys = struct.unpack('2i', f.read(8))
        ullon, ullat, lrlon, lrlat = struct.unpack('4d', f.read(32))
        min, max = struct.unpack('2d', f.read(16))
        data_len = xs * ys * 4
        arr = np.frombuffer(f.read(data_len), dtype=np.float32)
        xscale = (x_coord - ullon) / (lrlon - ullon)
        yscale = (y_coord - ullat) / (lrlat - ullat)
        xpix = int(xscale * xs)
        ypix = int(yscale * ys)
        print(xpix, ypix)
        return arr[xpix + (ypix * xs)]


meta = pd.read_csv(
    meta_file,
    header=None,
    names=['name', 'ullon', 'ullat', 'lrlon', 'lrlat', 'max_intensity'],
    skip_blank_lines=True,
    delimiter=','
)
geoms = []
for idx, row in meta.iterrows():
    lon_lat_list = [
        [row['ullon'], row['ullat']], [row['lrlon'], row['ullat']], [row['lrlon'], row['lrlat']],
        [row['ullon'], row['lrlat']], [row['ullon'], row['ullat']]
    ]
    geoms.append(Polygon(lon_lat_list))

df = gpd.GeoDataFrame(meta[['name', 'max_intensity']], crs=bigtiff_code, geometry=geoms)

fn = get_tile_for_lat_lon(df)

if fn is None:
    print('no file found')
    sys.exit(-1)

fn = os.path.join(cur_dir, fn)
pfx, extn = os.path.splitext(fn)
fn = f'{pfx}.bin'

data = get_data(fn)
print(data)
