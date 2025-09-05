import os, sys, subprocess
import rasterio, argparse
from pathlib import Path
import shutil
from icecream import ic


class GeotiffTranslate:
    def __init__(self, input_file, output_file, **kwargs):
        self.input_file = input_file
        self.output_file = output_file
        if not os.path.exists(input_file):
            raise FileNotFoundError(input_file)
        self.delta_lon = kwargs.get('delta_lon', 0.0)
        self.delta_lat = kwargs.get('delta_lat', 0.0)

    def execute(self):
        ulx = None
        lrx = None
        uly = None
        lry = None
        with rasterio.open(self.input_file) as src:
            bounds = src.bounds
            ic(bounds)
            ic(self.delta_lon)
            ic(self.delta_lat)
            ulx = bounds.left + self.delta_lon
            lrx = bounds.right + self.delta_lon
            uly = bounds.top + self.delta_lat
            lry = bounds.bottom + self.delta_lat
        cmd = [
            shutil.which('gdal_translate'), '-b', '1', '-a_ullr', f'{ulx}', f'{uly}', f'{lrx}', f'{lry}',
            self.input_file, self.output_file
        ]
        ic(cmd)
        subprocess.call(cmd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='geotiff-translate')
    parser.add_argument('--input', '-i', dest='input', required=True, type=str)
    parser.add_argument('--output', '-o', dest='output', required=False, type=str)
    parser.add_argument('--dlon', dest='delta_lon', required=False, type=float)
    parser.add_argument('--dlat', dest='delta_lat', required=False, type=float)

    argv = parser.parse_args()
    output_file = argv.output
    if output_file is None:
        fn, ext = os.path.splitext(os.path.basename(argv.input))
        output_file = os.path.join(os.path.dirname(argv.input), f'{fn}_translated{ext}')
    delta_lon = argv.delta_lon if argv.delta_lon else 0.0
    delta_lat = argv.delta_lat if argv.delta_lat else 0.0
    gt = GeotiffTranslate(argv.input, output_file, delta_lat=delta_lat, delta_lon=argv.delta_lon)
    gt.execute()
