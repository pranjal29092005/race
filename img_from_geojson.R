library(pacman)
pacman::p_load(tidyverse, rgdal, leaflet, mapview, argparser, fs)

parser = arg_parser('Geojson to Image converter')
parser = add_argument(parser, '--geojson-file', help = "Geojson file", flag = F)
parser = add_argument(parser, '--layer-name', help = 'Layer name', flag = F)
parser = add_argument(parser, '--csv-file', help = 'Csv file', flag = F)
parser = add_argument(parser, '--out-img', help = "Output file", flag = F)

c_args = commandArgs(trailingOnly = T)

argv = parse_args(parser, argv = c_args)

gj.file = argv$geojson_file
csv.file = argv$csv_file

layer_name = argv$layer_name

if (fs::file_exists(gj.file)) {
    geom = rgdal::readOGR(gj.file)
    if (is.na(layer_name)) {
        m = mapview(geom)
    } else {
        m = mapview(geom, layer.name=layer_name)
    }
    mapshot (m, file=argv$out_img)
} else if (fs::file_exists(csv.file)) {
    geom = read_csv(csv.file) %>%
        mutate (radii = radii * 1000)

    num = nrow(geom)

    ll = leaflet() %>%
        addTiles() %>%
        addCircles(lat=~lats, lng=~lons, radius=~radii, data=geom)

    if (num == 1) {
        ll = ll %>% setView(pull(geom, lons), pull(geom, lats), zoom=5)
    }
    
    mapshot(ll, file=argv$out_img)
}
