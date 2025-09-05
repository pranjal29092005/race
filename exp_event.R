library(pacman)
pacman::p_load(tidyverse, geojsonsf, leaflet, mapview, argparser, fs)

parser = arg_parser('Geojson to Image converter') %>%
    add_argument('--exp', help = "exp file", flag = F) %>%
    add_argument('--evt', help = "evt name", flag = F) %>%
    add_argument('--out', help = "out name", flag = F)

c_args = commandArgs(trailingOnly = T)

argv = parse_args(parser, argv = c_args)

exp = geojson_sf(fs::path(argv$exp))
evt = geojson_sf(fs::path(argv$evt))

m = leaflet() %>% addTiles()


m %>%
    addPolygons(data=exp, color='red', weight=1) %>%
    addPolygons(data=evt, color='green', weight=2) %>%
    mapshot(file=fs::path(argv$out))
