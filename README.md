# sift
## overview
get.it.done.

## usage
TODO

## results
    hamersaw@nightcrawler:~/development/sift$ time ./target/debug/sift index -b 1 -t 12 ~/downloads/sustain-data/region/tl_2010_us_county10.shp ~/downloads/sustain-data/macav2/macav2metdata_tasmax_NorESM1-M_r1i1p1_historical_2005_2005_CONUS_daily.nc > ~/downloads/county-index.csv

    real	21m57.346s
    user	258m27.184s
    sys     0m3.736s

    hamersaw@nightcrawler:~/development/sift$ time ./target/debug/sift dump ~/downloads/county-index.csv ~/downloads/sustain-data/macav2/macav2metdata_*.nc > ~/downloads/macav2-sift.csv

    real	2m33.676s
    user	17m35.131s
    sys	    0m10.392s

## todo
- add help messages to all structopt definitions
- implement aggregate - aggregate csv data along variable intervals
