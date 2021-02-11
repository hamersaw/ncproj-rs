# ncproj
## overview
get.it.done.

## usage
TODO

## results
    hamersaw@nightcrawler:~/development/sift$ time ./target/debug/sift index -b 1 -t 12 ~/downloads/sustain-data/region/tl_2010_us_county10.shp ~/downloads/sustain-data/macav2/macav2metdata_tasmax_NorESM1-M_r1i1p1_historical_2005_2005_CONUS_daily.nc > ~/downloads/county-index.csv

    real	21m57.346s
    user	258m27.184s
    sys     0m3.736s

    hamersaw@nightcrawler:~/development/sift$ time ./target/debug/sift index -b 2 -t 12 ~/downloads/sustain-data/region/tl_2010_us_county10.shp ~/downloads/sustain-data/macav2/macav2metdata_tasmax_NorESM1-M_r1i1p1_historical_2005_2005_CONUS_daily.nc > ~/downloads/county-index2.csv

    real	59m41.913s
    user	707m1.192s
    sys	    0m6.699s

    hamersaw@nightcrawler:~/development/sift$ time ./target/debug/sift index -b 5 -t 12 ~/downloads/sustain-data/region/tl_2010_us_county10.shp ~/downloads/sustain-data/macav2/2005/macav2metdata_tasmax_NorESM1-M_r1i1p1_historical_2005_2005_CONUS_daily.nc > ~/downloads/county-index5.csv

    real	182m51.109s
    user	2178m42.488s
    sys	    0m13.994s

    hamersaw@nightcrawler:~/development/sift$ time ./target/debug/sift dump -b 250 ~/downloads/county-index2.csv ~/downloads/sustain-data/macav2/2005/macav2metdata_*.nc > /dev/null

    real	0m46.168s
    user	3m5.984s
    sys	    0m1.785s

    hamersaw@nightcrawler:~/development/sift$ time ./target/debug/sift dump -b 100 ~/downloads/county-index2.csv ~/downloads/sustain-data/macav2/2005/macav2metdata_*.nc > /dev/null

    real	0m51.275s
    user	3m2.908s
    sys	    0m1.416s

    hamersaw@nightcrawler:~/development/sift$ time ./target/debug/sift dump -b 50 ~/downloads/county-index2.csv ~/downloads/sustain-data/macav2/2005/macav2metdata_*.nc > /dev/null

    real	1m5.341s
    user	3m17.563s
    sys	    0m1.821s

## todo
- add help messages to all structopt definitions
- implement aggregate - aggregate csv data along variable intervals
- use transitional buffers on dump
    - can be reading data while others are being processed
