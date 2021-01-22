use crossbeam_channel::{Receiver, Sender};
use dbase::FieldValue;
use geo::algorithm::centroid::Centroid;
use geo::algorithm::contains::Contains;
use geo::algorithm::euclidean_distance::EuclideanDistance;
use geo::algorithm::intersects::Intersects;
use geo_types::{LineString, MultiPolygon, Point, Polygon};
use ndarray::ArrayD;
use netcdf::File;
use shapefile::Reader;

use std::collections::BTreeMap;
use std::sync::Arc;

const CONTAINS_BUFFER_SIZE: usize = 5;

fn main() {
    // parse command line arguments
    let args: Vec<String> =  std::env::args().collect();
    if args.len() <= 2 {
        println!("Usage: {} <county-shapefile> variable-files...",
            args[0]);
        return;
    }

    // populate counties map
    let mut counties: BTreeMap<String, (Point<f64>, Polygon<f64>)> =
        BTreeMap::new();

    {
        // open county shapefile reader and iterator
        let reader = match Reader::from_path(&args[1]) {
            Ok(reader) => reader,
            Err(e) => panic!("failed to open shapefile '{}': {}", &args[1], e),
        };

        let iterator = match reader.iter_shapes_and_records_as
                ::<shapefile::Polygon>() {
            Ok(iterator) => iterator,
            Err(e) => panic!("failed to open iterator: {}", e),
        };

        // iterate over county shapefile
        for result in iterator {
            let (shape, record) = match result {
                Ok(x) => x,
                Err(e) => {
                    println!("failed to parse record: {}", e);
                    continue;
                },
            };

            // parse county bounds and centroid
            let multipolygon: MultiPolygon<f64> = shape.into();
            let polygon = multipolygon.into_iter().next().unwrap();
            let point = polygon.centroid().unwrap();

            // parse record metadata
            let id = match record.get("GEOID10") {
                Some(value) => match value {
                    FieldValue::Character(Some(id)) => id.to_string(),
                    x => panic!("unsupported field type: {}", x),
                },
                None => {
                    println!("failed to identify county id");
                    continue;
                },
            };

            counties.insert(id, (point, polygon));
        }
    }

    return;
    
    // open first netcdf file
    let reader = match netcdf::open(&args[2]) {
        Ok(reader) => reader,
        Err(e) => panic!("failed to open netcdf file '{}': {}", &args[2], e),
    };

    // read netcdf dimension values
    let longitudes = match get_values::<f64>(&reader, "lon") {
        Ok(longitudes) => longitudes,
        Err(e) => panic!("failed to get variable values: {}", e),
    };

    let latitudes = match get_values::<f64>(&reader, "lat") {
        Ok(latitudes) => latitudes,
        Err(e) => panic!("failed to get variable values: {}", e),
    };

    // days since 1900
    let _times = match get_values::<f32>(&reader, "time") {
        Ok(times) => times,
        Err(e) => panic!("failed to get variable values: {}", e),
    };

    // label netcdf indices with corresponding county
    let latitude_delta = latitudes[1] - latitudes[0];
    let longitude_delta = longitudes[1] - longitudes[0];

    let mut county_index = Vec::new();
    for _ in 0..longitudes.len() {
        let mut vec = Vec::new();
        for _ in 0..latitudes.len() {
            vec.push("".to_string());
        }

        county_index.push(vec);
    }

    let (index_tx, index_rx):
        (Sender<(usize, usize)>, Receiver<(usize, usize)>) =
            crossbeam_channel::unbounded();
    let (counties, latitudes, longitudes) = 
        (Arc::new(counties), Arc::new(latitudes), Arc::new(longitudes));

    let mut handles = Vec::new();
    for _ in 0..12 {
        let (counties, index_rx, latitudes, longitudes) = 
            (counties.clone(), index_rx.clone(),
                latitudes.clone(), longitudes.clone());

        let handle = std::thread::spawn(move || {
            let mut buffer: Vec<(f64, &str, &Polygon<f64>)> = Vec::new();
            for (i, j) in index_rx.iter() {
                // identify longitude and latitude of index
                let (longitude, latitude) =
                    (longitudes[i] - 360.0, latitudes[j]);
                //let index_point = Point::new(longitude, latitude);
                let index_polygon = Polygon::new(
                    LineString::from(vec![(longitude, latitude),
                        (longitude + longitude_delta, latitude),
                        (longitude + longitude_delta, latitude + latitude_delta), 
                        (longitude, latitude + latitude_delta),
                        (longitude, latitude)]),
                    vec![]);
                let index_point = index_polygon.centroid().unwrap();

                // identify closest counties by centroid
                for (k, (point, polygon)) in counties.iter() {
                    // compute distance
                    let distance = 
                        point.euclidean_distance(&index_point);

                    // identify ordered buffer location
                    let mut index = buffer.len();
                    while index != 0 && distance < buffer[index-1].0 {
                        index -= 1;
                    }

                    // insert into buffer at index
                    if index < CONTAINS_BUFFER_SIZE {
                        buffer.insert(index, (distance, k, polygon));
                    }

                    if buffer.len() > CONTAINS_BUFFER_SIZE {
                        buffer.pop();
                    }
                }

                // compute 'contains'
                for (_, k, polygon) in buffer.iter() {
                    if polygon.intersects(&index_polygon)
                            || index_polygon.contains(*polygon)
                            || polygon.contains(&index_polygon) {
                        println!("{} {} {}", i, j, k);
                    }
                }

                buffer.clear();
            }
        });

        handles.push(handle);
    }

    // send indices down channel
    for i in 0..longitudes.len() {
        for j in 0..latitudes.len() {
            if let Err(e) = index_tx.send((i, j)) {
                panic!("failed to send index: {}", e);
            }
        }
    }

    drop(index_tx);
    for handle in handles {
        if let Err(e) = handle.join() {
            panic!("failed to join handle: {:?}", e);
        }
    }

    /*for variable in reader.variables() {
        println!("{}", variable.name());
        println!("{:?}", variable.vartype());

        let values = variable.values::<f64>(None, None).unwrap();
        println!("{:?}", values.shape());

        for dimension in variable.dimensions() {
            println!("  d:{}", dimension.name());
        }

        for attribute in variable.attributes() {
            println!("  a:{}", attribute.name());
        }
    }*/
}

fn get_values<T: netcdf::Numeric>(reader: &File, name: &str) 
        -> Result<ArrayD<T>, netcdf::error::Error> {
    let variable = match reader.variable(name) {
        Some(variable) => variable,
        None => return Err(format!("variable {} not found", name).into()),
    };

    variable.values::<T>(None, None)
}
