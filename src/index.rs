use crossbeam_channel::{Receiver, Sender};
use dbase::FieldValue;
use geo::algorithm::centroid::Centroid;
use geo::algorithm::contains::Contains;
use geo::algorithm::euclidean_distance::EuclideanDistance;
use geo::algorithm::intersects::Intersects;
use geo_types::{LineString, MultiPolygon, Point, Polygon};
use shapefile::Reader;
use structopt::StructOpt;

use std::collections::BTreeMap;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(StructOpt)]
pub struct Index {
    #[structopt(short = "b", long = "buffer-size", default_value = "5")]
    buffer_size: usize,

    #[structopt(parse(from_os_str), index = 2)]
    grid_file: PathBuf,

    #[structopt(parse(from_os_str), index = 1)]
    shape_file: PathBuf,

    #[structopt(short = "t", long = "thread-count", default_value = "8")]
    thread_count: u8,
}

impl Index {
    pub fn execute(&self) -> Result<(), Box<dyn Error>> {
        // populate shapes map
        let mut shapes: BTreeMap<String, (Point<f64>, Polygon<f64>)> =
            BTreeMap::new();

        {
            // open shapefile reader and iterator
            let reader = Reader::from_path(&self.shape_file)?;
            let iterator = reader.iter_shapes_and_records_as
                    ::<shapefile::Polygon>()?;

            // iterate over shapefile
            for result in iterator {
                let (shape, record) = result?;

                // parse shape bounds and centroid
                let multipolygon: MultiPolygon<f64> = shape.into();
                let polygon = multipolygon.into_iter().next().unwrap();
                let point = polygon.centroid().unwrap();

                // parse record metadata
                let id = match record.get("GEOID10") {
                    Some(value) => match value {
                        FieldValue::Character(Some(id)) => id.to_string(),
                        x => return Err(format!(
                            "unsupported field type: {}", x).into()),
                    },
                    None => return Err("failed to identify shape id".into()),
                };

                shapes.insert(id, (point, polygon));
            }
        }
        
        // open netcdf grid_file
        let reader = netcdf::open(&self.grid_file)?;

        // read netcdf dimension values
        let longitudes = crate::get_netcdf_values::<f64>(&reader, "lon")?;
        let latitudes = crate::get_netcdf_values::<f64>(&reader, "lat")?;

        // label netcdf indices with corresponding shape
        let latitude_delta = latitudes[1] - latitudes[0];
        let longitude_delta = longitudes[1] - longitudes[0];

        let mut shape_index = Vec::new();
        for _ in 0..longitudes.len() {
            let mut vec = Vec::new();
            for _ in 0..latitudes.len() {
                vec.push("".to_string());
            }

            shape_index.push(vec);
        }

        let (index_tx, index_rx):
            (Sender<(usize, usize)>, Receiver<(usize, usize)>) =
                crossbeam_channel::unbounded();
        let (latitudes, longitudes, shapes) = 
            (Arc::new(latitudes), Arc::new(longitudes), Arc::new(shapes));

        let mut handles = Vec::new();
        for _ in 0..self.thread_count {
            let (buffer_size, index_rx, latitudes, longitudes, shapes) =
                (self.buffer_size.clone(), index_rx.clone(),
                    latitudes.clone(), longitudes.clone(), shapes.clone());

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
                            (longitude + longitude_delta,
                                latitude + latitude_delta), 
                            (longitude, latitude + latitude_delta),
                            (longitude, latitude)]),
                        vec![]);
                    let index_point = index_polygon.centroid().unwrap();

                    // identify closest shape by centroid
                    for (k, (point, polygon)) in shapes.iter() {
                        // compute distance
                        let distance = 
                            point.euclidean_distance(&index_point);

                        // identify ordered buffer location
                        let mut index = buffer.len();
                        while index != 0 && distance < buffer[index-1].0 {
                            index -= 1;
                        }

                        // insert into buffer at index
                        if index < buffer_size {
                            buffer.insert(index, (distance, k, polygon));
                        }

                        if buffer.len() > buffer_size {
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
                index_tx.send((i, j))?;
            }
        }

        // wait until all threads have finished
        drop(index_tx);
        for handle in handles {
            if let Err(e) = handle.join() {
                return Err(format!("failed to join handle: {:?}", e).into());
            }
        }

        Ok(())
    }
}
