use chrono::Duration;
use chrono::prelude::{TimeZone, Utc};
use crossbeam_channel::{Receiver, Sender};
use netcdf::attribute::AttrValue;
use structopt::StructOpt;

use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(StructOpt)]
pub struct Dump {
    #[structopt(parse(from_os_str), index = 2)]
    data_files: Vec<PathBuf>,

    #[structopt(parse(from_os_str), index = 1)]
    index_file: PathBuf,

    #[structopt(short = "t", long = "thread-count", default_value = "8")]
    thread_count: u8,
}

impl Dump {
    pub fn execute(&self) -> Result<(), Box<dyn Error>> {
        // read shape indices from file
        let mut shapes = BTreeMap::new();

        {
            // open index file
            let file = File::open(&self.index_file)?;
            let buf_reader = BufReader::new(file);

            // iterate over index entries
            for result in buf_reader.lines() {
                let line = result?;
                let fields: Vec<&str> = line.split(" ").collect();

                let x = fields[0].parse::<usize>()?;
                let y = fields[1].parse::<usize>()?;

                // add index to shapes map
                let indices = shapes.entry(fields[2].to_string())
                    .or_insert(Vec::new());
                indices.push((x, y));
            }
        }

        let shapes: Vec<(String, Vec<(usize, usize)>)> =
            shapes.into_iter().collect();

        // parse times
        let times: Vec<i64> = {
            let reader = netcdf::open(&self.data_files[0])?;
            let times = crate::get_netcdf_values::<i64>(&reader, "time")?;

            let datetime = Utc.ymd(1900, 1, 1).and_hms(0, 0, 0);
            times.iter().map(
                    |x| (datetime + Duration::days(*x)).timestamp()
                ).collect()
        };

        // parse data
        let (mut ndarrays, mut fill_values, mut features) =
            (Vec::new(), Vec::new(), Vec::new());

        for data_file in self.data_files.iter() {
            // open data file
            let reader = netcdf::open(data_file)?;

            // compile set of dimension names
            let mut dimensions = HashSet::new();
            for dimension in reader.dimensions() {
                dimensions.insert(dimension.name());
            }

            // iterate over variables
            for variable in reader.variables() {
                // skip dimension variables 
                if dimensions.contains(&variable.name()) {
                    continue;
                }
 
                // append feature to features vector
                let name = match variable.attribute("long_name") {
                    Some(attribute) => match attribute.value()? {
                        AttrValue::Str(value) => value.to_string(),
                        x => return Err(format!(
                            "unsupported name type '{:?}'", x).into()),
                    },
                    None => return Err("variable name not found".into()),
                };

                features.push(format!("Minimum {}", name));
                features.push(format!("Maximum {}", name));

                // parse values
                let ndarray = crate::get_netcdf_values
                    ::<f32>(&reader, &variable.name())?;
                ndarrays.push(ndarray);

                // parse fill value
                let fill_value = match variable.attribute("_FillValue") {
                    Some(attribute) => match attribute.value()? {
                        AttrValue::Float(value) => value as f32,
                        AttrValue::Ushort(value) => value as f32,
                        x => return Err(format!(
                            "unsupported fill value type '{:?}'", x).into()),
                    },
                    None => return Err("fill value not found".into()),
                };

                fill_values.push(fill_value);
            }
        }

        let (fill_values, ndarrays, shapes) =
            (Arc::new(fill_values), Arc::new(ndarrays), Arc::new(shapes));

        // initailize thread channels
        let (index_tx, index_rx): (Sender<(usize, usize)>,
            Receiver<(usize, usize)>) = crossbeam_channel::unbounded();
        let (data_tx, data_rx): (Sender<(usize, usize, Vec<f32>)>,
            Receiver<(usize, usize, Vec<f32>)>) =
                crossbeam_channel::unbounded();

        // initialize worker threads
        let mut worker_handles = Vec::new();
        for _ in 0..self.thread_count {
            let (data_tx, fill_values, index_rx, ndarrays, shapes) =
                (data_tx.clone(), fill_values.clone(), index_rx.clone(),
                    ndarrays.clone(), shapes.clone());

            let handle = std::thread::spawn(move || {
                // compute feature values for each <time, shape> pair
                for (i, j) in index_rx.iter() {
                    let mut data = Vec::new();

                    let (_, indices) = &shapes[j];
                    for k in 0..ndarrays.len() {
                        let ndarray = &ndarrays[k];
                        let fill_value = fill_values[k];

                        let (mut min, mut max) = (f32::MAX, f32::MIN);
                        for (x, y) in indices.iter() {
                            let value = ndarray.get([i, *y, *x]).unwrap();
                            if value == &fill_value {
                                continue;
                            }
                            
                            if value < &min {
                                min = *value;
                            }

                            if value > &max {
                                max = *value;
                            }
                        }

                        data.push(min);
                        data.push(max);
                    }

                    if let Err(e) = data_tx.send((i, j, data)) {
                        println!("failed to write data: {}", e);
                    }
                }
            });

            worker_handles.push(handle);
        }
 
        // initialize print threads
        let handle = {
            let (shapes, times) = (shapes.clone(), times.clone());  
            std::thread::spawn(move || {
                print!("time,shapeid");
                for feature in features {
                    print!(",{}", feature.to_lowercase());
                }
                println!("");

                for (i, j, data) in data_rx.iter() {
                    print!("{},{}", times[i], shapes[j].0);
                    for k in 0..data.len() {
                        print!(",{:.2}", data[k]);
                    }
                    println!("");
                }
            })
        };

        // send indices down channel
        for i in 0..times.len() {
            for j in 0..shapes.len() {
                index_tx.send((i, j))?;
            }
        }

        // wait until all threads have finished
        drop(index_tx);
        for handle in worker_handles {
            if let Err(e) = handle.join() {
                return Err(format!("failed to join handle: {:?}", e).into());
            }
        }

        drop(data_tx);
        if let Err(e) = handle.join() {
            return Err(format!("failed to join handle: {:?}", e).into());
        }

        Ok(())
    }
}
