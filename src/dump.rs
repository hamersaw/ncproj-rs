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
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(StructOpt)]
pub struct Dump {
    #[structopt(parse(from_os_str), index = 2)]
    data_files: Vec<PathBuf>,

    #[structopt(parse(from_os_str), index = 1)]
    index_file: PathBuf,

    #[structopt(short = "t", long = "thread-count", default_value = "8")]
    thread_count: u8,

    // number of time intervals to include
    //  larger is faster but uses more memory
    #[structopt(short = "b", long = "buffer-size", default_value = "250")]
    buffer_size: usize,
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
        let (times, latitudes_len, longitudes_len) = {
            let reader = netcdf::open(&self.data_files[0])?;
            let times = crate::get_netcdf_values::<i64>(&reader, "time")?;

            let datetime = Utc.ymd(1900, 1, 1).and_hms(0, 0, 0);
            let times: Vec<i64> = times.iter().map(
                    |x| (datetime + Duration::days(*x)).timestamp()
                ).collect();

            let latitudes = 
                crate::get_netcdf_values::<f64>(&reader, "lat")?;
            let longitudes = 
                crate::get_netcdf_values::<f64>(&reader, "lon")?;

            (times, latitudes.len(), longitudes.len())
        };

        // parse data
        let mut features: Vec<Vec<String>> = Vec::new();
        let buffers: Arc<RwLock<Vec<Vec<f32>>>> =
            Arc::new(RwLock::new(Vec::new()));
        let mut fill_values: Vec<f32> = Vec::new();

        for data_file in self.data_files.iter() {
            // open data file
            let reader = netcdf::open(data_file)?;

            // compile set of dimension names
            let mut dimensions = HashSet::new();
            for dimension in reader.dimensions() {
                dimensions.insert(dimension.name());
            }

            // iterate over variables
            let mut file_features = Vec::new();
            for variable in reader.variables() {
                // skip dimension variables 
                if dimensions.contains(&variable.name()) {
                    continue;
                }

                // add feature to features
                file_features.push(variable.name());

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

                // add buffer to buffers
                let mut buffers = buffers.write().unwrap();
                buffers.push(
                    vec![0f32; self.buffer_size * latitudes_len * longitudes_len]
                );
            }

            features.push(file_features);
        }

        // print csv header
        print!("gis_join,timestamp");
        for file_features in features.iter() {
            for feature in file_features.iter() {
                print!(",min_{},max_{}", feature, feature);
            }
        }
        println!();

        // initailize thread channels
        let (index_tx, index_rx): (Sender<(usize, usize)>,
            Receiver<(usize, usize)>) = crossbeam_channel::unbounded();
        let (data_tx, data_rx): (Sender<(usize, usize, Vec<f32>)>,
            Receiver<(usize, usize, Vec<f32>)>) = crossbeam_channel::unbounded();

        // initialize print thread
        let completed_count = Arc::new(AtomicUsize::new(0));
        let time_index_offset = Arc::new(AtomicUsize::new(0));

        let handle = {
            let (completed_count, time_index_offset) =
                (completed_count.clone(), time_index_offset.clone());  

            let (shapes, times) = (shapes.clone(), times.clone());  
            std::thread::spawn(move || {
                for (i, j, data) in data_rx.iter() {
                    let time_index_offset = time_index_offset
                        .load(Ordering::Relaxed);

                    print!("{},{}", shapes[j].0,
                        times[time_index_offset + i]);

                    for k in 0..data.len() {
                        print!(",{:.3}", data[k]);
                    }
                    println!("");

                    completed_count.fetch_add(1, Ordering::SeqCst);
                }
            })
        };

        // start worker threads
        let (fill_values, shapes) =
            (Arc::new(fill_values), Arc::new(shapes.clone()));

        let mut worker_handles = Vec::new();
        for _ in 0..self.thread_count {
            let (latitudes_len, longitudes_len) =
                (latitudes_len.clone(), longitudes_len.clone());

            let (buffers, data_tx, fill_values, index_rx, shapes) =
                (buffers.clone(), data_tx.clone(), fill_values.clone(), 
                    index_rx.clone(), shapes.clone());

            let handle = std::thread::spawn(move || {
                // compute feature values for each shape
                for (i, j) in index_rx.iter() {
                    let mut data = Vec::new();

                    // get shape indices - <x, y> coordinates in file
                    let (shape_id, indices) = &shapes[j];

                    let buffers = buffers.read().unwrap();
                    for k in 0..buffers.len() {
                        let buffer = &buffers[k];
                        let fill_value = fill_values[k];

                        let (mut min, mut max) = (f32::MAX, f32::MIN);
                        for (x, y) in indices.iter() {
                            let buffer_index = 
                                i * (latitudes_len * longitudes_len) 
                                + y * longitudes_len + x;

                            let value = buffer[buffer_index];
                            if value == fill_value {
                                continue;
                            }
                            
                            if value < min {
                                min = value;
                            }

                            if value > max {
                                max = value;
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

        // iterate over time values
        let mut count = 0;
        let sleep_duration = std::time::Duration::from_millis(50);
        for i in (0..times.len()).step_by(self.buffer_size) {
            time_index_offset.store(i, Ordering::SeqCst);

            let time_slice_len =
                std::cmp::min(self.buffer_size, times.len() - i);

            let slice_len = [time_slice_len,
                latitudes_len, longitudes_len];

            // read data into buffers
            let mut buffer_index = 0;
            for (j, data_file) in self.data_files.iter().enumerate() {
                // open data file
                let reader = netcdf::open(data_file)?;

                // iterate over identified variables
                for feature in features[j].iter() {
                    let variable = reader.variable(feature).unwrap();

                    // copy variable to buffer
                    let buffer_size = time_slice_len 
                        * latitudes_len * longitudes_len;
                    let mut buffers = buffers.write().unwrap();

                    variable.values_to(
                        &mut buffers[buffer_index][..buffer_size],
                        Some(&[i, 0, 0]), Some(&slice_len))?;

                    buffer_index += 1;
                }
            }

            // send indices down channel
            count += time_slice_len * shapes.len();
            for j in 0..time_slice_len {
                for k in 0..shapes.len() {
                    index_tx.send((j, k))?;
                }
            }

            // wait for all indices to be computed
            while completed_count.load(Ordering::SeqCst) != count {
                std::thread::sleep(sleep_duration);
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
