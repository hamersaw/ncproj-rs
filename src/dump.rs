use netcdf::attribute::AttrValue;
use structopt::StructOpt;

use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

#[derive(StructOpt)]
pub struct Dump {
    #[structopt(parse(from_os_str), index = 2)]
    data_files: Vec<PathBuf>,

    #[structopt(parse(from_os_str), index = 1)]
    index_file: PathBuf,
}

impl Dump {
    pub fn execute(&self) -> Result<(), Box<dyn Error>> {
        // read county indices from file
        let mut counties = BTreeMap::new();

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

                // add index to counties map
                let indices = counties.entry(fields[2].to_string())
                    .or_insert(Vec::new());
                indices.push((x, y));
            }
        }

        // parse data times
        let times = {
            let reader = netcdf::open(&self.data_files[0])?;
            crate::get_netcdf_values::<f32>(&reader, "time")?
        };

        // identify feature values
        let mut features = Vec::new();
        let mut data = Vec::new();

        for i in 0..times.len() { 
            data.push(Vec::new());
            for _ in 0..counties.len() {
                data[i].push(Vec::new());
            }
        }

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

                // parse variables values and fill value
                let values = crate::get_netcdf_values
                    ::<f32>(&reader, &variable.name())?;

                let fill_value = match variable.attribute("_FillValue") {
                    Some(attribute) => match attribute.value()? {
                        AttrValue::Float(value) => value as f32,
                        AttrValue::Ushort(value) => value as f32,
                        x => return Err(format!(
                            "unsupported fill value type '{:?}'", x).into()),
                    },
                    None => return Err("fill value not found".into()),
                };

                // iterate over time
                for i in 0..data.len() { 
                    // compute minimum and maximum value for each shape
                    for (j, (_, indices)) in counties.iter().enumerate() {
                        let (mut min, mut max) = (f32::MAX, f32::MIN);
                        for (x, y) in indices.iter() {
                            let value = values.get([i, *y, *x]).unwrap();
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

                        //println!("{},{},{},{}", id, i, min, max);
                        data[i][j].push(min);
                        data[i][j].push(max);
                    }
                }
            }
        }

        // print out results
        print!("time,shapeid");
        for feature in features {
            print!(",{}", feature.to_lowercase());
        }
        println!("");

        for i in 0..data.len() {
            for (j, (id, _)) in counties.iter().enumerate() {
                print!("{},{}", times[i], id);        

                for k in 0..data[i][j].len() {
                    print!(",{:.2}", data[i][j][k]);
                }
                println!("");
            }
        }

        Ok(())
    }
}
