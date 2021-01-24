use ndarray::ArrayD;
use netcdf::File;
use structopt::StructOpt;

mod dump;
mod index;

#[derive(StructOpt)]
struct Opt {
    #[structopt(subcommand)]
    cmd: Command
}

#[derive(StructOpt)]
enum Command {
    Dump(dump::Dump),
    Index(index::Index),
}

fn main() {
    // parse options
    let opt = Opt::from_args();

    // execute subcommand
    let result = match opt.cmd {
        Command::Dump(dump) => dump.execute(),
        Command::Index(index) => index.execute(),
    };

    // process result
    if let Err(e) = result {
        panic!("{}", e);
    }
}

fn get_netcdf_values<T: netcdf::Numeric>(reader: &File, name: &str) 
        -> Result<ArrayD<T>, netcdf::error::Error> {
    let variable = match reader.variable(name) {
        Some(variable) => variable,
        None => return Err(format!("variable {} not found", name).into()),
    };

    variable.values::<T>(None, None)
}
