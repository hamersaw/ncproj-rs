use structopt::StructOpt;

mod index;

#[derive(StructOpt)]
struct Opt {
    #[structopt(subcommand)]
    cmd: Command
}

#[derive(StructOpt)]
enum Command {
    Index(index::Index),
}

fn main() {
    // parse options
    let opt = Opt::from_args();

    // execute subcommand
    let result = match opt.cmd {
        Command::Index(index) => index.execute(),
    };

    // process result
    if let Err(e) = result {
        panic!("{}", e);
    }
}
