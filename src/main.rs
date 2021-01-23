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
    let opt = Opt::from_args();

    let result = match opt.cmd {
        Command::Index(index) => index.execute(),
    };

    if let Err(e) = result {
        panic!("{}", e);
    }
}
