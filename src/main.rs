
use std::fs::File;
use std::path::PathBuf;
use std::process::{self, Stdio};
use std::io::{BufReader, 
              BufWriter,
              Write,
              BufRead};
use std::error::Error;
use std::boxed::Box;
use clap::{Arg, Command};
use gzp::{par::compress::{ParCompress, ParCompressBuilder},
         deflate::Gzip, ZWriter, Compression};

fn cli() -> Command<'static> {
    Command::new("buffout")
            .args(&[
                Arg::new("cmd")
                    .long("cmd")
                    .short('c')
                    .required(true)
                    .takes_value(true)
                    .value_name("STRING"),
                Arg::new("out")
                    .long("out")
                    .short('o')
                    .required(true)
                    .takes_value(true)
                    .value_name("FILE"),
                Arg::new("gzip")
                    .long("gzip")
                    .short('z')
                    .takes_value(true)
                    .default_missing_value("6")
            ])
}

fn main() -> Result<(), Box<dyn Error>> {

    let matches = cli().get_matches();

    let cmd = matches.value_of("cmd")
                    .expect("error parsing cmd")
                    .split_whitespace()
                    .collect::<Vec<&str>>();

    let out_f = matches.value_of("out")
                    .expect("error parsing out");
    
    let bed_f = File::create(PathBuf::from(out_f)).expect("Error parsing out file path");
    let mut bed_child = process::Command::new(cmd[0])
                            .args(cmd[1..].into_iter())
                            .stdout(Stdio::piped())
                            .spawn().expect("failed to execute command");
    let reader = BufReader::new(bed_child.stdout
                                .take()
                                .expect("failed to capture process stdout"));
    
    let mut writer = BufWriter::with_capacity(1024 * 1024 * 100, bed_f);
    if matches.is_present("gzip") {
        let level = matches.value_of("gzip")
                           .map(|v| v.parse::<u32>().unwrap())
                           .expect("error in gzip value");
        let mut e_writer: ParCompress<Gzip> = ParCompressBuilder::new()
                                                    .num_threads(4).unwrap()
                                                    .compression_level(Compression::new(level))
                                                    .from_writer(writer); 

        reader.lines().for_each(|l| e_writer.write_all((l.unwrap() + "\n").as_bytes()).unwrap());
        e_writer.finish().unwrap();
    } else {
        reader.lines().for_each(|l| writer.write_all((l.unwrap() + "\n").as_bytes()).unwrap());
        writer.flush().unwrap();
    }
    let exit_stat = bed_child.wait()
                             .expect("error waiting on process");

    assert!(exit_stat.success());

    Ok(())
}