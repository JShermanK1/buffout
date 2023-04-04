
use std::fs::File;
use std::path::PathBuf;
use std::process::{self, Stdio};
use std::io::{BufReader, 
              BufWriter,
              Write,
              BufRead,
              self};
use std::error::Error;
use std::boxed::Box;
use clap::{arg, Command, value_parser};
use gzp::{par::compress::{ParCompress, ParCompressBuilder},
         deflate::Gzip, 
         ZWriter, 
         Compression};

fn cli() -> Command {
    Command::new("buffout")
            .args(&[
                arg!(
                    -c --cmd <STRING> "Command to start as sub process and then buffer the stdout"
                ).required(false),
                arg!(
                    -o --out <FILE> "Path to file for buffered output"
                ).required(true)
                 .value_parser(value_parser!(PathBuf)),
                arg!(
                    -z --gzip <COMPRESSION_LEVEL> "Compression level to pass to gzip. Accepts values 1(fastest)-9(most compressed). Requires equals"
                ).value_parser(1..=9)
                 .default_missing_value("6")
                 .require_equals(true)
                 .num_args(0..=1),
            ])
}

fn main() -> Result<(), Box<dyn Error>> {

    let matches = cli().get_matches();

    let out_f = matches.get_one::<PathBuf>("out")
                    .expect("error parsing out");
    let out_f = File::create(out_f).expect("Error parsing out file path");

    let reader: Box<dyn BufRead>;
    let mut child_p = None;
    if let Some(args) = matches.get_one::<String>("cmd") {

        let mut args = args.split_whitespace();
        let cmd =  args.next().expect("Command flag used but no command present");   

        let mut sp = process::Command::new(cmd)
                        .args(args)
                        .stdout(Stdio::piped())
                        .spawn().expect("failed to execute command");

        reader = Box::new(BufReader::new(
                            sp.stdout.take()
                                     .expect("failed to capture process stdout")
                        ));

        child_p = Some(sp);

    } else {
        reader = Box::new(io::stdin().lock());
    }

    let mut writer = BufWriter::with_capacity(1024 * 1024 * 100, out_f);
    if let Some(level) = matches.get_one::<i64>("gzip") {

        let mut e_writer: ParCompress<Gzip> = ParCompressBuilder::new()
                                                .num_threads(4).unwrap()
                                                //parser should restrict to 1-9
                                                .compression_level(Compression::new((*level).try_into().unwrap()))
                                                .from_writer(writer); 

        reader.split(b'\n').for_each(|l| {
            let mut l = l.unwrap();
            l.push(b'\n'); 
            e_writer.write_all(&l).unwrap();
        });
        e_writer.finish().unwrap();

    } else {
        reader.split(b'\n').for_each(|l| {
            let mut l = l.unwrap();
            l.push(b'\n'); 
            writer.write_all(&l).unwrap();
        });
        writer.flush().unwrap();
    }
    
    if let Some(mut process) = child_p {
        let exit_stat = process.wait()
                               .expect("error waiting on process"); 
        assert!(exit_stat.success());
    }
                             

    Ok(())
}