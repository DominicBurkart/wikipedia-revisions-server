use std::thread;
use std::process::Command;
use std::fs::File;
use std::collections::BTreeMap;
use std::io::Write;
use std::cmp;

use csv::Reader;
use brotli;
use serde_json;
use serde_json::json;
use chrono::{DateTime, FixedOffset};

extern crate num_cpus;
extern crate clap;
use clap::{Arg, App};

/// Uses https://github.com/dominicburkart/wikipedia-revisions
fn download_revisions(working_dir: String, storage_dir: String, date: String) {
    fn revisions_csv_to_files(input_path: String, base_dir: String) {
        let mut date_map: BTreeMap<DateTime<FixedOffset>, Vec<u64>> = BTreeMap::new();

        let mut reader = Reader::from_path(input_path).unwrap();
        for result in reader.records() {
            // row content:
            //    [
            //        "id",
            //        "parent_id",
            //        "page_title",
            //        "contributor_id",
            //        "contributor_name",
            //        "contributor_ip",
            //        "timestamp",
            //        "text",
            //        "comment",
            //        "page_id",
            //        "page_ns",
            //    ]
            let record = result.unwrap();

            // write record to file
            let rev_id = u64::from_str_radix(&record[0], 10).unwrap();
            {
                let record_path = format!(
                    "{}/{}/{}/{}",
                    base_dir,
                    rev_id % 10000,
                    rev_id % 100000000,
                    record[0].to_string()
                );
                let record_string = json!({
                    "id": record[0],
                    "parent_id": record[1],
                    "page_title": record[2],
                    "contributor_id": record[3],
                    "contributor_name": record[4],
                    "contributor_ip": record[5],
                    "timestamp": record[6],
                    "text": record[7],
                    "comment": record[8],
                    "page_id": record[9],
                    "page_ns": record[10]
                }).to_string();
                let mut out_file = File::create(&record_path).unwrap();
                let mut writer = brotli::DecompressorWriter::new(&mut out_file, 4096);
                writer.write(record_string.as_bytes());
            }

            // populate date to id map
            let date = DateTime::parse_from_rfc3339(&record[6]).unwrap();
            if date_map.contains_key(&date) {
                date_map.get_mut(&date).unwrap().push(rev_id);
            } else {
                date_map.insert(date, vec![rev_id]);
            }
        }

        // save date to id map
        let date_map_file = File::create(
            format!(
                "{}/date_map.json",
                base_dir
            )
        ).unwrap();
        serde_json::to_writer(
            date_map_file,
            &date_map
        ).unwrap();
    }


    let pipe_name = "/revisions.pipe";

    let downloader_handle = {
        let working_dir = working_dir.clone();
        let pipe_name = pipe_name.clone();
        let num_cores = format!("{}", cmp::max(num_cpus::get() - 1, 2));

        let out = File::create("out.log").unwrap();
        let err = File::create("err.log").unwrap();
        thread::spawn(
            move || {
                let status = Command::new("/src/download")
                    .arg(&working_dir)
                    .arg(&date)
                    .arg(&num_cores)
                    .arg(&pipe_name)
                    .status()
                    .unwrap();
                if !status.success() {
                    panic!("loader failed. Exit code: {:#?}", status.code());
                }
            }
        )
    };

    let processor_handle = {
        let pipe_name = pipe_name.to_string();
        thread::spawn(
            move || {
                revisions_csv_to_files(
                    pipe_name,
                    storage_dir.to_string()
                )
            }
        )
    };

    downloader_handle.join().unwrap();
    processor_handle.join().unwrap();
    if !Command::new("sh")
        .arg("-c")
        .arg(
           format!(
               "rm {}",
               pipe_name
           )
        )
        .status()
        .unwrap()
        .success() {
            panic!("named pipe could not be deleted");
    }
}

fn main() {
    let matches = App::new("Wikipedia Revisions Server")
        .version("0.1")
        .author("Dominic <@DominicBurkart>")
        .about("Serves new & updated wikipedia article fragments within a set time period.")
        .arg(Arg::with_name("date")
           .short("d")
           .long("date")
           .value_name("DATE")
           .help("download revisions from the wikidump on this date. Format YYYYMMDD (e.g. 20201201). If not passed, local revisions are used.")
           .takes_value(true))
      .get_matches();

    // if we have a passed date, download the revisions
    match matches.value_of("date") {
        Some(date) => download_revisions("working_dir".to_string(), "storage_dir".to_string(), date.to_string()),
        None => (),
    }

    // start server
    println!("nice");
}