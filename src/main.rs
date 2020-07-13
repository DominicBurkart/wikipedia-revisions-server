use std::thread;
use std::process::Command;
use std::fs::{File, create_dir_all, remove_file, read_dir};
use std::collections::BTreeMap;
use std::io::{Write, BufWriter, BufReader};
use std::cmp;
use std::collections::HashMap;
use std::time::Duration;

use csv::Reader;
use serde_json::json;
use chrono::{DateTime, FixedOffset};
use brotli2::write;
extern crate num_cpus;
extern crate clap;
use clap::{Arg, App};
use rayon::iter::ParallelBridge;
use rayon::prelude::ParallelIterator;
use regex::Regex;
use crossbeam::crossbeam_channel::{Receiver, Select, unbounded};


const BROTLI_COMPRESSION_LEVEL: u32 = 9;

fn revisions_csv_to_files<'a>(input_path: &'a str, base_dir: &'a str) {
    let f = File::open(&input_path).unwrap();
    let buf = BufReader::new(f);
    let reader = Reader::from_reader(buf);
    let mut date_vec: Vec<(DateTime<FixedOffset>, u64)> = reader
        .into_records()
        .filter_map(Result::ok)
        .par_bridge()
        .map(
            |record| {
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
                // write record to file
                let rev_id = u64::from_str_radix(&record[0], 10).unwrap();
                {
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
                    let record_dir = format!(
                        "/{}/{}/{}",
                        base_dir,
                        rev_id % 10000,
                        rev_id % 100000000,
                    );
                    let record_path = format!(
                        "{}/{}",
                        record_dir,
                        record[0].to_string()
                    );
                    create_dir_all(record_dir).unwrap();
                    let out_file = File::create(&record_path).unwrap();
                    let buf = BufWriter::new(out_file);
                    let mut writer = write::BrotliEncoder::new(buf, BROTLI_COMPRESSION_LEVEL);
                    writer.write_all(record_string.as_bytes()).unwrap();
                }

                // populate date to id map
                let date = DateTime::parse_from_rfc3339(&record[6]).unwrap();
                (date, rev_id)
        }
    )
    .collect();

    println!("pipe read completed. saving date index...");

    let mut date_map: BTreeMap<DateTime<FixedOffset>, Vec<u64>> = Default::default();
    date_vec
        .drain(..)
        .for_each(
            |(date, rev_id)|
                date_map
                    .entry(date)
                    .or_insert_with(Vec::new)
                    .push(rev_id)
        );

    // save date to id map
    // todo should we mutex this into a single thread for combination? or send it via messages?
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

    println!("date index complete");
}

fn monitor_input_pipes(
    storage_dir: String,
    downloader_receiver: Receiver<bool>
) {
    let mut pending_receivers = HashMap::new();
    pending_receivers.insert("_".to_string(), downloader_receiver);
    let re = Regex::new(r"revisions-\d+-\d+\.pipe").unwrap();
    let mut complete = false;
    let mut one_found = false;
    while !complete {
        for entry in read_dir("/").unwrap() {
            let name = entry.unwrap().file_name().into_string().unwrap();
            let storage_dir = storage_dir.clone();
            if re.is_match(&name) && !pending_receivers.contains_key(&name) {
                let (tx, rx) = unbounded();
                {
                    let name = name.clone();
                    thread::spawn(
                        move || {
                            revisions_csv_to_files(
                                &name,
                                &storage_dir
                            );
                            remove_file(&name).unwrap();
                            tx.send(true).unwrap();
                        }
                    );
                }
                pending_receivers.insert(name, rx);
                one_found = true;
            }
        }

        if !one_found {
            println!("[monitor_input_pipes] awaiting revision pipes...");
            thread::sleep(Duration::from_secs(60))
        } else if pending_receivers.is_empty() {
            complete = true;
        } else {
            let mut select = Select::new();
            for receiver in pending_receivers.values() {
                select.recv(receiver);
            }
            select.ready();

            pending_receivers
                .retain(
                    |_, receiver|
                        match receiver.try_recv() {
                            Ok(_) => false,
                            _ => true
                        }
                );
        }
    }
}

/// Wraps https://github.com/dominicburkart/wikipedia-revisions
fn download_revisions(working_dir: String, storage_dir: String, date: String) {
    let downloader_receiver = {
        let num_cores = format!("{}", cmp::max(num_cpus::get() - 1, 3));
        let (tx, rx) = unbounded();

        thread::spawn(
            move || {
                println!("starting downloader program...");
                let status = Command::new("/src/download")
                    .arg(&working_dir)
                    .arg(&date)
                    .arg(&num_cores)
                    .status()
                    .unwrap();
                if !status.success() {
                    match status.code() {
                        Some(n) => panic!("loader failed. Exit code: {}", n),
                        None => panic!("loader failed. No exit code collected.")
                    }
                }
                tx.send(true).unwrap();
            }
        );
        rx
    };

    monitor_input_pipes(storage_dir, downloader_receiver);
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
    if let Some(date) = matches.value_of("date") {
        download_revisions(
            "working_dir".to_string(),
            "storage_dir".to_string(),
            date.to_string()
        );
    }

    // start server
    unimplemented!();
}