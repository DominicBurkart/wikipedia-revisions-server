use std::thread;
use std::process::Command;
use std::fs::{File, remove_file, read_dir};
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write, BufReader, BufWriter, SeekFrom, Seek};
use std::cmp;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::default::Default;
use std::convert::TryFrom;

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
use crossbeam::crossbeam_channel::{Receiver, Select, bounded};
use serde::{Serialize, Deserialize};
use actix_web::{get, web, App as ActixApp, HttpServer, HttpResponse, Responder, middleware};
use futures::stream::{self, Stream};
use bytes::Bytes;
#[macro_use]
extern crate lazy_static;

const BROTLI_COMPRESSION_LEVEL: u32 = 9;
const WORKING_DIR: &str = "/working_dir";
const STORAGE_DIR: &str = "/storage_dir";
const DATES_TO_IDS_FILE: &str = "/storage_dir/date_map.json";
const IDS_TO_POSITIONS_FILE: &str = "/storage_dir/id_map.json";
const N_REVISION_FILES: u64 = 200; // note: changing this field requires rebuilding files
// ^ must be less than max usize.

type RevisionID = u64;
type ContributorID = u64;
type PageID = u64;
type Instant = DateTime<FixedOffset>;
type Offset = u64;
type RecordLength = u64;
type Position = (Offset, RecordLength); // position of record in corresponding file
type DatesToIds = BTreeMap<Instant, Vec<RevisionID>>;
type IdsToPositions = HashMap<RevisionID, Position>;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
struct Revision {
    id: RevisionID,
    parent_id: Option<RevisionID>,
    page_title: String,
    contributor_id: Option<ContributorID>,
    contributor_name: Option<String>,
    contributor_ip: Option<String>,
    timestamp: String,
    text: Option<String>,
    comment: Option<String>,
    page_id: PageID,
    page_ns: u32
}

pub struct State {
    dates_to_ids: Arc<DatesToIds>,
    ids_to_positions: Arc<IdsToPositions>
}

impl State {
    fn revision_ids_from_period<'a, 'b>(&'a self, start: &'b Instant, end: &'b Instant) -> impl Iterator<Item=RevisionID> + 'a {
        self
            .dates_to_ids
            .range(start..end)
            .map(|(_date, ids)| ids)
            .flatten()
            .copied()
    }

    fn revisions_from_period<'a, 'b>(&'a self, start: &'b Instant, end: &'b Instant) -> impl Iterator<Item=Revision> + 'a {
        self
            .revision_ids_from_period(start, end)
            .map(move |id| self.get_revision(id))
    }

    fn diffs_for_period<'a, 'b>(&'a self, start: &'b Instant, end: &'b Instant) -> impl Iterator<Item=Vec<String>> + 'a {
        self
            .revision_ids_from_period(start, end)
            .map(move |id| self.get_new_or_modified_fragments(id))
    }

    fn get_revision(&self, id: RevisionID) -> Revision {
        // todo should return a Result, but I wasn't sure what the error type should be
        let uncompressed_serialized_revision = {
            let compressed_revision = {
                let mut file = File::open(
                    &path_from_revision_id(id)
                ).unwrap();

                let (offset, length) = self.ids_to_positions.get(&id).unwrap();
                let mut compressed_revision = vec![0; usize::try_from(*length).unwrap()];
                file.seek(SeekFrom::Start(*offset)).unwrap();
                file.read_exact(&mut compressed_revision).unwrap();
                compressed_revision
            };

            let mut decompressor = brotli2::read::BrotliDecoder::new(&*compressed_revision);
            let mut uncompressed_file_contents = String::new();
            decompressor.read_to_string(&mut uncompressed_file_contents).unwrap();
            uncompressed_file_contents
        };
        serde_json::from_str(&uncompressed_serialized_revision).unwrap()
    }

    fn diff<'a>(&self, _old: &'a str, _new: &'a str) -> Vec<String> {
        unimplemented!();
    }

    fn get_new_or_modified_fragments(&self, id: RevisionID) -> Vec<String> {
        let revision = self.get_revision(id);
        match revision.text {
            Some(revision_text) => {
                if let Some(parent_id) = revision.parent_id {
                    let parent = self.get_revision(parent_id);
                    return self.diff(&parent.text.unwrap_or_default(), &revision_text);
                }
                self.diff("", &revision_text)
            },
            None => {
                if let Some(parent_id) = revision.parent_id {
                    let parent = self.get_revision(parent_id);
                    return self.diff(&parent.comment.unwrap_or_default(), &revision.comment.unwrap())
                }
                self.diff("", &revision.comment.unwrap())
            }
        }
    }
}

lazy_static! {
    pub static ref STATE: State = {
        println!("loading state...");
        let serialized_date_map = File::open(DATES_TO_IDS_FILE).unwrap();
        let serialized_id_map = File::open(IDS_TO_POSITIONS_FILE).unwrap();
        State {
            dates_to_ids: Arc::new(
                serde_json::from_reader(serialized_date_map).unwrap()
            ),
            ids_to_positions: Arc::new(
                serde_json::from_reader(serialized_id_map).unwrap()
            )
        }
    };
}

struct WriteCounter {
    writer: BufWriter<File>,
    size: u64
}

impl WriteCounter {
    fn write<'a>(&mut self, bytes: &'a [u8]) {
        self.writer.write_all(bytes).unwrap();
        self.size += bytes.len() as u64;
    }

    fn flush(&mut self) {
        self.writer.flush().unwrap();
    }
}

fn path_from_revision_id(id: RevisionID) -> String {
    format!(
        "{}/{}",
        STORAGE_DIR,
        id % N_REVISION_FILES
    )
}

fn revisions_csv_to_files<'a>(
    input_path: &'a str,
    dates_to_ids: Arc<Mutex<DatesToIds>>,
    ids_to_positions: Arc<Mutex<IdsToPositions>>,
    writer_locks: Arc<Vec<Mutex<WriteCounter>>>
) {
    let mut records_vec: Vec<(Instant, u64, Position)> = {
        let f = File::open(&input_path).unwrap();
        let buf = BufReader::new(f);
        let reader = Reader::from_reader(buf);

        reader
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
                    let revision_id = RevisionID::from_str_radix(&record[0], 10).unwrap();
                    let (record_start, record_length) = {
                        // compress the revision
                        let compressed_bytes = {
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

                            let mut v = Vec::new();
                            {
                                let mut writer = write::BrotliEncoder::new(&mut v, BROTLI_COMPRESSION_LEVEL);
                                writer.write_all(record_string.as_bytes()).unwrap();
                            }
                            v
                        };

                        // write the compressed revision out
                        let lock_index = (revision_id % N_REVISION_FILES) as usize;
                        let mut write_guard = writer_locks[lock_index].lock().unwrap();
                        let record_start = write_guard.size;
                        let record_length = compressed_bytes.len() as u64;
                        write_guard.write(&compressed_bytes);
                        (record_start, record_length)
                    };

                    // send summary info for constructing date & id mappings.
                    let date = DateTime::parse_from_rfc3339(&record[6]).unwrap();
                    (date, revision_id, (record_start, record_length))
                }
            )
            .collect()
    };

    println!("pipe read for {} completed. saving indices... 🎸", input_path);

    let mut date_map = dates_to_ids.lock().unwrap();
    let mut id_map = ids_to_positions.lock().unwrap();
    records_vec
        .drain(..)
        .for_each(
            |(date, revision_id, position)| {
                // populate date_map
                date_map
                    .entry(date)
                    .or_insert_with(Vec::new)
                    .push(revision_id);

                // populate id_map
                id_map.insert(revision_id, position);
            }
        );

    println!("indices from pipe {} saved. 👩‍🎤", input_path);
}

fn process_input_pipes(
    downloader_receiver: Receiver<bool>
) {
    let pipe_dir = "/pipes";
    let mut pending_receivers = HashMap::new();
    pending_receivers.insert("_".to_string(), downloader_receiver);
    let re = Regex::new(r"revisions-\d+-\d+\.pipe").unwrap();
    let mut complete = false;
    let mut one_found = false;
    let dates_to_ids: Arc<Mutex<DatesToIds>> = Default::default();
    let ids_to_positions: Arc<Mutex<IdsToPositions>> = Default::default();
    let mut processor_threads = Vec::new();
    let writer_locks = {
        let mut inner_locks = Vec::new();
        for i in 0..N_REVISION_FILES {
            let path = format!(
                "{}/{}",
                STORAGE_DIR,
                i
            );
            let f = File::create(path).unwrap();
            let buf = BufWriter::with_capacity(2 * 1024 * 1024,f);
            let writer_counter = WriteCounter {
                writer: buf,
                size: 0
            };
            inner_locks.push(Mutex::new(writer_counter));
        }
        Arc::new(inner_locks)
    };

    // until the downloader is complete, scan for open pipes. Each time one is opened, start a
    // thread devoted to reading its contents into the storage directory.
    while !complete {
        for entry in read_dir(pipe_dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            if re.is_match(&name) && !pending_receivers.contains_key(&name) {
                let (tx, rx) = bounded(1);
                let dates_to_ids = Arc::clone(&dates_to_ids);
                let ids_to_positions = Arc::clone(&ids_to_positions);
                let writer_locks = Arc::clone(&writer_locks);
                processor_threads.push(
                    thread::spawn(
                        move || {
                            let entry_path = entry.path();
                            let path = entry_path.to_str().unwrap();
                            revisions_csv_to_files(
                                path,
                                dates_to_ids,
                                ids_to_positions,
                                writer_locks
                            );
                            remove_file(path).unwrap();
                            tx.send(true).unwrap();
                        }
                    )
                );
                pending_receivers.insert(name, rx);
                one_found = true;
            }
        }

        if !one_found {
            println!("[process_input_pipes] awaiting revision pipes...");
            thread::sleep(Duration::from_secs(60))
        } else if pending_receivers.is_empty() {
            complete = true;
        } else {
            // pause this thread until the downloader or one of the
            // open processors sends a completion message.
            // Restart this thread once per minute to scan for any
            // new pipes that might have been created.

            let mut select = Select::new();
            for receiver in pending_receivers.values() {
                select.recv(receiver);
            }

            select.ready_timeout(Duration::from_secs(60)).ok();

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

    // we need all of the processors to finish before writing out the maps
    for handle in processor_threads {
        handle.join().unwrap()
    }

    // empty file buffers
    writer_locks
        .iter()
        .for_each(
            |mutex| {
                let mut writer = mutex.lock().unwrap();
                writer.flush();
            }
        );

    // save date to id map
    let date_map_file = File::create(
        DATES_TO_IDS_FILE
    ).unwrap();
    serde_json::to_writer(
        date_map_file,
        &*dates_to_ids.lock().unwrap()
    ).unwrap();

    // save id to position map
    let id_map_file = File::create(
        IDS_TO_POSITIONS_FILE
    ).unwrap();
    serde_json::to_writer(
        id_map_file,
        &*ids_to_positions.lock().unwrap()
    ).unwrap();
}

/// Wraps https://github.com/dominicburkart/wikipedia-revisions
fn download_revisions(date: String) {
    let downloader_receiver = {
        let num_subprocesses = format!("{}", cmp::max((num_cpus::get() * 2) - 2, 4));
        let (tx, rx) = bounded(1);

        thread::spawn(
            move || {
                println!("starting downloader program...");
                let status = Command::new("/src/download")
                    .arg(WORKING_DIR)
                    .arg(&date)
                    .arg(&num_subprocesses)
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

    process_input_pipes(downloader_receiver);
}

fn iter_to_byte_stream<'a, It, T1>(it: It) -> impl Stream<Item=serde_json::Result<bytes::Bytes>>
    where
        It: Iterator<Item=T1>,
        T1: Serialize + Deserialize<'a> {
    let byte_result_iter = it.map(
        |v| {
            match serde_json::to_vec(&v) {
                Err(e) => Err(e),
                Ok(s) => Ok(Bytes::from(s))
            }
        }
    );
    stream::iter(byte_result_iter)
}

#[get("{start}/{end}/diffs")]
async fn get_diffs_for_period(info: web::Path<(Instant, Instant)>) -> impl Responder {
    let stream = iter_to_byte_stream(
        STATE.diffs_for_period(&info.0, &info.1)
    );
    HttpResponse::Ok()
        .streaming(stream)
}

#[get("{start}/{end}/revisions")]
async fn get_revisions_for_period(info: web::Path<(Instant, Instant)>) -> impl Responder {
    let stream = iter_to_byte_stream(
        STATE.revisions_from_period(&info.0, &info.1)
    );
    HttpResponse::Ok()
        .streaming(stream)
}

#[actix_rt::main]
async fn server(bind: String) -> std::io::Result<()> {
    HttpServer::new(|| {
        ActixApp::new()
            .wrap(middleware::Compress::default())
            .service(get_diffs_for_period)
            .service(get_revisions_for_period)
    })
        .keep_alive(45)
        .bind(&bind)?
        .run()
        .await
}

fn main() {
    let matches = App::new("Wikipedia Revisions Server")
        .version("0.1")
        .author("Dominic <@DominicBurkart>")
        .about("Serves new & updated wikipedia articles (or fragments) within a set time period.")
        .arg(Arg::with_name("date")
           .short("d")
           .long("date")
           .value_name("DATE")
           .help("download revisions from the wikidump on this date. Format YYYYMMDD (e.g. 20201201). If not passed, local revisions are used.")
           .takes_value(true))
        .arg(Arg::with_name("bind")
           .short("b")
           .long("bind")
           .value_name("BIND")
           .help("address and port to bind the server to. Example: 127.0.0.1:8088")
           .takes_value(true))
      .get_matches();

    // if we have a passed date, download the revisions
    if let Some(date) = matches.value_of("date") {
        download_revisions(
            date.to_string()
        );
    }

    // instantiate global state object so it's ready before the first request
    STATE.get_revision(1);

    // start server
    let bind = matches
        .value_of("bind")
        .unwrap()
        .to_string();
    server(bind).unwrap();
}