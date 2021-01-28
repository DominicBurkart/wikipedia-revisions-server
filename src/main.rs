#[macro_use]
extern crate lazy_static;

use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::default::Default;
use std::error::Error;
use std::fs::{File, read_dir, remove_file};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Bound::{Included, Unbounded};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use actix_web::{App as ActixApp, get, HttpResponse, HttpServer, middleware, Responder, web};
use bytes::Bytes;
use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use clap::{App, Arg};
use colored::*;
use crossbeam::crossbeam_channel::{bounded, Receiver, Select};
use futures::stream::{self, Stream};
use regex::Regex;
use serde::{Deserialize, Serialize};

const BUF_SIZE: usize = 512 * 1024;
const BROTLI_DATA_COMPRESSION_LEVEL: u32 = 8;
const BROTLI_INDEX_COMPRESSION_LEVEL: u32 = 3;
const FAST_DIR: &str = "/fast_dir";
const BIG_DIR: &str = "/big_dir";
const PIPE_DIR: &str = "/pipes";
const DATES_TO_IDS_INTERMEDIARY_CSV: &str = "fast_dir/dates_to_ids.csv";
const SUPER_DATE_BTREE_FILE: &str = "fast_dir/super_date_btree_file.json";
const N_REVISION_FILES: u64 = 200; // note: changing this field requires rebuilding files
// ^ must be less than max usize.

type RevisionID = u64;
type ContributorID = u64;
type PageID = u64;
type Instant = DateTime<FixedOffset>;
type Offset = u64;
type RecordLength = u64;
type RecordFileName = String;
type Position = (RecordFileName, Offset, RecordLength);
type NewRevisionFragment = String;
type CompressedRevision = Vec<u8>;
type UnixTimeStamp = i64;
type PageNs = u32;
// position of record in corresponding file

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
    page_ns: PageNs,
}

#[derive(Serialize, Deserialize)]
struct PositionEntry {
    revision_id: RevisionID,
    record_start: Offset,
    record_length: RecordLength,
}

#[derive(Serialize, Deserialize)]
struct DateEntry {
    revision_id: RevisionID,
    instant: Instant,
}

#[derive(Debug, Clone)]
pub struct RetrievalError {}

impl std::fmt::Display for RetrievalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "retrieval error while trying to access revisions on disk"
        )
    }
}

impl Error for RetrievalError {}

pub struct State {
    starting_date_per_index: BTreeMap<Instant, String>,
}

impl State {
    fn id_to_position(&self, id: RevisionID) -> Result<Position, Box<dyn Error>> {
        let index_path = position_map_file_from_id(id);
        let index_file = File::open(index_path)?;
        let index_buf = BufReader::with_capacity(BUF_SIZE, index_file);
        let mut reader = csv::Reader::from_reader(index_buf);
        for result in reader.deserialize() {
            let record: PositionEntry = result?;
            if record.revision_id == id {
                return Ok((
                    path_from_revision_id(id),
                    record.record_start,
                    record.record_length,
                ));
            }
        }
        Err(RetrievalError {}.into())
    }

    fn revision_ids_from_period<'a>(
        &'a self,
        start: Instant,
        end: Instant,
    ) -> impl Iterator<Item=Vec<RevisionID>> + 'a {

        // the prior start and trailing end are the
        // edges of the window of the trees that
        // contain the revisions from from start to end.

        let included_window_start = self
            .starting_date_per_index
            .range((Unbounded, Included(start)))
            .map(|(instant, _path)| instant)
            .next_back()
            .unwrap_or(&start); // start is out of known range

        let included_window_end = self
            .starting_date_per_index
            .range(end..)
            .map(|(instant, _path)| instant)
            .next()
            .unwrap_or(&end); // end is out of known range

        self.starting_date_per_index
            .range((Included(included_window_start), Included(included_window_end)))
            .map(
                move |(_date, path)| {
                    let time_indices: BTreeMap<Instant, Vec<RevisionID>> = {
                        let f = File::open(path).unwrap();
                        let buf = BufReader::with_capacity(BUF_SIZE, f);
                        let compressor = brotli2::read::BrotliDecoder::new(buf);
                        serde_json::from_reader(compressor).unwrap()
                    };
                    time_indices
                        .range(start..end)
                        .map(|(_date, ids)| ids)
                        .flatten()
                        .copied()
                        .collect()
                }
            )
    }

    fn revisions_from_period<'a>(
        &'a self,
        start: Instant,
        end: Instant,
    ) -> impl Iterator<Item=Revision> + 'a {
        self.revision_ids_from_period(start, end)
            .flatten()
            .map(move |id| self.get_revision(id))
    }

    fn diffs_for_period<'a>(
        &'a self,
        start: Instant,
        end: Instant,
    ) -> impl Iterator<Item=Vec<NewRevisionFragment>> + 'a {
        self.revision_ids_from_period(start, end)
            .flatten()
            .map(move |id| self.get_new_or_modified_fragments(id))
    }

    fn get_revision(&self, id: RevisionID) -> Revision {
        // todo should return a Result, but I wasn't sure what the error type should be
        let uncompressed_serialized_revision = {
            let compressed_revision = {
                let (path, offset, length) = self.id_to_position(id).unwrap();
                let mut file = File::open(&path).unwrap();
                let mut compressed_revision = vec![0; usize::try_from(length).unwrap()];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut compressed_revision).unwrap();
                compressed_revision
            };

            let mut decompressor = brotli2::read::BrotliDecoder::new(&*compressed_revision);
            let mut uncompressed_file_contents = String::new();
            decompressor
                .read_to_string(&mut uncompressed_file_contents)
                .unwrap();
            uncompressed_file_contents
        };
        serde_json::from_str(&uncompressed_serialized_revision).unwrap()
    }

    fn diff<'a>(&self, _old: &'a str, _new: &'a str) -> Vec<NewRevisionFragment> {
        unimplemented!();
    }

    fn get_new_or_modified_fragments(&self, id: RevisionID) -> Vec<NewRevisionFragment> {
        let revision = self.get_revision(id);
        match revision.text {
            Some(revision_text) => {
                if let Some(parent_id) = revision.parent_id {
                    let parent = self.get_revision(parent_id);
                    return self.diff(&parent.text.unwrap_or_default(), &revision_text);
                }
                self.diff("", &revision_text)
            }
            None => {
                if let Some(parent_id) = revision.parent_id {
                    let parent = self.get_revision(parent_id);
                    return self.diff(
                        &parent.comment.unwrap_or_default(),
                        &revision.comment.unwrap(),
                    );
                }
                self.diff("", &revision.comment.unwrap())
            }
        }
    }
}

lazy_static! {
    pub static ref STATE: State = {
        log("initializing state... ");
        let f = File::open(SUPER_DATE_BTREE_FILE).unwrap();
        let buf = BufReader::with_capacity(BUF_SIZE, f);
        let super_map: BTreeMap<Instant, String> = serde_json::from_reader(buf).unwrap();
        State {
            starting_date_per_index: super_map,
        }
    };
    pub static ref BUCKET_FROM_LITTLE_MAP_PACK: Regex = {
        let pattern = format!(r"{}/(\d+)_temp_date_mapping.csv", FAST_DIR);
        Regex::new(&pattern).unwrap()
    };
}

struct WriteCounter {
    writer: BufWriter<File>,
    size: u64,
}

impl WriteCounter {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.writer.write_all(bytes)?;
        self.size += bytes.len() as u64;
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

fn path_from_revision_id(id: RevisionID) -> String {
    format!("{}/{}", BIG_DIR, id % N_REVISION_FILES)
}

fn position_map_file_from_id(id: RevisionID) -> String {
    format!("{}/{}_maps.csv", FAST_DIR, id % N_REVISION_FILES)
}

fn compress_revision(revision: &Revision) -> CompressedRevision {
    let revision_string = serde_json::to_string(revision).unwrap();

    let mut v = Vec::new();
    {
        let mut writer = brotli2::write::BrotliEncoder::new(&mut v, BROTLI_DATA_COMPRESSION_LEVEL);
        writer.write_all(revision_string.as_bytes()).unwrap();
    }
    v
}

fn write_compressed_revision(
    compressed_bytes: CompressedRevision,
    mut write_guard: std::sync::MutexGuard<WriteCounter>,
) -> (u64, u64) {
    let record_start = {
        let record_start = write_guard.size;
        write_guard.write_all(&compressed_bytes).unwrap();
        record_start
    };
    let record_length = compressed_bytes.len() as u64;
    (record_start, record_length)
}

fn revisions_csv_to_files(
    input_path: &str,
    dates_to_ids: Arc<Mutex<csv::Writer<BufWriter<File>>>>,
    ids_to_positions: Arc<Mutex<Vec<csv::Writer<BufWriter<File>>>>>,
    writer_locks: Arc<Vec<Mutex<WriteCounter>>>,
) {
    let mut records_vec: Vec<(Instant, u64, Offset, RecordLength)> = {
        let reader = csv::Reader::from_path(&input_path).unwrap();

        reader
            .into_deserialize()
            .filter_map(Result::ok)
            .map(|record: Revision| {
                // row content:
                //                        [
                //                            "id",
                //                            "parent_id",
                //                            "page_title",
                //                            "contributor_id",
                //                            "contributor_name",
                //                            "contributor_ip",
                //                            "timestamp",
                //                            "text",
                //                            "comment",
                //                            "page_id",
                //                            "page_ns",
                //                        ]

                // write record to file
                let (record_start, record_length) = {
                    let compressed_bytes = compress_revision(&record);
                    let lock_index = (record.id % N_REVISION_FILES) as usize;
                    let writer_lock = writer_locks.get(lock_index).unwrap();
                    let write_guard = writer_lock.lock().unwrap();
                    write_compressed_revision(compressed_bytes, write_guard)
                };

                // send summary info for constructing date & id mappings.
                let date = DateTime::parse_from_rfc3339(&record.timestamp).unwrap();
                (date, record.id, record_start, record_length)
            })
            .collect()
    };

    log(&format!(
        "pipe read for {} completed. saving indices... 🎸",
        input_path
    ));

    let mut date_writer = dates_to_ids.lock().unwrap();
    let mut id_map_writers = ids_to_positions.lock().unwrap();
    records_vec
        .drain(..)
        .for_each(|(instant, revision_id, record_start, record_length)| {
            // populate date_map
            date_writer
                .serialize(DateEntry {
                    revision_id,
                    instant,
                })
                .unwrap();

            // populate id map
            let index_file = (revision_id % N_REVISION_FILES) as usize;
            id_map_writers[index_file]
                .serialize(PositionEntry {
                    revision_id,
                    record_start,
                    record_length,
                })
                .unwrap();
        });

    log(&format!("indices from pipe {} saved. 👩‍🎤", input_path));
}

fn all_revisions_files() -> impl Iterator<Item=String> {
    (0..N_REVISION_FILES).map(|i| format!("{}/{}", BIG_DIR, i))
}

fn all_ids_to_positions_paths() -> impl Iterator<Item=String> {
    (0..N_REVISION_FILES).map(|i| format!("{}/{}_maps.csv", FAST_DIR, i))
}

fn ith_temporary_little_date_path(i: u64) -> String {
    format!("{}/{}_temp_date_mapping.csv", FAST_DIR, i)
}

fn all_temporary_little_date_file_paths() -> impl Iterator<Item=String> {
    (1..(N_REVISION_FILES + 1)).map(ith_temporary_little_date_path)
}

fn ith_date_to_id_path(i: &str) -> String {
    format!("{}/{}_date_map.json", FAST_DIR, i)
}

fn temporary_little_date_file_path_to_date_to_id_path(little_file_path: &str) -> String {
    let bucket = BUCKET_FROM_LITTLE_MAP_PACK
        .captures(little_file_path)
        .unwrap()
        .get(1)
        .unwrap()
        .as_str();
    ith_date_to_id_path(bucket)
}

fn get_largest_id() -> RevisionID {
    all_ids_to_positions_paths()
        .filter_map(|path| {
            csv::Reader::from_path(path)
                .unwrap()
                .into_deserialize()
                .filter_map(|res| res.ok())
                .map(|position_entry: PositionEntry| position_entry.revision_id)
                .max()
        })
        .max()
        .unwrap()
}

fn process_input_pipes(downloader_receiver: Receiver<bool>) {
    let pipe_dir = "/pipes";
    let mut pending_receivers = HashMap::new();
    pending_receivers.insert("_".to_string(), downloader_receiver);
    let re = Regex::new(r"revisions-\d+-\d+\.pipe").unwrap();
    let mut complete = false;
    let mut one_found = false;
    let mut processor_threads = Vec::new();
    let writer_locks = {
        let mut inner_locks = Vec::new();
        all_revisions_files().for_each(|path| {
            let f = File::create(path).unwrap();
            let buf = BufWriter::with_capacity(BUF_SIZE, f);
            let writer_counter = WriteCounter {
                writer: buf,
                size: 0,
            };
            inner_locks.push(Mutex::new(writer_counter));
        });
        Arc::new(inner_locks)
    };
    let ids_to_positions = {
        let mut csv_writers = Vec::new();
        all_ids_to_positions_paths().for_each(|path| {
            let f = File::create(path).unwrap();
            let buf = BufWriter::with_capacity(BUF_SIZE, f);
            let csv_writer = csv::Writer::from_writer(buf);
            csv_writers.push(csv_writer);
        });
        Arc::new(Mutex::new(csv_writers))
    };
    let dates_to_ids = {
        let path = DATES_TO_IDS_INTERMEDIARY_CSV;
        let f = File::create(path).unwrap();
        let buf = BufWriter::with_capacity(BUF_SIZE, f);
        Arc::new(Mutex::new(csv::Writer::from_writer(buf)))
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
                processor_threads.push(thread::spawn(move || {
                    let entry_path = entry.path();
                    let path = entry_path.to_str().unwrap();
                    revisions_csv_to_files(path, dates_to_ids, ids_to_positions, writer_locks);
                    remove_file(path).unwrap();
                    tx.send(true).unwrap();
                }));
                pending_receivers.insert(name, rx);
                one_found = true;
            }
        }

        if !one_found {
            log("[process_input_pipes] awaiting revision pipes...");
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

            pending_receivers.retain(|_, receiver| !matches!(receiver.try_recv(), Ok(_)));
        }
    }

    // we need all of the processors to finish before writing out the maps
    for handle in processor_threads {
        handle.join().unwrap()
    }

    // empty file buffers
    writer_locks.iter().for_each(|mutex| {
        let mut writer = mutex.lock().unwrap();
        writer.flush().unwrap();
    });
    ids_to_positions
        .lock()
        .unwrap()
        .iter_mut()
        .for_each(|writer| writer.flush().unwrap());
    dates_to_ids.lock().unwrap().flush().unwrap();
    log("revision processing complete. generating date indices...");

    // *assuming that ids increase monotonically over time,*
    // we can use the largest ID to split the ids by date.
    let largest_id = get_largest_id();
    let approx_bucket_size = std::cmp::max(largest_id / N_REVISION_FILES, 1);

    // split the big date file into little date files
    let mut little_date_file_writers = {
        let mut writers = Vec::new();
        for path in all_temporary_little_date_file_paths() {
            let f = File::create(path).unwrap();
            let buf = BufWriter::with_capacity(BUF_SIZE, f);
            let compressor = brotli2::write::BrotliEncoder::new(buf, BROTLI_INDEX_COMPRESSION_LEVEL);
            writers.push(csv::Writer::from_writer(compressor));
        }
        writers
    };
    let mut dates_to_ids_reader = csv::Reader::from_path(DATES_TO_IDS_INTERMEDIARY_CSV).unwrap();
    dates_to_ids_reader
        .deserialize()
        .for_each(
            |res| {
                let record: DateEntry = res.unwrap();
                let has_remainder = record.revision_id % approx_bucket_size != 0;
                let bucket = {
                    if has_remainder {
                        let bucket = record.revision_id / approx_bucket_size;
                        if bucket > N_REVISION_FILES {
                            bucket - 1 // handle last remainders
                        } else {
                            bucket
                        }
                    } else {
                        (record.revision_id / approx_bucket_size) + 1
                    }
                };
                little_date_file_writers[bucket as usize]
                    .serialize(record)
                    .unwrap()
            }
        );
    little_date_file_writers
        .drain(..)
        .for_each(|mut writer| writer.flush().unwrap());

    // convert the little date files into serialized binary tree maps
    let mut b_tree_map_files: BTreeMap<Instant, String> = Default::default();
    all_temporary_little_date_file_paths()
        .for_each(
            |path| {
                let out_path = temporary_little_date_file_path_to_date_to_id_path(&path);
                let mut output_map: BTreeMap<Instant, Vec<RevisionID>> = Default::default();
                let in_file = File::open(&path).unwrap();
                let in_buf = BufReader::with_capacity(BUF_SIZE, in_file);
                let decompressor = brotli2::read::BrotliDecoder::new(in_buf);
                csv::Reader::from_reader(decompressor)
                    .into_deserialize()
                    .for_each(
                        |res| {
                            let record: DateEntry = res.unwrap();
                            output_map
                                .entry(record.instant)
                                .or_insert_with(Vec::new)
                                .push(record.revision_id);
                        }
                    );
                if let Some(min_value) = output_map.keys().next() {
                    let out_file = File::create(&out_path).unwrap();
                    let out_buf = BufWriter::with_capacity(BUF_SIZE, out_file);
                    let out_compressor = brotli2::write::BrotliEncoder::new(out_buf, BROTLI_INDEX_COMPRESSION_LEVEL);
                    serde_json::to_writer(out_compressor, &output_map).unwrap();
                    b_tree_map_files.insert(*min_value, out_path);
                } else {
                    warning(
                        &format!(
                            "Empty temp date file detected: {} Are we testing with a small number of revisions?",
                            &path
                        )
                    );
                }
                remove_file(path).unwrap();
            }
        );

    // save super date index
    let super_map_file = File::create(SUPER_DATE_BTREE_FILE).unwrap();
    let out_writer = BufWriter::with_capacity(BUF_SIZE, super_map_file);
    serde_json::to_writer(out_writer, &b_tree_map_files).unwrap();
    log("date indices saved.");
}

/// Wraps https://github.com/dominicburkart/wikipedia-revisions
fn download_revisions(date: String) {
    let downloader_receiver = {
        let num_subprocesses = format!("{}", cmp::max(num_cpus::get(), 2));
        let (tx, rx) = bounded(1);

        thread::spawn(move || {
            log("starting downloader program...");
            let status = Command::new("/src/download")
                .arg(FAST_DIR)
                .arg(&date)
                .arg(&num_subprocesses)
                .arg(PIPE_DIR)
                .status()
                .unwrap();
            if !status.success() {
                match status.code() {
                    Some(n) => panic!("loader failed. Exit code: {}", n),
                    None => panic!("loader failed. No exit code collected."),
                }
            }
            tx.send(true).unwrap();
        });
        rx
    };

    process_input_pipes(downloader_receiver);
}

fn iter_to_byte_stream<'a, It, T1>(it: It) -> impl Stream<Item=serde_json::Result<bytes::Bytes>>
    where
        It: Iterator<Item=T1>,
        T1: Serialize + Deserialize<'a>,
{
    let byte_result_iter = it.map(
        |obj|
            match serde_json::to_string(&obj) {
                Err(e) => Err(e),
                Ok(mut s) => {
                    s.push('\n');
                    Ok(Bytes::from(s.into_bytes()))
                }
            }
    );
    stream::iter(byte_result_iter)
}

#[get("/{start}/{end}/diffs")]
async fn get_diffs_for_period(info: web::Path<(UnixTimeStamp, UnixTimeStamp)>) -> impl Responder {
    let start = FixedOffset::east(0).timestamp(info.0, 0); // arbitrary offset
    let end = FixedOffset::east(0).timestamp(info.1, 0);
    let stream = iter_to_byte_stream(STATE.diffs_for_period(start, end));
    HttpResponse::Ok().streaming(stream)
}

#[get("/{start}/{end}/revisions")]
async fn get_revisions_for_period(info: web::Path<(UnixTimeStamp, UnixTimeStamp)>) -> impl Responder {
    let start = FixedOffset::east(0).timestamp(info.0, 0); // arbitrary offset
    let end = FixedOffset::east(0).timestamp(info.1, 0);
    let stream = iter_to_byte_stream(STATE.revisions_from_period(start, end));
    HttpResponse::Ok().streaming(stream)
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

fn log(s: &str) {
    let dt = Utc::now();
    let message = (dt.format("%+").to_string() + " " + s).magenta().on_blue();
    println!("{}", message);
}

fn warning(s: &str) {
    let dt = Utc::now();
    let message = (dt.format("%+").to_string() + " " + s).yellow().on_green();
    println!("{}", message);
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
        download_revisions(date.to_string());
    }

    // start server
    let bind = matches.value_of("bind").unwrap().to_string();
    server(bind).unwrap();
}

#[cfg(test)]
mod tests {
    use std::fs;

    use actix_web::test as actix_test;
    use futures_util::stream::TryStreamExt;

    use super::*;

    #[test]
    fn test_temporary_little_date_file_path_to_date_to_id_path() {
        for i in 0..(N_REVISION_FILES + 1) {
            let lil_temp_path = ith_temporary_little_date_path(i);
            let derived_date_to_id =
                temporary_little_date_file_path_to_date_to_id_path(&lil_temp_path);
            let true_date_to_id = ith_date_to_id_path(&i.to_string());
            assert_eq!(derived_date_to_id, true_date_to_id);
        }
    }

    #[actix_rt::test]
    async fn test_get_revisions() {
        let mut app = {
            // copy test data
            let fake_pipe_path = format!("{}/{}", PIPE_DIR, "revisions-0-0.pipe");
            fs::copy("test_data/sample.csv", &fake_pipe_path).unwrap();

            // process data
            let (tx, rx) = bounded(1);
            tx.send(true).unwrap();
            process_input_pipes(rx);

            // run server
            actix_test::init_service(
                ActixApp::new()
                    .wrap(middleware::Compress::default())
                    .service(get_diffs_for_period)
                    .service(get_revisions_for_period),
            )
                .await
        };

        // return all test revisions
        let inclusive_uri = format!(
            "/{}/{}/revisions",
            DateTime::parse_from_rfc3339("2017-03-18T04:23:23Z")
                .unwrap()
                .timestamp(),
            DateTime::parse_from_rfc3339("2017-03-20T04:23:23Z")
                .unwrap()
                .timestamp()
        );
        let req = actix_test::TestRequest::with_uri(&inclusive_uri).to_request();
        let mut resp = actix_test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let bytes = actix_test::load_stream(resp.take_body().into_stream())
            .await
            .unwrap();
        let response_str = std::str::from_utf8(&bytes).unwrap();
        let revisions: Vec<Revision> =
            response_str
                .lines()
                .map(
                    |line| serde_json::from_str(line).unwrap()
                )
                .collect();
        let first_revision = Revision {
            id: 1,
            parent_id: None,
            page_title: "nice".to_string(),
            contributor_id: None,
            contributor_name: None,
            contributor_ip: Some("192.168.0.1".to_string()),
            timestamp: "2017-03-19T04:23:23Z".to_string(),
            text: Some("hi".to_string()),
            comment: None,
            page_id: 1,
            page_ns: 1,
        };
        let middle_revision = Revision {
            id: 2,
            parent_id: Some(1),
            page_title: "nice".to_string(),
            contributor_id: Some(1),
            contributor_name: Some("person".to_string()),
            contributor_ip: None,
            timestamp: "2017-03-19T04:24:23Z".to_string(),
            text: Some("hi\nhi".to_string()),
            comment: None,
            page_id: 1,
            page_ns: 1,
        };
        let final_revision = Revision {
            id: 3,
            parent_id: None,
            page_title: "also nice".to_string(),
            contributor_id: Some(2),
            contributor_name: Some("another_person".to_string()),
            contributor_ip: None,
            timestamp: "2017-03-19T04:25:23Z".to_string(),
            text: None,
            comment: Some("sometimes there are comments".to_string()),
            page_id: 2,
            page_ns: 2,
        };
        assert_eq!(
            revisions,
            vec![first_revision, middle_revision, final_revision]
        );

        // return some revisions
        let exclusive_uri = format!(
            "/{}/{}/revisions",
            DateTime::parse_from_rfc3339("2017-03-19T04:24:21Z")
                .unwrap()
                .timestamp(),
            DateTime::parse_from_rfc3339("2017-03-19T04:24:30Z")
                .unwrap()
                .timestamp()
        );
        let req = actix_test::TestRequest::with_uri(&exclusive_uri).to_request();
        let mut resp = actix_test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let bytes = actix_test::load_stream(resp.take_body().into_stream())
            .await
            .unwrap();
        let response_str = std::str::from_utf8(&bytes).unwrap();
        let revisions: Vec<Revision> = response_str
            .lines()
            .map(
                |line| serde_json::from_str(line).unwrap()
            )
            .collect();
        assert_eq!(
            revisions,
            vec![middle_revision]
        );

        // queries are left-inclusive to passed values, but not right-inclusive
        let exclusive_uri = format!(
            "/{}/{}/revisions",
            DateTime::parse_from_rfc3339("2017-03-19T04:24:23Z")
                .unwrap()
                .timestamp(),
            DateTime::parse_from_rfc3339("2017-03-19T04:25:23Z")
                .unwrap()
                .timestamp()
        );
        let req = actix_test::TestRequest::with_uri(&exclusive_uri).to_request();
        let mut resp = actix_test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let bytes = actix_test::load_stream(resp.take_body().into_stream())
            .await
            .unwrap();
        let response_str = std::str::from_utf8(&bytes).unwrap();
        let revisions: Vec<Revision> = response_str
            .lines()
            .map(
                |line| serde_json::from_str(line).unwrap()
            )
            .collect();
        assert_eq!(
            revisions,
            vec![middle_revision]
        );
    }
}
