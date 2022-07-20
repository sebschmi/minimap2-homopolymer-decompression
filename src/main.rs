use cbor::Decoder;
use clap::Parser;
use crossbeam::channel;
use log::{info, LevelFilter};
use minimap2_paf_io::data::PAFLine;
use minimap2_paf_io::input::parse_line;
use simplelog::{ColorChoice, TermLogger, TerminalMode};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

#[derive(Parser, Clone, Debug)]
struct Configuration {
    /// The input file. Must be in wtdbg2's .ctg.lay format.
    #[clap(long, parse(from_os_str))]
    input: PathBuf,

    /// The output file. Must be in wtdbg2's .ctg.lay format.
    #[clap(long, parse(from_os_str))]
    output: PathBuf,

    /// The file containing the map used to homopolymer decompress the input.
    // TODO which hodeco map is this, reference hopefully?
    #[clap(long, parse(from_os_str))]
    hodeco_map: PathBuf,

    /// The size of the queues between threads.
    #[clap(long, default_value = "32768")]
    queue_size: usize,

    /// The size of the I/O buffers in bytes.
    #[clap(long, default_value = "67108864")]
    io_buffer_size: usize,

    /// The number of compute threads to use for decompression.
    /// Note that the input and output threads are not counted under this number.
    #[clap(long, default_value = "1")]
    compute_threads: usize,

    /// The level of log messages to be produced.
    #[clap(long, default_value = "Info")]
    log_level: LevelFilter,
}

fn initialise_logging(log_level: &LevelFilter) {
    TermLogger::init(
        *log_level,
        Default::default(),
        TerminalMode::Stderr,
        ColorChoice::Auto,
    )
    .unwrap();
    info!("Logging initialised successfully")
}

fn main() {
    let configuration = Configuration::parse();
    initialise_logging(&configuration.log_level);

    info!("Opening files...");
    let input_file = File::open(&configuration.input)
        .unwrap_or_else(|error| panic!("Cannot open input file: {error:?}"));
    let output_file = File::create(&configuration.output)
        .unwrap_or_else(|error| panic!("Cannot open output file: {error:?}"));
    let hodeco_map_file = File::open(&configuration.hodeco_map)
        .unwrap_or_else(|error| panic!("Cannot open hodeco map file: {error:?}"));
    let hodeco_map_reader = BufReader::with_capacity(configuration.io_buffer_size, hodeco_map_file);
    let mut hodeco_map_decoder = Decoder::from_reader(hodeco_map_reader);

    info!("Loading hodeco map...");
    let hodeco_maps: HashMap<_, _> = hodeco_map_decoder
        .decode::<(String, Vec<usize>)>()
        .map(|result| match result {
            Ok(item) => item,
            Err(error) => panic!("Cannot read hodeco map: {error:?}"),
        })
        .collect();

    info!("Creating channels and threads...");
    crossbeam::scope(|scope| {
        let (input_sender, input_receiver) = channel::bounded(configuration.queue_size);
        scope
            .builder()
            .name("input_thread".to_string())
            .spawn(move |_| {
                let input_file_reader =
                    BufReader::with_capacity(configuration.io_buffer_size, input_file);
                for line in input_file_reader.lines() {
                    let line =
                        line.unwrap_or_else(|error| panic!("Cannot read PAF line: {error:?}"));
                    let mut line = line.as_str();
                    let paf_line = parse_line(&mut line)
                        .unwrap_or_else(|error| panic!("Cannot parse PAF line: {error:?}"));
                    assert!(line.is_empty(), "Line was not parsed completely");
                    input_sender
                        .send(paf_line)
                        .unwrap_or_else(|error| panic!("Cannot send PAF line: {error:?}"));
                }
            })
            .unwrap_or_else(|error| panic!("Cannot spawn input thread: {error:?}"));

        let (output_sender, output_receiver) = channel::bounded::<String>(configuration.queue_size);
        scope
            .builder()
            .name("output_thread".to_string())
            .spawn(move |_| {
                let mut output_file_writer =
                    BufWriter::with_capacity(configuration.io_buffer_size, output_file);
                while let Ok(hodeco_paf_line) = output_receiver.recv() {
                    output_file_writer
                        .write_all(hodeco_paf_line.as_bytes())
                        .unwrap_or_else(|error| panic!("Cannot write PAF line: {error:?}"));
                    output_file_writer
                        .write_all(&[b'\n'])
                        .unwrap_or_else(|error| panic!("Cannot write line feed: {error:?}"));
                }
            })
            .unwrap_or_else(|error| panic!("Cannot spawn input thread: {error:?}"));

        for thread_id in 0..configuration.compute_threads {
            let hodeco_maps = &hodeco_maps;
            let input_receiver = input_receiver.clone();
            let output_sender = output_sender.clone();
            scope
                .builder()
                .name(format!("compute_thread_{thread_id}"))
                .spawn(move |_| {
                    while let Ok(paf_line) = input_receiver.recv() {
                        let hodeco_paf_line = hodeco_paf_line(paf_line, hodeco_maps);
                        let hodeco_paf_line = hodeco_paf_line.to_string();
                        output_sender
                            .send(hodeco_paf_line)
                            .unwrap_or_else(|error| panic!("Cannot send PAF line: {error:?}"));
                    }
                })
                .unwrap_or_else(|error| panic!("Cannot spawn input thread: {error:?}"));
        }
    })
    .unwrap_or_else(|error| panic!("Error: {error:?}"));
}

fn hodeco_paf_line(hoco_paf: PAFLine, hodeco_maps: &HashMap<String, Vec<usize>>) -> PAFLine {
    todo!()
}
