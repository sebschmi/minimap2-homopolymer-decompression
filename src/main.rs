use cbor::Decoder;
use clap::Parser;
use crossbeam::channel;
use log::{info, LevelFilter};
use minimap2_paf_io::data::{CigarColumn, DifferenceColumn, PAFLine};
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

    /// The file containing the homopolymer compression map of the query sequences.
    #[clap(long, parse(from_os_str))]
    query_hodeco_map: PathBuf,

    /// The file containing the homopolymer compression map of the target sequences.
    #[clap(long, parse(from_os_str))]
    target_hodeco_map: PathBuf,

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

    let query_hodeco_map_file = File::open(&configuration.query_hodeco_map)
        .unwrap_or_else(|error| panic!("Cannot open query hodeco map file: {error:?}"));
    let query_hodeco_map_reader =
        BufReader::with_capacity(configuration.io_buffer_size, query_hodeco_map_file);
    let mut query_hodeco_map_decoder = Decoder::from_reader(query_hodeco_map_reader);

    let target_hodeco_map_file = File::open(&configuration.target_hodeco_map)
        .unwrap_or_else(|error| panic!("Cannot open target hodeco map file: {error:?}"));
    let target_hodeco_map_reader =
        BufReader::with_capacity(configuration.io_buffer_size, target_hodeco_map_file);
    let mut target_hodeco_map_decoder = Decoder::from_reader(target_hodeco_map_reader);

    info!("Loading hodeco maps...");
    let query_hodeco_maps: HashMap<_, _> = query_hodeco_map_decoder
        .decode::<(String, Vec<usize>)>()
        .map(|result| match result {
            Ok(item) => item,
            Err(error) => panic!("Cannot read hodeco map: {error:?}"),
        })
        .collect();
    let target_hodeco_maps: HashMap<_, _> = target_hodeco_map_decoder
        .decode::<(String, Vec<usize>)>()
        .map(|result| match result {
            Ok(item) => item,
            Err(error) => panic!("Cannot read hodeco map: {error:?}"),
        })
        .collect();

    info!("Homopolymer decompressing...");
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
            let query_hodeco_maps = &query_hodeco_maps;
            let target_hodeco_maps = &target_hodeco_maps;
            let input_receiver = input_receiver.clone();
            let output_sender = output_sender.clone();
            scope
                .builder()
                .name(format!("compute_thread_{thread_id}"))
                .spawn(move |_| {
                    while let Ok(paf_line) = input_receiver.recv() {
                        let hodeco_paf_line =
                            hodeco_paf_line(paf_line, query_hodeco_maps, target_hodeco_maps);
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

    info!("Done");
}

fn hodeco_paf_line(
    mut hoco_paf: PAFLine,
    query_hodeco_maps: &HashMap<String, Vec<usize>>,
    target_hodeco_maps: &HashMap<String, Vec<usize>>,
) -> PAFLine {
    let query_hodeco_map = query_hodeco_maps
        .get(&hoco_paf.query_sequence_name)
        .unwrap_or_else(|| {
            panic!(
                "Query hodeco map not found: {}",
                hoco_paf.query_sequence_name
            )
        });
    let target_hodeco_map = target_hodeco_maps
        .get(&hoco_paf.target_sequence_name)
        .unwrap_or_else(|| {
            panic!(
                "Target hodeco map not found: {}",
                hoco_paf.target_sequence_name
            )
        });

    let hoco_query_start = hoco_paf.query_start_coordinate;
    let hoco_target_start = hoco_paf.target_start_coordinate_on_original_strand;
    let hoco_query_sequence_length = hoco_paf.query_sequence_length;

    assert_eq!(hoco_paf.query_sequence_length, query_hodeco_map.len() - 1);
    assert_eq!(hoco_paf.target_sequence_length, target_hodeco_map.len() - 1);
    hoco_paf.query_sequence_length = *query_hodeco_map.last().unwrap();
    hoco_paf.target_sequence_length = *target_hodeco_map.last().unwrap();

    hoco_paf.query_start_coordinate = query_hodeco_map[hoco_paf.query_start_coordinate];
    hoco_paf.query_end_coordinate = query_hodeco_map[hoco_paf.query_end_coordinate];
    hoco_paf.target_start_coordinate_on_original_strand =
        target_hodeco_map[hoco_paf.target_start_coordinate_on_original_strand];
    hoco_paf.target_end_coordinate_on_original_strand =
        target_hodeco_map[hoco_paf.target_end_coordinate_on_original_strand];
    assert!(hoco_paf.query_end_coordinate as isize - hoco_paf.query_start_coordinate as isize > 0);
    assert!(
        hoco_paf.target_end_coordinate_on_original_strand as isize
            - hoco_paf.target_start_coordinate_on_original_strand as isize
            > 0
    );

    if let Some(cigar_string) = &mut hoco_paf.cigar_string {
        let mut number_of_matching_bases = 0;
        let mut number_of_bases_and_gaps = 0;

        let mut query_offset = hoco_query_start;
        let mut target_offset = hoco_target_start;

        for cigar_column in &mut cigar_string.0 {
            match cigar_column {
                CigarColumn::Match(count) => {
                    let query_limit = query_offset + *count;
                    let target_limit = target_offset + *count;
                    let hodeco_count =
                        query_hodeco_map[query_limit] - query_hodeco_map[query_offset];
                    query_offset = query_limit;
                    target_offset = target_limit;
                    *count = hodeco_count;
                    number_of_matching_bases += *count;
                }
                CigarColumn::Deletion(count) => {
                    let target_limit = target_offset + *count;
                    let hodeco_count =
                        target_hodeco_map[target_limit] - target_hodeco_map[target_offset];
                    target_offset = target_limit;
                    *count = hodeco_count;
                }
                CigarColumn::Insertion(count) => {
                    let query_limit = query_offset + *count;
                    let hodeco_count =
                        query_hodeco_map[query_limit] - query_hodeco_map[query_offset];
                    query_offset = query_limit;
                    *count = hodeco_count;
                }
                CigarColumn::Mismatch(_) => panic!("Mismatch not supported in CIGAR"),
            }

            match cigar_column {
                CigarColumn::Match(count)
                | CigarColumn::Deletion(count)
                | CigarColumn::Insertion(count)
                | CigarColumn::Mismatch(count) => number_of_bases_and_gaps += *count,
            }
        }

        hoco_paf.number_of_matching_bases = number_of_matching_bases;
        hoco_paf.number_of_bases_and_gaps = number_of_bases_and_gaps;
    }

    if let Some(difference_string) = &mut hoco_paf.difference_string {
        let mut total_number_of_mismatches_and_gaps = 0;

        let mut query_offset = hoco_query_start;
        let mut target_offset = hoco_target_start;
        let mut mismatch_insertion = Vec::new();

        for (index, difference_column) in difference_string.0.iter_mut().enumerate() {
            match difference_column {
                DifferenceColumn::Match { length } => {
                    let query_limit = query_offset + *length;
                    let target_limit = target_offset + *length;
                    let hodeco_count =
                        query_hodeco_map[query_limit] - query_hodeco_map[query_offset];
                    query_offset = query_limit;
                    target_offset = target_limit;
                    *length = hodeco_count;
                }
                DifferenceColumn::Deletion {
                    missing_query_characters,
                } => {
                    let target_limit = target_offset + missing_query_characters.len();
                    *missing_query_characters = homopolymer_decompress_string(
                        missing_query_characters,
                        &target_hodeco_map[target_offset..target_limit + 1],
                    );
                    target_offset = target_limit;
                    total_number_of_mismatches_and_gaps += missing_query_characters.len();
                }
                DifferenceColumn::Insertion {
                    superfluous_query_characters,
                } => {
                    let query_limit = query_offset + superfluous_query_characters.len();
                    *superfluous_query_characters = homopolymer_decompress_string(
                        superfluous_query_characters,
                        &query_hodeco_map[query_offset..query_limit + 1],
                    );
                    query_offset = query_limit;
                    total_number_of_mismatches_and_gaps += superfluous_query_characters.len();
                }
                DifferenceColumn::Mismatch { reference, query } => {
                    let query_limit = query_offset + 1;
                    let target_limit = target_offset + 1;
                    let hodeco_count =
                        query_hodeco_map[query_limit] - query_hodeco_map[query_offset] - 1;
                    query_offset = query_limit;
                    target_offset = target_limit;
                    mismatch_insertion.push((index, hodeco_count, *reference, *query));
                    total_number_of_mismatches_and_gaps += hodeco_count;
                }
            }
        }

        for (index, hodeco_count, reference, query) in mismatch_insertion.into_iter().rev() {
            for _ in 0..hodeco_count {
                difference_string
                    .0
                    .insert(index, DifferenceColumn::Mismatch { reference, query });
            }
        }

        hoco_paf.total_number_of_mismatches_and_gaps = Some(total_number_of_mismatches_and_gaps);
    }

    if let Some(approximate_per_base_sequence_divergence) =
        &mut hoco_paf.approximate_per_base_sequence_divergence
    {
        *approximate_per_base_sequence_divergence *=
            hoco_paf.query_sequence_length as f64 / hoco_query_sequence_length as f64;
    }
    if let Some(gap_compressed_per_base_sequence_divergence) =
        &mut hoco_paf.gap_compressed_per_base_sequence_divergence
    {
        *gap_compressed_per_base_sequence_divergence *=
            hoco_paf.query_sequence_length as f64 / hoco_query_sequence_length as f64;
    }

    hoco_paf
}

fn homopolymer_decompress_string(input: &str, hodeco_map: &[usize]) -> String {
    let mut result = String::new();
    for (index, character) in input.chars().enumerate() {
        let count = hodeco_map[index + 1] - hodeco_map[index];
        for _ in 0..count {
            result.push(character);
        }
    }
    result
}
