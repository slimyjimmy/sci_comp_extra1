// Djimon Nowak

use clap::Parser;
use memchr::memchr;
use std::collections::{BTreeMap, HashMap};
use std::io::Read;

const READ_BUF_SIZE: usize = 128 * 1024; // 128 KiB

#[derive(Parser, Debug)]
#[command(
    name = "sci_comp_extra1",
    version = "1.3",
    about = "10 times the one billion row challenge with a twist"
)]
struct Args {
    file: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct StationValues {
    min: f64,
    max: f64,
    mean: f64,
    count: u32,
}

fn process_chunk(data: &[u8], result: &mut HashMap<Box<[u8]>, StationValues>) {
    let mut buffer = &data[..];

    loop {
        match memchr(b';', buffer) {
            None => {
                break;
            }
            Some(comma_seperator) => {
                let end = memchr(b'\n', &buffer[comma_seperator..]).unwrap();
                let name = &buffer[..comma_seperator];
                let value = &buffer[comma_seperator + 1..comma_seperator + end];
                let value: f64 = fast_float::parse(value).expect("Failed to parse value");

                result
                    .entry(name.into())
                    .and_modify(|e| {
                        if value < e.min {
                            e.min = value;
                        }
                        if value > e.max {
                            e.max = value;
                        }
                        e.mean += value;
                        e.count += 1;
                    })
                    .or_insert(StationValues {
                        min: value,
                        max: value,
                        mean: value,
                        count: 1,
                    });
                buffer = &buffer[comma_seperator + end + 1..];
            }
        }
    }

    // result
}

pub fn round_off(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

fn write_result_stdout(mut result: HashMap<Box<[u8]>, StationValues>) {
    let mut ordered_result = BTreeMap::new();
    for (station_name, station_values) in result.iter_mut() {
        ordered_result.insert(station_name, station_values);
    }
    let mut iterator = ordered_result.iter().peekable();
    print!("{{");
    while let Some((station_name, station_values)) = iterator.next() {
        if iterator.peek().is_none() {
            print!(
                "{}={:.1}/{:.1}/{:.1}}}",
                std::str::from_utf8(station_name)
                    .expect("Can't parse station name as UTF-8 string"),
                station_values.min,
                station_values.mean,
                station_values.max
            );
        } else {
            print!(
                "{}={:.1}/{:.1}/{:.1}, ",
                std::str::from_utf8(station_name)
                    .expect("Can't parse station name as UTF-8 string"),
                station_values.min,
                station_values.mean,
                station_values.max
            );
        }
    }
}

fn calculate_station_values(mut file: std::fs::File) -> HashMap<Box<[u8]>, StationValues> {
    // Start the processor threads
    let (sender, receiver) = crossbeam_channel::bounded::<Box<[u8]>>(1_000);
    let n_threads = std::thread::available_parallelism().unwrap().into();
    let mut handles = Vec::with_capacity(n_threads);
    for _ in 0..n_threads {
        let receiver = receiver.clone();
        let handle = std::thread::spawn(move || {
            let mut result = HashMap::<Box<[u8]>, StationValues>::default();
            // wait until the sender sends the chunk
            for buf in receiver {
                process_chunk(&buf, &mut result);
            }
            result
        });
        handles.push(handle);
    }

    // Read the file in chunks and send the chunks to the processor threads
    let mut buf = vec![0; READ_BUF_SIZE];
    let mut bytes_not_processed = 0;
    loop {
        let bytes_read = file.read(&mut buf[bytes_not_processed..]).expect("Failed to read file");
        if bytes_read == 0 {
            break;
        }

        let actual_buf = &mut buf[..bytes_not_processed+bytes_read];
        let last_new_line_index = match find_new_line_pos(actual_buf) {
            Some(index) => index,
            None => {
                println!("No new line found in the read buffer");
                bytes_not_processed += bytes_read;
                if bytes_not_processed == buf.len(){
                    panic!("No new line found in the read buffer");
                }
                continue; // try again, maybe we next read will have a newline
            }
        };

        let buf_boxed = Box::<[u8]>::from(&actual_buf[..(last_new_line_index + 1)]);
        sender.send(buf_boxed).expect("Failed to send buffer");

        actual_buf.copy_within(last_new_line_index+1.., 0);
        // You cannot use bytes_not_processed = bytes_read - last_new_line_index
        // - 1; because the buffer will contain unprocessed bytes from the
        // previous iteration and the new line index will be calculated from the
        // start of the buffer
        bytes_not_processed = actual_buf.len() - last_new_line_index - 1;
    }
    drop(sender);

    // Combine data from all threads
    let mut result = HashMap::<Box<[u8]>, StationValues>::default();
    for handle in handles {
        let map = handle.join().unwrap();
        for (station_name, station_values) in map.into_iter() {
            // dbg!(station_values);
            result
                .entry(station_name)
                .and_modify(|e| {
                    if station_values.min < e.min {
                        e.min = station_values.min;
                    }
                    if station_values.max > e.max {
                        e.max = station_values.max;
                    }
                    e.mean += station_values.mean;
                    e.count += station_values.count;
                })
                .or_insert(station_values);
        }
    }

    // Calculate the mean for all entries and round off to 1 decimal place
    for (_name, station_values) in result.iter_mut() {
        station_values.mean = round_off(station_values.mean / station_values.count as f64);
        station_values.min = round_off(station_values.min);
        station_values.max = round_off(station_values.max);
    }

    result
}

fn main() {
    // let start = Instant::now();
    let args = Args::parse();

    let file = std::fs::File::open(&args.file).expect("Failed to open file");
    let result = calculate_station_values(file);
    write_result_stdout(result);

    // let duration = start.elapsed();
    // println!("\nTime taken is: {:?}", duration);
}

fn find_new_line_pos(bytes: &[u8]) -> Option<usize> {
    // In this case (position is not far enough),
    // naive version is faster than bstr (memchr)
    bytes.iter().rposition(|&b| b == b'\n')
}
