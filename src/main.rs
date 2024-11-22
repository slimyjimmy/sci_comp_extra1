// Djimon Nowak

use clap::Parser;
use ordered_float::NotNan;
use std::collections::{BTreeMap, HashMap};
use std::time::Instant;
use rustc_hash::FxHashMap;
use memmap2::Mmap;
use memchr::memchr;

#[derive(Parser, Debug)]
#[command(
    name = "sci_comp_extra1",
    version = "1.0",
    about = "Scientific Computing exercise extra 1"
)]
struct Args {
    #[arg(short = 'f', long, help = "Path to the measurement file")]
    file: String,
}

#[derive(Debug, Clone, PartialEq)]
struct StationValues {
    min: f64,
    max: f64,
    frequency: HashMap<NotNan<f64>, u64>,
    count: u64,
}

impl StationValues {
    fn new() -> Self {
        StationValues {
            min: 0.0,
            max: 0.0,
            count: 0,
            frequency: (-999..=999)
            .map(|x| (NotNan::new(round_off(x as f64 * 0.1)).unwrap(), 0)) // Convert to f64 and pair with 0
            .collect()
        }
    }

    fn new_with_value(value: f64) -> Self {
        let mut station_values = StationValues::new();
        station_values.min = value;
        station_values.max = value;
        *station_values.frequency.get_mut(&NotNan::new(value).expect("Value is NaN")).unwrap_or_else(|| panic!("Get mut failed with {}", value)) += 1;
        station_values.count = 1;
        station_values
    }

    // get 0-indexed value by index
    fn get_nth_value(&self, n: u64) -> f64 {
        let mut cur = 0;

        let keys: Vec<NotNan<f64>> = (-999..=999)
        .map(|x| NotNan::new(round_off(x as f64 * 0.1)).unwrap())
        .collect();

        for key in keys {
            let count = self.frequency[&key];
            if n < cur + count {
                return key.into_inner();
            }
            cur += count;
        }

        0.0
    }

    fn get_median(&self) -> f64 {
        if self.count % 2 == 0 {
            // even number of values -> return 1/2(left-middle + right-middle)
            let left_mid_index = (self.count / 2) - 1;
            (self.get_nth_value(left_mid_index) + self.get_nth_value(left_mid_index + 1)) / 2.0
        } else {
            // odd number of values -> return middle
            self.get_nth_value(self.count / 2)
        }
    }
}

// Calculate the station values
fn calculate_station_values(data:&[u8]) -> FxHashMap<&[u8], StationValues> {
    let mut result: FxHashMap<&[u8], StationValues> = FxHashMap::default();
    let  mut buffer = data;
    loop {
        match memchr(b';', buffer) {
            None => {
                break;
            }
            Some(comma_seperator) => {
                let end = memchr(b'\n', &buffer[comma_seperator..]).unwrap();
                let name = &buffer[..comma_seperator];
                let value = &buffer[comma_seperator+1..comma_seperator+end];
                let value = fast_float::parse(value).expect("Failed to parse value");

                result
                    .entry(name)
                    .and_modify(|e| {
                        if value < e.min {
                            e.min = value;
                        }
                        if value > e.max {
                            e.max = value;
                        }
                        *e.frequency.get_mut(&NotNan::new(value).unwrap()).unwrap() += 1;
                        e.count += 1;
                    })
                    .or_insert(StationValues::new_with_value(value));
                buffer = &buffer[comma_seperator+end+1..];
            }

        }
    }


    // Calculate the mean for all entries and round off to 1 decimal place
    for (_, station_values) in result.iter_mut() {
        station_values.min = round_off(station_values.min);
        station_values.max = round_off(station_values.max);
    }

    result
}

fn round_off(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

fn write_result_stdout(result: FxHashMap<&[u8], StationValues>) {
    let mut ordered_result = BTreeMap::new();
    for (station_name, station_values) in result {
        ordered_result.insert(station_name, station_values);
    }
    let mut iterator = ordered_result.iter().peekable();
    print!("{{");
    while let Some((station_name, station_values)) = iterator.next() {
        if iterator.peek().is_none() {
            print!(
                "{}={:.1}/{:.1}/{:.1}}}",
                std::str::from_utf8(station_name).expect("Unable to validate station name as UTF-8"), station_values.min, station_values.get_median(), station_values.max
            );
        } else {
            print!(
                "{}={:.1}/{:.1}/{:.1}, ",
                std::str::from_utf8(station_name).expect("Unable to validate station name as UTF-8"), station_values.min, station_values.get_median(), station_values.max
            );
        }
    }
}

fn main() {
    let start = Instant::now();
    let args = Args::parse();

    let file = std::fs::File::open(&args.file).expect("Failed to open file");
    let mmap = unsafe { Mmap::map(&file).expect("Failed to map file") };
    let data = &*mmap;

    let result = calculate_station_values(data);
    write_result_stdout(result);
    let duration = start.elapsed();
    println!("\nTime taken is: {:?}", duration);

}
