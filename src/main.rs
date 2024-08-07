use chrono::{Local};
use std::fs;
use std::path::Path;
use std::time::{Instant, Duration};
use std::sync::mpsc;
use std::thread;
use csv::ReaderBuilder;
use std::env;

struct DataLoader {
    folder: String,
}

impl DataLoader {
    fn new(folder: String) -> Self {
        DataLoader { folder }
    }

    fn load_files(&self, concurrent: bool, multi_core: bool) {
        let program_start = Instant::now();
        let start_time = Local::now().format("%Y-%m-%d %H:%M:%S%.f").to_string();
        println!("Hora de inicio del programa: {}", start_time);

        let paths = self.get_csv_files();
        let first_load = Local::now().format("%Y-%m-%d %H:%M:%S%.f").to_string();
        println!("Hora de la carga del primer archivo: {}", first_load);

        if concurrent {
            if multi_core {
                self.load_files_multicore(paths)
            } else {
                self.load_files_concurrently(paths)
            }
        } else {
            self.load_files_sequentially(paths)
        };

        let end_time = Local::now().format("%Y-%m-%d %H:%M:%S%.f").to_string();
        println!("Hora de finalización del programa: {}", end_time);
        let total_process_time = program_start.elapsed();
        println!("Tiempo total del proceso: {:.4} ms", total_process_time.as_millis());
    }

    fn get_csv_files(&self) -> Vec<String> {
        let paths = fs::read_dir(&self.folder).unwrap();
        paths
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().map_or(false, |e| e == "csv"))
            .map(|entry| entry.path().display().to_string())
            .collect()
    }

    fn load_files_sequentially(&self, paths: Vec<String>) -> Duration {
        let mut total_duration = Duration::new(0, 0);
        for path in &paths {
            let file_start_time = Instant::now();
            DataLoader::load_file(path);
            let file_duration = file_start_time.elapsed();
            total_duration += file_duration;
            println!("Duración de la carga del archivo {}: {:?}", Path::new(path).file_name().unwrap().to_string_lossy(), file_duration);
        }
        let last_load = Local::now().format("%Y-%m-%d %H:%M:%S%.f").to_string();
        println!("Hora de finalizacion de la carga del ultimo archivo: {}", last_load);
        total_duration
    }

    fn load_files_concurrently(&self, paths: Vec<String>) -> Duration {
        let (tx, rx) = mpsc::channel();
        for path in paths {
            let tx = tx.clone();
            thread::spawn(move || {
                let file_start_time = Instant::now();
                let result = DataLoader::load_file(&path);
                let file_duration = file_start_time.elapsed();
                tx.send((path, file_duration)).unwrap();
                result
            });
        }

        drop(tx);
        let mut total_duration = Duration::new(0, 0);
        for (path, duration) in rx {
            total_duration += duration;
            println!("Archivo: {} cargado en {:.3} ms", Path::new(&path).file_name().unwrap().to_string_lossy(), duration.as_secs_f64() * 100.0);
        }
        total_duration
    }

    fn load_files_multicore(&self, paths: Vec<String>) -> Duration {
        let (tx, rx) = mpsc::channel();
        let num_threads = num_cpus::get();
        let chunk_size = (paths.len() + num_threads - 1) / num_threads;

        for chunk in paths.chunks(chunk_size) {
            let chunk: Vec<String> = chunk.to_vec();
            let tx = tx.clone();
            thread::spawn(move || {
                for path in chunk {
                    let file_start_time = Instant::now();
                    let result = DataLoader::load_file(&path);
                    let file_duration = file_start_time.elapsed();
                    tx.send((path, file_duration)).unwrap();
                    result
                }
            });
        }

        drop(tx);
        let mut total_duration = Duration::new(0, 0);
        for (path, duration) in rx {
            total_duration += duration;
            println!("Archivo: {} cargado en {:?}", Path::new(&path).file_name().unwrap().to_string_lossy(), duration);
        }
        total_duration
    }

    fn load_file(path: &str) {
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_path(path)
            .expect("Failed to open file");
        for result in rdr.records() {
            let _record = result.expect("Failed to read record");
            // Aquí procesar el contenido del archivo
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let folder_index = args.iter().position(|arg| arg == "-f").expect("Folder not specified");
    let folder = &args[folder_index + 1];
    let concurrent = args.contains(&"-s".to_string());
    let multi_core = args.contains(&"-m".to_string());

    let data_loader = DataLoader::new(folder.to_string());
    data_loader.load_files(concurrent, multi_core);
}
