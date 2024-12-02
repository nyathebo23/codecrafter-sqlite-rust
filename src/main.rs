use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;
            #[allow(unused_variables)]
            let page_size = u16::from_be_bytes([header[16], header[17]]);

            let mut header_page = [100; 112];
            file.read_exact(&mut header_page)?;
            #[allow(unused_variables)]
            let cells_count = u16::from_be_bytes([header_page[3], header_page[4]]);
            // You can use print statements as follows for debugging, they'll be visible when running tests.
            eprintln!("Logs from your program will appear here!");

            println!("database page size: {}", page_size);

            println!("number of tables: {}", cells_count);


        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
