use anyhow::{bail, Result};
use core::slice::Iter;

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

fn offset_cells_list(page_data: &Vec<u8>, cells_count: usize) -> Vec<u8> {
    let mut offset_cells_array: Vec<u8> = Vec::new();
    let mut count: usize = 0;
    while count < cells_count {
        offset_cells_array.push(page_data[12+count]);
        count += 1;
    }
    offset_cells_array
}


fn varint_val(cell_data: &mut Iter<'_, u8>) -> (u64, u8) {
    let mut cell_val = result_on_iter_num(cell_data);
    let mut first_bit = cell_val >> 7;
    let mut varint_num = (cell_val & 127) as u64;
    let mut bits_count: u8 = 7;
    while first_bit == 1 && bits_count < 63{
        cell_val = match cell_data.nth(0) {
            Some(i) => *i,
            None => 0,
        };
        varint_num = varint_num << 7 | (cell_val & 127) as u64;
        first_bit = cell_val >> 7;
        bits_count += 1;
    }
    (varint_num, bits_count)
}

fn result_on_iter_num(iter_data: &mut Iter<'_, u8>) -> u8 {
    match iter_data.nth(0) {
        Some(i) => *i,
        None => 0,
    }
}

fn table_name(cell_data: &mut Iter<'_, u8>) -> String {
    let _record_size = varint_val(cell_data).0;
    let _rowid = varint_val(cell_data).0;
    let header_size = result_on_iter_num(cell_data);
    let table_type_size = (result_on_iter_num(cell_data) - 13)/2;
    let table_name0_size = (result_on_iter_num(cell_data) - 13)/2;
    let table_name_size = (result_on_iter_num(cell_data) - 13)/2;
    cell_data.nth((header_size - 5) as usize);
    cell_data.nth((table_type_size + table_name0_size - 1) as usize);

    let mut table_name = String::new();
    let mut ind: u8 = 0;
    while ind < table_name_size {
        let byte = result_on_iter_num(cell_data);
        table_name.push(byte as char);
        ind += 1;
    }
    table_name
}

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
            let mut header_and_header_page = [0; 112];
            file.read_exact(&mut header_and_header_page)?;
            #[allow(unused_variables)]
            let page_size = u16::from_be_bytes([header_and_header_page[16], header_and_header_page[17]]);

            file.read_exact(&mut header_and_header_page)?;
            #[allow(unused_variables)]
            let cells_count = u16::from_be_bytes([header_and_header_page[103], header_and_header_page[104]]);
            // You can use print statements as follows for debugging, they'll be visible when running tests.
            eprintln!("Logs from your program will appear here!");

            println!("database page size: {}", page_size);

            println!("number of tables: {}", cells_count);
        },
        ".tables" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            let page_size = u16::from_be_bytes([header[16], header[17]]);

            let mut reader = BufReader::with_capacity(page_size as usize, file);
            let mut page_data = Vec::new();
            let size = reader.read_to_end(&mut page_data);  
            match size {
                Ok(size) => println!("size {} pagesize {}", size, page_size),
                Err(error) => panic!("error copy file content {}", error)
            }
            let cells_count = u16::from_be_bytes([page_data[3], page_data[4]]);
            let cells_num_size = (cells_count * 2) as usize;
            let offset_cells_array = offset_cells_list(&page_data, cells_count as usize);
            println!("{} ", cells_count);
            let mut cells_num = 0;
            let mut offset = u16::from_be_bytes([offset_cells_array[cells_num], offset_cells_array[cells_num+1]]) as usize;
            let mut cell_data: Iter<'_, u8> = page_data.iter();
            cell_data.nth(offset - 1);
            let tbl_name =  table_name(&mut cell_data);
            println!("{} ", tbl_name);
            cells_num += 2;
            while cells_num != cells_num_size {
                offset = u16::from_be_bytes([offset_cells_array[cells_num], offset_cells_array[cells_num+1]]) as usize;
                cell_data.nth(offset - 1);
                let tbl_name =  table_name(&mut cell_data);
                println!("{} ", tbl_name);
                cells_num += 2;
            }
        },
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
