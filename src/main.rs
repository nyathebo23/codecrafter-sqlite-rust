use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

fn process_cells_content_areas(page_data: &Vec<u8>, cells_count: usize, start_page_offset: usize) {
    let mut count: usize = 0;
    while count < cells_count {
        let ind_offset_array = start_page_offset + count;
        let offset = u16::from_be_bytes([page_data[ind_offset_array], page_data[ind_offset_array+1]]);
        let mut start_area_iter = page_data.iter().skip(offset as usize);
        let tbl_name = table_name(&mut start_area_iter);
        print!("{} ", tbl_name);
        count += 2;
    }
}


fn varint_val<'a> (cell_data: &mut impl Iterator <Item = &'a u8>) -> (u64, u8) {
    let mut cell_val = result_on_iter_num(cell_data);
    let mut first_bit = cell_val >> 7;
    let mut varint_num = (cell_val & 127) as u64;
    let mut bits_count: u8 = 7;
    while first_bit == 1 && bits_count < 63{
        cell_val = result_on_iter_num(cell_data);
        varint_num = varint_num << 7 | (cell_val & 127) as u64;
        first_bit = cell_val >> 7;
        bits_count += 7;
    }
    (varint_num, bits_count)
}

fn result_on_iter_num<'a> (iter_data: &mut impl Iterator <Item = &'a u8>) -> u8 {
    match iter_data.nth(0) {
        Some(i) => *i,
        None => 0,
    }
}

fn table_name<'a> (cell_data: &mut impl Iterator <Item = &'a u8>) -> String {
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
            let file = File::open(&args[1])?;
            let mut reader = BufReader::new(file);

            let mut pages_data = Vec::new();

            reader.read_to_end(&mut pages_data)?; 

            let cells_count = u16::from_be_bytes([pages_data[103], pages_data[104]]);
            let cells_num_size = (cells_count * 2) as usize;
            process_cells_content_areas(&pages_data, cells_num_size, 108);
    
        },
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
