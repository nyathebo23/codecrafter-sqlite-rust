
use std::fs::File;
use std::io::Read;
use std::io::BufReader;
use crate::utils_func::result_on_iter_num;
use crate::utils_func::varint_val;

pub fn database_tables_names(dbfile: File){
    let mut reader = BufReader::new(dbfile);

    let mut pages_data = Vec::new();

    let _ = reader.read_to_end(&mut pages_data); 

    let cells_count = u16::from_be_bytes([pages_data[103], pages_data[104]]);
    let cells_num_size = (cells_count * 2) as usize;
    process_cells_content_areas(&pages_data, cells_num_size, 108);
}

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