
use std::fs::File;
use std::io::Read;
use std::io::BufReader;
use crate::utils_func::table_name;

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
