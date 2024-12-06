use std::fs::File;
use std::io::Read;
use std::io::BufReader;
use crate::utils_func::result_on_iter_num;
use crate::utils_func::varint_val;

pub fn select_count(dbfile: File, table_name: String){
    let mut reader = BufReader::new(dbfile);
    let mut pages_data = Vec::new();

    let _ = reader.read_to_end(&mut pages_data); 
    let cells_count = u16::from_be_bytes([pages_data[103], pages_data[104]]);
    let cells_num_size = (cells_count * 2) as usize;
    let page_size = u16::from_be_bytes([pages_data[16], pages_data[17]]);
    let page_data = page_data(&mut pages_data, table_name, page_size, cells_num_size);
    let rows_count =  count_table_rows(page_data);
    println!("{}", rows_count);
}

fn page_data(pages_datas: &mut Vec<u8>, table_name: String, pagesize: u16, cells_num_size: usize) -> Vec<&u8> {
    let mut count: usize = 0;
    let (mut tbl_name, mut rootpage) =  (String::from(""), 0);
    while count < cells_num_size && tbl_name != table_name {
        let ind_offset_array = 108 + count;
        let offset = u16::from_be_bytes([pages_datas[ind_offset_array], pages_datas[ind_offset_array+1]]);
        let mut start_area_iter = pages_datas.iter().skip(offset as usize);
        (tbl_name, rootpage) = table_name_and_page(&mut start_area_iter);
        count += 2;
    }
    if tbl_name == table_name {
        let vec: Vec<&u8> = Vec::new();
        return vec;
    }
    let page_data: Vec<&u8> = pages_datas.iter().skip((pagesize as usize) * ((rootpage - 1) as usize)).
    take(pagesize as usize).collect();
    page_data
}

fn table_name_and_page<'a> (cell_data: &mut impl Iterator <Item = &'a u8>) -> (String, u8) {
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
    let rootpage = result_on_iter_num(cell_data);
    (table_name, rootpage)
}

fn count_table_rows (page: Vec<&u8>) -> u16 {
    if page.is_empty() {
        return  0;
    }
    let cells_count = u16::from_be_bytes([*page[3], *page[4]]);
    cells_count 
}