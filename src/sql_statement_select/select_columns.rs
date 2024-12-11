use std::fs::File;
use std::io::Read;
use std::io::BufReader;
use crate::utils_func::{varint_val, result_on_iter_num, text_from_cell};
use super::parser::parse_statement::table_column_names;
use super::parser_utils::TableHeadDesc;
use super::select_query_result::query_result_array;
use super::parser_utils::SelectStmtData;
use super::select_query_result::ColumnValue;

pub fn select_columns(dbfile: &File, select_stmt: &SelectStmtData){
    let mut reader = BufReader::new(dbfile);
    let mut all_pages = Vec::new();

    let _ = reader.read_to_end(&mut all_pages); 
    let cells_count = u16::from_be_bytes([all_pages[103], all_pages[104]]);

    let cells_num_size = (cells_count * 2) as usize;
    let table_page_size = u16::from_be_bytes([all_pages[16], all_pages[17]]);
    let (page, table_head_desc) = page_data_and_table_columns(
        &mut all_pages, select_stmt.table_name.clone(), table_page_size, cells_num_size
    );
 
    let result =  query_result_array(page, table_page_size as usize,  &all_pages,
        select_stmt, &table_head_desc);
    display_query_result(result);
}

pub fn page_data_and_table_columns(pages_datas: &mut Vec<u8>, tablename: String, pagesize: u16, cells_num_size: usize) -> 
    (Vec<u8>, TableHeadDesc) {
    let mut count: usize = 0;
    let (mut tbl_name, mut rootpage, mut sql_text_size) = (String::from(""), 0, 0);
    let mut start_area_iter = pages_datas.iter().skip(0);
    while count < cells_num_size && tbl_name != tablename {
        let offset = u16::from_be_bytes([pages_datas[108 + count], pages_datas[count+109]]);
        start_area_iter = pages_datas.iter().skip(offset as usize);
        (tbl_name, rootpage, sql_text_size) = table_schema_infos(&mut start_area_iter);
        count += 2;
    }
    if tbl_name != tablename {
        let vec = Vec::new();
        return (vec, TableHeadDesc {columns_names: Vec::new(), rowid_column_name: None});
    }

    let sql = text_from_cell(&mut start_area_iter, sql_text_size);

    let table_head_desc = table_column_names(sql.as_str()).unwrap();
    
    let page_data: Vec<u8> = pages_datas.iter().skip((pagesize as usize) * ((rootpage - 1) as usize)).
    take(pagesize as usize).cloned().collect();
    (page_data, table_head_desc)

}


fn table_schema_infos<'a, T> (cell_datas: &mut T) -> (String, usize, usize) 
where T : Iterator <Item = &'a u8> {
    let _record_size = varint_val(cell_datas);
    let _rowid = varint_val(cell_datas);
    #[allow(unused_variables)]
    let header_size = result_on_iter_num(cell_datas);
    let table_type_size = (result_on_iter_num(cell_datas) - 13)/2;
    let table_name0_size = (result_on_iter_num(cell_datas) - 13)/2;
    let table_name_size = (result_on_iter_num(cell_datas) - 13)/2;
    cell_datas.nth(0);
    let sql_text_size = varint_val(cell_datas);

    cell_datas.nth((table_type_size + table_name0_size - 1) as usize);

    let table_name= text_from_cell(cell_datas, table_name_size as usize);
    let rootpage = varint_val(cell_datas);
    
    (table_name, rootpage, (sql_text_size - 13)/2)
}


fn display_query_result(result: Vec<Vec<ColumnValue>>) {
    for item in result {
        print!("{}", item[0].value);
        for colvalue in item.iter().skip(1) {
            print!("|{}", colvalue.value);
        }
        println!();
    }

}
