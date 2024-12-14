use std::fs::File;
use std::io::Read;
use std::io::BufReader;
use crate::utils_func::table_schema_infos;
use crate::utils_func::SchemaInfos;
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
    let table_page_size = u16::from_be_bytes([all_pages[16], all_pages[17]]) as usize;
    let (page, table_head_desc) = page_data_and_table_columns(
        &mut all_pages, select_stmt.table_name.clone(), table_page_size, cells_num_size
    );
 
    let result =  query_result_array(page, table_page_size as usize,  &all_pages,
        select_stmt, &table_head_desc);
    display_query_result(result);
}

pub fn page_data_and_table_columns(pages_datas: &Vec<u8>, tablename: String, pagesize: usize, cells_num_size: usize) -> 
    (Vec<u8>, TableHeadDesc) {
    let mut count: usize = 0;
    let mut table_schema_inf = SchemaInfos::new(String::from(""),
     String::from(""), String::from(""), 0, String::from(""));

    while count < cells_num_size && table_schema_inf.tbl_name != tablename {
        let offset = u16::from_be_bytes([pages_datas[108 + count], pages_datas[count+109]]);
        let mut start_area_iter: std::iter::Skip<std::slice::Iter<'_, u8>> = pages_datas.iter().skip(offset as usize);
        table_schema_inf = table_schema_infos(&mut start_area_iter);
        count += 2;
    }

    if table_schema_inf.tbl_name != tablename {
        let vec = Vec::new();
        return (vec, TableHeadDesc {columns_names: Vec::new(), rowid_column_name: None});
    }

    let table_head_desc = table_column_names(table_schema_inf.sql.as_str()).unwrap();
    
    let page_data: Vec<u8> = pages_datas.iter().skip((pagesize as usize) * ((table_schema_inf.rootpage - 1) as usize)).
    take(pagesize as usize).cloned().collect();
    (page_data, table_head_desc)

}

pub fn display_query_result(result: Vec<Vec<ColumnValue>>) {
    for item in result {
        print!("{}", item[0].value);
        for colvalue in item.iter().skip(1) {
            print!("|{}", colvalue.value);
        }
        println!();
    }

}
