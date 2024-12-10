use std::fs::File;
use std::io::Read;
use std::io::BufReader;
use crate::sql_statement_select::parser_utils::CondExpression;
use crate::utils_func::page_data;

use super::parser_utils::SelectStmtData;
use super::select_columns::page_data_and_table_columns;
use super::select_query_result::query_result_array;

pub fn select_count(dbfile: &File, select_stmt: &SelectStmtData){
    let mut reader = BufReader::new(dbfile);
    let mut pages_data = Vec::new();

    let _ = reader.read_to_end(&mut pages_data); 
    let cells_count = u16::from_be_bytes([pages_data[103], pages_data[104]]);
    let cells_num_size = (cells_count * 2) as usize;
    let page_size = u16::from_be_bytes([pages_data[16], pages_data[17]]);
    if select_stmt.condition == CondExpression::Null {
        let page_data = page_data(&mut pages_data, select_stmt.table_name.clone(), page_size, cells_num_size);
        let rows_count =  count_table_rows(page_data);
        println!("{}", rows_count);
    }
    else {
        let (page, columns) = page_data_and_table_columns(
            &mut pages_data, select_stmt.table_name.clone(), page_size, cells_num_size
        );
        let table_page_data: Vec<u8> = page.iter().map(|data| **data).collect();

        let rows_count =  count_table_rows_with_condition(table_page_data, columns, select_stmt);
        println!("{}", rows_count);
    }
}

pub fn count_table_rows (page: Vec<&u8>) -> u16 {
    if page.is_empty() {
        return  0;
    }
    let cells_count = u16::from_be_bytes([*page[3], *page[4]]);
    cells_count 
}

pub fn count_table_rows_with_condition(page: Vec<u8>, columns: Vec<String>, select_stmt: &SelectStmtData) -> usize {
    let cells_count = u16::from_be_bytes([page[3], page[4]]);
    let cols = vec![columns[0].clone()];
    let select_stmt_data = SelectStmtData {
        columns: cols,
        table_name: select_stmt.table_name.clone(),
        condition: select_stmt.condition.clone()
    };
    let result =  query_result_array(page, cells_count as usize, 
        &select_stmt_data, &columns);
    result.len()
}