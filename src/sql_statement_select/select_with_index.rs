use std::collections::HashMap;
use std::io::Read;
use std::fs::File;
use std::io::BufReader;

use crate::utils_func::{page_data, varint_val};

use super::parser_utils::{CompOperand, CondExpression, SelectStmtData, TableHeadDesc};
use super::select_columns::{display_query_result, page_data_and_table_columns};
use super::select_query_result::{consume_data, ColumnValue, ColumnValueType};


pub fn select_with_index(dbfile: &File, select_stmt: &SelectStmtData){
    let mut reader = BufReader::new(dbfile);
    let mut all_pages = Vec::new();

    let _ = reader.read_to_end(&mut all_pages); 
    let cells_count: u16 = u16::from_be_bytes([all_pages[103], all_pages[104]]);

    let cells_num_size = (cells_count * 2) as usize;
    let table_page_size = u16::from_be_bytes([all_pages[16], all_pages[17]]);
    #[allow(unused_variables)]
    let (page, table_head_desc) = page_data_and_table_columns(
        &mut all_pages, select_stmt.table_name.clone(), table_page_size, cells_num_size
    );

    let table_head = rearrange_columns(table_head_desc, String::from("country"));
    let page_data = page_data(&mut all_pages, String::from("idx_companies_country"),
     table_page_size, cells_num_size);
    let result =  query_result_on_index(page_data, table_page_size as usize,  &all_pages,
        select_stmt, &table_head);
    display_query_result(result);
}

pub fn rearrange_columns(table_header: TableHeadDesc, col_index_name: String) -> TableHeadDesc {
    let columns = table_header.columns_names;
    let mut ind = 0;
    let columns_count = columns.len();
    let mut new_columns_array = vec![col_index_name.clone()];
    while ind < columns_count && columns[ind] != col_index_name {
        new_columns_array.push(columns[ind].clone());
        ind += 1;
    } 
    ind += 1;
    while ind < columns_count {
        new_columns_array.push(columns[ind].clone());
        ind += 1;
    }
    TableHeadDesc {
        rowid_column_name: table_header.rowid_column_name,
        columns_names: new_columns_array
    }
}

pub fn query_result_on_index(table_page_data: Vec<u8>, page_size: usize, all_pages: &Vec<u8>, 
    select_stmt: &SelectStmtData, table_columns: &TableHeadDesc) -> Vec<Vec<ColumnValue>> {
    let page_type = table_page_data[0];
    
    let query_result = match page_type {
        2 => query_interior_page(table_page_data, page_size, all_pages, select_stmt, table_columns),
        10 => query_leaf_page(table_page_data, select_stmt, table_columns),
        _ => {panic!("error in header");}
    };
    query_result
}

fn query_interior_page(table_page_data: Vec<u8>, page_size: usize, all_pages: &Vec<u8>, 
    select_stmt: &SelectStmtData, table_columns: &TableHeadDesc) -> Vec<Vec<ColumnValue>> {
        let mut query_datas: Vec<Vec<ColumnValue>> = Vec::new();
        let cells_num_size = (u16::from_be_bytes([table_page_data[3], table_page_data[4]]) * 2) as usize;
        let mut count = 0;
        while count < cells_num_size {
            let query_result = query_page_on_node(&table_page_data, page_size, 
                all_pages, select_stmt, table_columns, count); 
            if !query_result.is_empty() {
                query_datas.extend(query_result);
                break;
            }
            count += 2;
        }
        while count < cells_num_size {
            let query_result = query_page_on_node(&table_page_data, page_size, 
                all_pages, select_stmt, table_columns, count); 
            if query_result.is_empty() {break;}
            query_datas.extend(query_result); 
            count += 2;
        }
        query_datas
}

fn query_page_on_node(table_page_data: &Vec<u8>, page_size: usize, all_pages: &Vec<u8>, 
    select_stmt: &SelectStmtData, table_columns: &TableHeadDesc, count: usize) -> Vec<Vec<ColumnValue>> {
    let mut query_result = Vec::new();
    let offset = u16::from_be_bytes([table_page_data[12+count], table_page_data[count+13]]);
    let mut content_offset_area = table_page_data.iter().skip(offset as usize);
    let page_pointer_vec: Vec<u8> = content_offset_area.by_ref().take(4).cloned().collect();
    varint_val(&mut content_offset_area);
    let row_result = row_statement_index_result(&mut content_offset_area,
        table_columns.clone(), select_stmt, column_index_value(select_stmt.condition.clone()));
    println!("{:?}", row_result);
    if !row_result.is_empty() {
        query_result.push(row_result);
    }
    let page_num = u32::from_be_bytes([page_pointer_vec[0], page_pointer_vec[1], page_pointer_vec[2], page_pointer_vec[3]]);
    let page_data: Vec<u8> = all_pages.iter().skip((page_size as usize) * ((page_num - 1) as usize)).
    take(page_size as usize).cloned().collect();
    query_result.extend( query_result_on_index(page_data, page_size, all_pages, select_stmt, table_columns));
    query_result
}

fn query_leaf_page(table_page_data: Vec<u8>, select_stmt: &SelectStmtData, table_columns: &TableHeadDesc)
    -> Vec<Vec<ColumnValue>> {
    let mut query_datas: Vec<Vec<ColumnValue>> = Vec::new();
    let cells_count = (u16::from_be_bytes([table_page_data[3], table_page_data[4]]) * 2) as usize;
    let mut count: usize = 0;
    while count < cells_count {
        let offset = u16::from_be_bytes([table_page_data[8 + count], table_page_data[count+9]]);

        let mut content_offset_area = table_page_data.iter().skip(offset as usize);
        varint_val(&mut content_offset_area);

        let row_result = row_statement_index_result(&mut content_offset_area,
            table_columns.clone(), select_stmt, column_index_value(select_stmt.condition.clone()));
        
        if !row_result.is_empty() { 
            query_datas.push(row_result); 
        }
        count += 2;
    }
    query_datas
}

fn row_statement_index_result<'a, T>(cell_datas: &mut T, table_head: TableHeadDesc, select_stmt: &SelectStmtData, index_val: ColumnValue) 
-> Vec<ColumnValue> where T : Iterator <Item = &'a u8>  {
    let cell_datas_head: Vec<u8> = cell_datas.cloned().collect();
    let mut cell_datas_head_iter = cell_datas_head.iter();
    let mut cell_datas_body = cell_datas_head.iter();

    let header_size = varint_val(&mut cell_datas_head_iter); 

    cell_datas_body.nth(header_size - 1);
    let mut statement_result = Vec::new();
    let serial_type = varint_val(&mut cell_datas_head_iter);
    let col_data = consume_data(&mut cell_datas_body,  serial_type);
    if col_data.value != index_val.value {
        return statement_result;
    }
    let mut row_values = HashMap::new();
    row_values.insert(table_head.columns_names[0].clone(), index_val.clone());
    for table_col in table_head.columns_names.iter().skip(1) {
        let serial_type = varint_val(&mut cell_datas_head_iter);
        let col_data = consume_data(&mut cell_datas_body,  serial_type);
        row_values.insert(table_col.clone(), col_data);
    }
    for attr in select_stmt.columns.iter() {
        let column_value = match row_values.get(attr) {
            Some(val) => val,
            None => {panic!("field {} is unknow", attr);}
        }; 

        statement_result.push(column_value.clone());
    }
    
    statement_result
}
    
fn column_index_value(condition: CondExpression) -> ColumnValue {
    match condition {
        CondExpression::Comparison(comp_expr) => {
            match *comp_expr.right_operand {
                CondExpression::Literal(val) => {
                    match val {
                        CompOperand::Identifier(string) => panic!("error {}", string),
                        CompOperand::Str(string) => ColumnValue::new(ColumnValueType::Text, string),
                        CompOperand::Number(num) => ColumnValue::new(ColumnValueType::Real, num.to_string()),       
                    }
                },
                _ => panic!("error")
            }
        },
        _ => panic!("error")
    }
}