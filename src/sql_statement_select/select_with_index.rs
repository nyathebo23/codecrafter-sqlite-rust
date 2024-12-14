use std::io::Read;
use std::fs::File;
use std::io::BufReader;
use std::collections::VecDeque;
use crate::utils_func::{page_data, varint_val};

use super::parser_utils::{CompOperand, CondExpression, SelectStmtData, TableHeadDesc};
use super::select_columns::{display_query_result, page_data_and_table_columns};
use super::select_query_result::{consume_data, row_statement_result, ColumnValue};


pub fn select_with_index(dbfile: &File, select_stmt: &SelectStmtData){
    let mut reader = BufReader::new(dbfile);
    let mut all_pages = Vec::new();

    let _ = reader.read_to_end(&mut all_pages); 
    let cells_count = u16::from_be_bytes([all_pages[103], all_pages[104]]) as usize;

    let cells_num_size = cells_count * 2;
    let table_page_size = u16::from_be_bytes([all_pages[16], all_pages[17]]) as usize;

    let (page, table_head_desc) = page_data_and_table_columns(
        &all_pages, select_stmt.table_name.clone(), table_page_size, cells_num_size
    );

    let page_data = page_data(&all_pages, String::from("idx_companies_country"),
     table_page_size, cells_num_size);
    let key_index_val = column_index_value(select_stmt.condition.clone());
    let mut rowid_array = VecDeque::from(query_result_on_index(page_data, table_page_size, 
        &all_pages, &key_index_val));
    let select_stment = SelectStmtData{
        columns: select_stmt.columns.clone(),
        table_name: select_stmt.table_name.clone(),
        condition: CondExpression::Null
    };
    println!("{:?}", rowid_array);

    let result = query_on_table(page, table_page_size, &all_pages, 
        &select_stment, &table_head_desc, &mut rowid_array);
    display_query_result(result);
}



pub fn query_result_on_index(table_page_data: Vec<u8>, page_size: usize, all_pages: &Vec<u8>, keyindex_value: &String) -> Vec<i64> {
    let page_type = table_page_data[0];

    let query_result = match page_type {
        2 => query_interior_index_page(table_page_data, page_size, all_pages, keyindex_value),
        10 => query_leaf_index_page(table_page_data, keyindex_value),
        _ => {
            println!("{}", page_type);
            panic!("error in header");
        }
    };
    query_result
}

fn query_interior_index_page(table_page_data: Vec<u8>, page_size: usize, all_pages: &Vec<u8>, keyindex_value: &String) -> Vec<i64> {
        let mut query_rowid: Vec<i64> = Vec::new();
        let cells_num_size = u16::from_be_bytes([table_page_data[3], table_page_data[4]]) as usize * 2;

        let mut count = 0;
        while count < cells_num_size {
            let query_result = query_on_node(&table_page_data, page_size, all_pages, keyindex_value, count);
            if query_result.len() != 0 {break;}
            query_rowid.extend(query_result);
            count += 2;
        }
        while count < cells_num_size {
            let query_result = query_on_node(&table_page_data, page_size, all_pages, keyindex_value, count);
            if query_result.len() == 0 {break;}
            query_rowid.extend(query_result);
            count += 2;
        }
        if count == cells_num_size {
            let page_num = u32::from_be_bytes([table_page_data[8], table_page_data[9], table_page_data[10], table_page_data[11]]) as usize;
            let page_data: Vec<u8> = all_pages.iter().skip(page_size * (page_num - 1)).
            take(page_size).cloned().collect();
        
            query_rowid.extend(query_result_on_index(page_data, page_size, all_pages, keyindex_value));
        }
        query_rowid
}

fn query_on_node(table_page_data: &Vec<u8>, page_size: usize, all_pages: &Vec<u8>, keyindex_value: &String, count: usize) -> Vec<i64> {
    let mut query_rowid: Vec<i64> = Vec::new();
    let offset = u16::from_be_bytes([table_page_data[12+count], table_page_data[count+13]]);
    let mut content_offset_area = table_page_data.iter().skip(offset as usize);
    let page_pointer_vec: Vec<u8> = content_offset_area.by_ref().take(4).cloned().collect();

    let page_num = u32::from_be_bytes([page_pointer_vec[0], page_pointer_vec[1], page_pointer_vec[2], page_pointer_vec[3]]);
    let page_data: Vec<u8> = all_pages.iter().skip(page_size * ((page_num - 1) as usize)).
    take(page_size).cloned().collect();

    query_rowid.extend(query_result_on_index(page_data, page_size, all_pages, keyindex_value));
    match get_rowid(&mut content_offset_area, keyindex_value) {
        Some(val) => { query_rowid.push(val);}
        None => {}
    }
    query_rowid
}

fn query_leaf_index_page(table_page_data: Vec<u8>, keyindex_value: &String) -> Vec<i64> {
    let mut query_rowid: Vec<i64> = Vec::new();
    let cells_count = u16::from_be_bytes([table_page_data[3], table_page_data[4]]) as usize * 2;
    let mut count: usize = 0;
    while count < cells_count {
        let offset = u16::from_be_bytes([table_page_data[8 + count], table_page_data[count+9]]);
        let mut content_area_iter = table_page_data.iter().skip(offset as usize);
        let rowid =  get_rowid(&mut content_area_iter, keyindex_value);
        match rowid {
            Some(id) => query_rowid.push(id),
            None => {}
        }
        count += 2;
    }
    query_rowid
}

fn get_rowid<'a, T>(content_area_iter: &mut T, keyindex_value: &String) -> Option<i64>
        where T:  Iterator <Item = &'a u8> {
    varint_val(content_area_iter);
    let cell_datas_head: Vec<u8> = content_area_iter.cloned().collect();
    let mut cell_datas_head_iter = cell_datas_head.iter();
    let mut cell_datas_body = cell_datas_head.iter();

    let header_size = varint_val(&mut cell_datas_head_iter); 
    cell_datas_body.nth(header_size - 1);

    let serial_type = varint_val(&mut cell_datas_head_iter);
    let col_data = consume_data(&mut cell_datas_body,  serial_type);
    if col_data.value != *keyindex_value {
        return None
    }
    let serial_type_rowid = varint_val(&mut cell_datas_head_iter);
    let col_data_rowid = consume_data(&mut cell_datas_body,  serial_type_rowid);
    let id: i64 = col_data_rowid.value.parse().unwrap();
    Some(id)
}

    
fn column_index_value(condition: CondExpression) -> String {
    match condition {
        CondExpression::Comparison(comp_expr) => {
            match *comp_expr.right_operand {
                CondExpression::Literal(val) => {
                    match val {
                        CompOperand::Identifier(string) => panic!("error {}", string),
                        CompOperand::Str(string) => string,
                        CompOperand::Number(num) => num.to_string(),       
                    }
                },
                _ => panic!("error")
            }
        },
        _ => panic!("error")
    }
}


pub fn query_on_table(table_page_data: Vec<u8>, page_size: usize, all_pages: &Vec<u8>, 
    select_stmt: &SelectStmtData, table_columns: &TableHeadDesc, rowid_array: &mut VecDeque<i64>) -> Vec<Vec<ColumnValue>> 
    { 
    if rowid_array.len() == 0 {
        return Vec::new();
    }
    let page_type = table_page_data[0];
    let query_result = match page_type {
        5 => query_interior_table_page(table_page_data, page_size, all_pages, select_stmt, table_columns, rowid_array),
        13 => query_leaf_table_page(table_page_data, select_stmt, table_columns, rowid_array),
        _ => {panic!("error in header");}
    };
    query_result
}

fn query_interior_table_page(table_page_data: Vec<u8>, page_size: usize, all_pages: &Vec<u8>, 
    select_stmt: &SelectStmtData, table_columns: &TableHeadDesc, rowid_array: &mut VecDeque<i64>) -> Vec<Vec<ColumnValue>> {
        let mut query_datas: Vec<Vec<ColumnValue>> = Vec::new();
        if rowid_array.len() == 0 {
            return  query_datas;
        }
        let cells_num_size = (u16::from_be_bytes([table_page_data[3], table_page_data[4]]) * 2) as usize;
        let mut count = 0;
        while count < cells_num_size && rowid_array.len() != 0 {
            let offset = u16::from_be_bytes([table_page_data[12 + count], table_page_data[count+13]]);
            let content_offset_area = table_page_data.iter().skip(offset as usize);
            let page_pointer_vec: Vec<u8> = content_offset_area.take(4).cloned().collect();
            let page_num = u32::from_be_bytes([page_pointer_vec[0], page_pointer_vec[1], page_pointer_vec[2], page_pointer_vec[3]]) as usize;
            let page_data: Vec<u8> = all_pages.iter().skip(page_size * (page_num - 1)).
            take(page_size as usize).cloned().collect();
            let query_result = query_on_table(page_data, page_size, 
                all_pages, select_stmt, table_columns, rowid_array);
            query_datas.extend(query_result);
            count += 2;
        }
        let right_most_page_num = u32::from_be_bytes([table_page_data[8], table_page_data[9], table_page_data[10], table_page_data[11]]) as usize;
        let right_page_data: Vec<u8> = all_pages.iter().skip(page_size * (right_most_page_num - 1)).
        take(page_size as usize).cloned().collect();
        let query_result = query_on_table(right_page_data, page_size, 
            all_pages, select_stmt, table_columns, rowid_array);
        query_datas.extend(query_result);
        query_datas
}

fn query_leaf_table_page (table_page_data: Vec<u8>, select_stmt: &SelectStmtData, 
    table_columns: &TableHeadDesc, rowid_array: &mut VecDeque<i64>) -> Vec<Vec<ColumnValue>> {
    let mut query_datas: Vec<Vec<ColumnValue>> = Vec::new();
    if rowid_array.len() == 0 {
        return  query_datas;
    }
    let cells_count = u16::from_be_bytes([table_page_data[3], table_page_data[4]]) as usize * 2;
    let mut count: usize = 0;
    let i = 0;
    while i < rowid_array.len() {
        let rowid = rowid_array[i] as usize;
        if check_rowid_in_page(&table_page_data, rowid) && count < cells_count {
            let mut offset = u16::from_be_bytes([table_page_data[8 + count], table_page_data[count+9]]) as usize;
            while count < cells_count && rowid_by_offset(&table_page_data, offset) != rowid {
                offset = u16::from_be_bytes([table_page_data[8 + count], table_page_data[count+9]]) as usize;
                count += 2;
            }
            if count < cells_count {
                let mut content_offset_area = table_page_data.iter().skip(offset);
                let row_result = row_statement_result(&mut content_offset_area,
                    table_columns.clone(), select_stmt);
                query_datas.push(row_result); 
                rowid_array.pop_front();
                count += 2;
            }
            else {break;}
        }
        else {
            break;
        }
    }
    query_datas
}

fn check_rowid_in_page(table_page_data: &Vec<u8>, rowid: usize) -> bool {
    let count_cells = u16::from_be_bytes([table_page_data[3], table_page_data[4]]) as usize * 2;

    let high_offset = u16::from_be_bytes([table_page_data[8], table_page_data[9]]);
    let high_rowid = rowid_by_offset(table_page_data, high_offset as usize);

    let low_offset = u16::from_be_bytes([table_page_data[count_cells+6], table_page_data[count_cells+7]]);
    let low_rowid = rowid_by_offset(table_page_data, low_offset as usize);

    (low_rowid <= rowid && rowid <= high_rowid)  ||  (low_rowid >= rowid && rowid >= high_rowid)
}

fn rowid_by_offset(table_page_data: &Vec<u8>, offset: usize) -> usize {
    let mut content_offset_area = table_page_data.iter().skip(offset);
    let _offset_record_size = varint_val(&mut content_offset_area);
    let rowid = varint_val(&mut content_offset_area);
    rowid
}