use std::collections::HashMap;
use crate::utils_func::{varint_val, bytes_to_string, bytes_to_float};
use super::{parser_utils::{CondExpression, SelectStmtData, TableHeadDesc}, select_on_condition_utils::ExprCondition};

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValueType {
    Integer,
    Real,
    Text,
    Blob,
    Null,
    BitInt,
    None
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ColumnValue {
    pub data_type: ColumnValueType,
    pub value: String,
}

impl ColumnValue {
    pub fn new(datatype: ColumnValueType, val: String) -> ColumnValue{
        ColumnValue {
            data_type: datatype,
            value: val,
        }
    }
}


pub fn query_result_array(table_page_data: Vec<u8>, page_size: usize, all_pages: &Vec<u8>, 
    select_stmt: &SelectStmtData, table_columns: &TableHeadDesc) -> Vec<Vec<ColumnValue>> {

    let page_type = table_page_data[0];
    
    let query_result = match page_type {
        5 => query_interior_page(table_page_data, page_size, all_pages, select_stmt, table_columns),
        13 => query_leaf_page(table_page_data, select_stmt, table_columns),
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
            let offset = u16::from_be_bytes([table_page_data[12 + count], table_page_data[count+13]]);
            let content_offset_area = table_page_data.iter().skip(offset as usize);
            let page_pointer_vec: Vec<u8> = content_offset_area.take(4).cloned().collect();
            let page_num = u32::from_be_bytes([page_pointer_vec[0], page_pointer_vec[1], page_pointer_vec[2], page_pointer_vec[3]]);
            let page_data: Vec<u8> = all_pages.iter().skip((page_size as usize) * ((page_num - 1) as usize)).
            take(page_size as usize).cloned().collect();
            let query_result = query_result_array(page_data, page_size, all_pages, select_stmt, table_columns);
            query_datas.extend(query_result);
            count += 2;
        }
        let right_most_page_num = u32::from_be_bytes([table_page_data[8], table_page_data[9], table_page_data[10], table_page_data[11]]) as usize;
        let right_page_data: Vec<u8> = all_pages.iter().skip(page_size * (right_most_page_num - 1)).
        take(page_size as usize).cloned().collect();
        let query_result = query_result_array(right_page_data, page_size, 
            all_pages, select_stmt, table_columns);
        query_datas.extend(query_result);
        query_datas
}

fn query_leaf_page(table_page_data: Vec<u8>, select_stmt: &SelectStmtData, table_columns: &TableHeadDesc)
 -> Vec<Vec<ColumnValue>> {
    let mut query_datas: Vec<Vec<ColumnValue>> = Vec::new();
    let cells_count = u16::from_be_bytes([table_page_data[3], table_page_data[4]]) as usize * 2;
    let mut count: usize = 0;
    while count < cells_count {
        let offset = u16::from_be_bytes([table_page_data[8 + count], table_page_data[count+9]]);

        let mut content_offset_area = table_page_data.iter().skip(offset as usize);

        let row_result = row_statement_result(&mut content_offset_area,
            table_columns.clone(), select_stmt);
        
        if !row_result.is_empty() { 
            query_datas.push(row_result); 
        }
        count += 2;
    }
    query_datas
}

pub fn row_statement_result<'a>(cell_datas: &mut impl Iterator <Item = &'a u8>, table_head: TableHeadDesc, select_stmt: &SelectStmtData) 
    -> Vec<ColumnValue> {

    let record_size = varint_val(cell_datas);
    let rowid = varint_val(cell_datas);

    let cell_datas_head: Vec<u8> = cell_datas.copied().collect();
    let mut cell_datas_head_iter = cell_datas_head.iter();
    let mut cell_datas_body = cell_datas_head.iter();

    let header_size = varint_val(&mut cell_datas_head_iter); 

    cell_datas_body.nth(header_size - 1);

    let mut statement_result = Vec::new();
    let mut row_values = HashMap::new();
    for table_col in table_head.columns_names {
        let serial_type = varint_val(&mut cell_datas_head_iter);
        let col_data = consume_data(&mut cell_datas_body,  serial_type);
        row_values.insert(table_col, col_data);
    }
    let payload_size =  cell_datas_head.len() -  cell_datas_body.count();
    assert_eq!(payload_size, record_size);
    match table_head.rowid_column_name {
        Some(name) => { 
            let rowid_colvalue = ColumnValue::new(ColumnValueType::Integer, rowid.to_string());
            row_values.insert(name, rowid_colvalue); 
        },
        None => {}
    }
    if select_stmt.condition != CondExpression::Null && !check_condition(&select_stmt.condition, &row_values) {
        return statement_result;
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


pub fn consume_data<'a, T>(cell_datas_values: &mut T, serialtype: usize) -> ColumnValue 
    where T : Iterator <Item = &'a u8> {
    
    match serialtype {
        n if n >= 12 => {
            if n % 2 == 0 {
                let bytes = (n - 12)/2;
                let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(bytes).map(|val| *val).collect();
                let str_data = bytes_to_string(bytes_values);
                ColumnValue::new(ColumnValueType::Blob, str_data)
            }
            else {
                let bytes = (n - 13)/2;
                let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(bytes).map(|val| *val).collect();
                let str_data = bytes_to_string(bytes_values);
                ColumnValue::new(ColumnValueType::Text, str_data)
            }
        },
        1 => {
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(1).map(|val| *val).collect();
            let str_data = i8::from_be_bytes([bytes_values[0]]).to_string();
            ColumnValue::new(ColumnValueType::Integer, str_data)

        },
        2 => {
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(2).map(|val| *val).collect();
            let str_data = i16::from_be_bytes([bytes_values[0], bytes_values[1]]).to_string();
            ColumnValue::new(ColumnValueType::Integer, str_data)

        },
        3 => {
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(3).map(|val| *val).collect();
            let str_data = i32::from_be_bytes([0, bytes_values[0], bytes_values[1], bytes_values[2]]).to_string();
            ColumnValue::new(ColumnValueType::Integer, str_data)

        },
        4 => {
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(4).map(|val| *val).collect();
            let str_data = i32::from_be_bytes([bytes_values[0], bytes_values[1], bytes_values[2], bytes_values[3]])
            .to_string();
            ColumnValue::new(ColumnValueType::Integer, str_data)
        },
        5 => {
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(6).map(|val| *val).collect();
            let str_data = i64::from_be_bytes([0, 0, bytes_values[0], bytes_values[1], 
                bytes_values[2], bytes_values[3], bytes_values[4], bytes_values[5]]).to_string();
            ColumnValue::new(ColumnValueType::Integer, str_data)
        },
        6 => {
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(8).map(|val| *val).collect();
            let fixed_array: [u8; 8] = bytes_values.try_into().expect("error");
            let str_data = i64::from_be_bytes(fixed_array).to_string();
            ColumnValue::new(ColumnValueType::Integer, str_data)
        }
        // 1 | 2 | 3 | 4 | 5 | 6 => {
        //     let str_data = varint_val(cell_datas_values).to_string();
        //     ColumnValue::new(ColumnValueType::Integer, str_data)
        // },
        7 => {
            let bytes = 8;
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(bytes).map(|val| *val).collect();
            let str_data =  bytes_to_float(bytes_values).to_string();
            ColumnValue::new(ColumnValueType::Real, str_data)
        },
        0 => {
            ColumnValue::new(ColumnValueType::Null, String::from(""))
        },
        8 | 9 => {
            let str_data = if serialtype == 8 {"0"} else {"1"};
            ColumnValue::new(ColumnValueType::BitInt, String::from(str_data))
        },
        _ => {
            ColumnValue::new(ColumnValueType::None, String::from(""))
        }
    }
}

pub fn check_condition(cond: &CondExpression, row_values: &HashMap<String, ColumnValue>) -> bool {
    match cond {
        CondExpression::Comparison(value) =>  {
            value.evaluate(row_values)
        },
        CondExpression::Condition(value) =>  {
            value.evaluate(row_values)
        },
        CondExpression::Literal(_oper) => {
            panic!("Error in condition expression");
        },
        _ => false
    }
}




