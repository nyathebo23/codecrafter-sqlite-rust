use std::collections::HashMap;
use crate::utils_func::{varint_val, bytes_to_string, bytes_to_float};
use super::{parser_utils::{CondExpression, SelectStmtData}, select_on_condition_utils::ExprCondition};

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValueType {
    Integer,
    Float,
    String,
    Blob,
    Null,
    BitInt,
    None
}
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum ColumnSpecialProperties {
    PrimaryKey,
    ForeignKey,
}
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ColumnValue {
    pub data_type: ColumnValueType,
    pub value: String,
    pub specials_properties: Vec<ColumnSpecialProperties>
}

impl ColumnValue {
    pub fn new(datatype: ColumnValueType, val: String, spec: Vec<ColumnSpecialProperties>) -> ColumnValue{
        ColumnValue {
            data_type: datatype,
            value: val,
            specials_properties: spec
        }
    }
}


pub fn query_result_array(table_page_data: Vec<u8>, rows_count: usize, select_stmt: &SelectStmtData, 
    table_columns: &Vec<String>) -> Vec<Vec<ColumnValue>> {
    let cells_num_size = rows_count * 2;
    let mut count: usize = 0;

    let mut query_result: Vec<Vec<ColumnValue>> = Vec::new();
    while count < cells_num_size {
        let offset = u16::from_be_bytes([table_page_data[8 + count], table_page_data[count+9]]);

        let mut content_offset_area = table_page_data.iter().skip(offset as usize);

        let row_result = row_statement_result(&mut content_offset_area,
            table_columns.clone(), select_stmt);
        
        if !row_result.is_empty() {query_result.push(row_result);}
        count += 2;
    }
    query_result
}


pub fn row_statement_result<'a>(cell_datas: &mut impl Iterator <Item = &'a u8>, table_columns: Vec<String>, select_stmt: &SelectStmtData) 
    -> Vec<ColumnValue> {

    let _record_size = varint_val(cell_datas);
    let _rowid = varint_val(cell_datas);

    let cell_datas_head: Vec<u8> = cell_datas.copied().collect();
    let mut cell_datas_head_iter = cell_datas_head.iter();
    let mut cell_datas_body = cell_datas_head.iter();

    let header_size = varint_val(&mut cell_datas_head_iter); 

    cell_datas_body.nth(header_size - 1);

    let mut statement_result = Vec::new();
    let mut row_values = HashMap::new();
    for table_col in table_columns {
        let serial_type = varint_val(&mut cell_datas_head_iter);
        let col_data = consume_data(&mut cell_datas_body,  serial_type);
        row_values.insert(table_col, col_data);
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


fn consume_data<'a, T>(cell_datas_values: &mut T, serialtype: usize) -> ColumnValue 
    where T : Iterator <Item = &'a u8> {
    
    match serialtype {
        n if n >= 12 => {
            if n % 2 == 0 {
                let bytes = (n - 12)/2;
                let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(bytes).map(|val| *val).collect();
                let str_data = bytes_to_string(bytes_values);
                ColumnValue::new(ColumnValueType::Blob, str_data, Vec::new())
            }
            else {
                let bytes = (n - 13)/2;
                let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(bytes).map(|val| *val).collect();
                let str_data = bytes_to_string(bytes_values);
                ColumnValue::new(ColumnValueType::String, str_data, Vec::new())
            }
        },
        1 => {
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(1).map(|val| *val).collect();
            let str_data = i8::from_be_bytes([bytes_values[0]]).to_string();
            ColumnValue::new(ColumnValueType::Integer, str_data, Vec::new())

        },
        2 => {
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(2).map(|val| *val).collect();
            let str_data = i16::from_be_bytes([bytes_values[0], bytes_values[1]]).to_string();
            ColumnValue::new(ColumnValueType::Integer, str_data, Vec::new())

        },
        3 => {
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(3).map(|val| *val).collect();
            let str_data = i32::from_be_bytes([0, bytes_values[0], bytes_values[1], bytes_values[2]]).to_string();
            ColumnValue::new(ColumnValueType::Integer, str_data, Vec::new())

        },
        4 => {
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(4).map(|val| *val).collect();
            let str_data = i32::from_be_bytes([bytes_values[0], bytes_values[1], bytes_values[2], bytes_values[3]])
            .to_string();
            ColumnValue::new(ColumnValueType::Integer, str_data, Vec::new())
        },
        5 => {
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(6).map(|val| *val).collect();
            let str_data = i64::from_be_bytes([0, 0, bytes_values[0], bytes_values[1], 
                bytes_values[2], bytes_values[3], bytes_values[4], bytes_values[5]]).to_string();
            ColumnValue::new(ColumnValueType::Integer, str_data, Vec::new())
        },
        6 => {
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(8).map(|val| *val).collect();
            let fixed_array: [u8; 8] = bytes_values.try_into().expect("error");
            let str_data = i64::from_be_bytes(fixed_array).to_string();
            ColumnValue::new(ColumnValueType::Integer, str_data, Vec::new())
        }
        // 1 | 2 | 3 | 4 | 5 | 6 => {
        //     let str_data = varint_val(cell_datas_values).to_string();
        //     ColumnValue::new(ColumnValueType::Integer, str_data, Vec::new())
        // },
        7 => {
            let bytes = 8;
            let bytes_values: Vec<u8> = cell_datas_values.by_ref().take(bytes).map(|val| *val).collect();
            let str_data =  bytes_to_float(bytes_values).to_string();
            ColumnValue::new(ColumnValueType::Float, str_data, Vec::new())
        },
        0 => {
            ColumnValue::new(ColumnValueType::Null, String::from(""), Vec::new())
        },
        8 | 9 => {
            let str_data = if serialtype == 8 {"0"} else {"1"};
            ColumnValue::new(ColumnValueType::BitInt, String::from(str_data), Vec::new())
        },
        _ => {
            ColumnValue::new(ColumnValueType::None, String::from(""), Vec::new())
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




