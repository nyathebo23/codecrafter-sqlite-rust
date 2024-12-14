#[allow(dead_code)]
pub struct SchemaInfos {
    pub schema_type: String,
    pub name: String,
    pub tbl_name: String,
    pub rootpage: usize,
    pub sql: String
}

impl SchemaInfos {
    pub fn new(schema_type: String, name: String, tbl_name: String, rootpage: usize, sql: String) -> SchemaInfos {
        SchemaInfos {
            schema_type,
            name,
            tbl_name,
            rootpage,
            sql
        }
    }
}

pub fn varint_val<'a, T> (cell_datas: &mut T) -> usize
where T : Iterator <Item = &'a u8> {
    let mut cell_val = result_on_iter_num(cell_datas);

    let mut first_bit = cell_val >> 7;
    let mut varint_num = (cell_val & 127) as u64;
    while first_bit == 1 {
        cell_val = result_on_iter_num(cell_datas);
        varint_num = varint_num << 7 | (cell_val & 127) as u64;
        first_bit = cell_val >> 7;
    }

    varint_num as usize
}

pub fn result_on_iter_num<'a, T> (iter_data: &mut T) -> u8 
where T : Iterator <Item = &'a u8> {
    match iter_data.nth(0) {
        Some(i) => *i,
        None => 0,
    }
}

// fn rootpage_size<'a> (iter_data: &mut impl Iterator <Item = &'a u8>) -> u8{
//     let serialtype_rootpage =  result_on_iter_num(iter_data);
//     let rootpage_size = match serialtype_rootpage {
//         1 => 1,
//         2 => 2,
//         3 => 3,
//         4 => 4,
//         5 => 6,
//         6 => 8,
//         _ => 255
//     };
//     rootpage_size
// }

pub fn page_data(pages_datas: &Vec<u8>, tablename: String, pagesize: usize, cells_num_size: usize) -> Vec<u8> {
    let mut count: usize = 0;
    let mut table_schema_inf = SchemaInfos::new(String::from(""),
     String::from(""), String::from(""), 0, String::from(""));

    while count < cells_num_size && table_schema_inf.name != tablename {
        let offset = u16::from_be_bytes([pages_datas[108 + count], pages_datas[count+109]]) as usize;
        let mut start_area_iter = pages_datas.iter().skip(offset);
        table_schema_inf = table_schema_infos(&mut start_area_iter);
        println!("name {} table name {} root {}", table_schema_inf.name, table_schema_inf.tbl_name, table_schema_inf.rootpage);
        count += 2;
    }

    if table_schema_inf.name != tablename {
        let vec: Vec<u8> = Vec::new();
        return vec;
    }

    let page_data: Vec<u8> = pages_datas.iter().skip(pagesize * (table_schema_inf.rootpage - 1)).
    take(pagesize).cloned().collect();
    page_data
}


pub fn table_schema_infos<'a, T> (cell_datas: &mut T) -> SchemaInfos 
where T : Iterator <Item = &'a u8> {
    let _record_size = varint_val(cell_datas);
    let _rowid = varint_val(cell_datas);
    #[allow(unused_variables)]
    let header_size = result_on_iter_num(cell_datas);
    let table_type_size = (result_on_iter_num(cell_datas) - 13)/2;
    let name_size = (result_on_iter_num(cell_datas) - 13)/2;
    let table_name_size = (result_on_iter_num(cell_datas) - 13)/2;
    #[allow(unused_variables)]
    let rootpage_size = varint_val(cell_datas);
    let sql_text_size = varint_val(cell_datas);

    let table_type = text_from_cell(cell_datas, table_type_size as usize);
    let name = text_from_cell(cell_datas, name_size as usize);

    let table_name= text_from_cell(cell_datas, table_name_size as usize);
    let rootpage = varint_val(cell_datas);
    let sql = text_from_cell(cell_datas, (sql_text_size - 13)/2);
    println!("table name {} tabletype {} rootpage size {} rootpage {} ", name, table_type, rootpage_size, rootpage);
    SchemaInfos::new(table_type, name, table_name, rootpage, sql)
}


pub fn table_name<'a, T> (cell_datas: &mut T) -> String
where T : Iterator <Item = &'a u8> {
    let _record_size = varint_val(cell_datas);
    let _rowid = varint_val(cell_datas);

    let header_size = result_on_iter_num(cell_datas);
    let table_type_size = (result_on_iter_num(cell_datas) - 13)/2;
    let name_size = (result_on_iter_num(cell_datas) - 13)/2;
    cell_datas.nth((header_size - 4) as usize);
    cell_datas.nth((table_type_size - 1) as usize);

    let table_name= text_from_cell(cell_datas, name_size as usize);
    table_name
}


pub fn text_from_cell<'a, T> (datas: &mut T, text_size: usize) -> String 
where  T : Iterator <Item = &'a u8>  {
    let data: Vec<u8> = datas.take(text_size).cloned().collect();
    bytes_to_string(data)
}

pub fn bytes_to_float(data: Vec<u8>) -> f64 {
    let array: [u8; 8] = data.try_into().expect("bad length");
    f64::from_be_bytes(array)
}

// fn bytes_to_int(data: Vec<u8>) -> usize {
//     let array: [u8; 8] = data.try_into().expect("bad length");
//     usize::from_be_bytes(array)}

pub fn bytes_to_string(data: Vec<u8>) -> String {
    let string: String = data.iter().map(|&byte| byte as char).collect();
    string
}

// fn bytes_to_blob(data: Vec<u8>) -> String{
//     String::from_utf8(data).unwrap()
// }

// fn serial_type_offset(serialtype: usize) -> usize {
//     let serialtype_offset = match serialtype {
//         0 | 8 | 9 | 10 | 11 |12 | 13 => 0,
//         n if n < 128 => 1,
//         n if n < (1 << 14) => 2,
//         _ => 3
//     };
//     return  serialtype_offset;
// }
