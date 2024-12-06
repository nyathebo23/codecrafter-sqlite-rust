
pub fn varint_val<'a> (cell_data: &mut impl Iterator <Item = &'a u8>) -> (u64, u8) {
    let mut cell_val = result_on_iter_num(cell_data);
    let mut first_bit = cell_val >> 7;
    let mut varint_num = (cell_val & 127) as u64;
    let mut bits_count: u8 = 7;
    while first_bit == 1 && bits_count < 63{
        cell_val = result_on_iter_num(cell_data);
        varint_num = varint_num << 7 | (cell_val & 127) as u64;
        first_bit = cell_val >> 7;
        bits_count += 7;
    }
    (varint_num, bits_count)
}

pub fn result_on_iter_num<'a> (iter_data: &mut impl Iterator <Item = &'a u8>) -> u8 {
    match iter_data.nth(0) {
        Some(i) => *i,
        None => 0,
    }
}