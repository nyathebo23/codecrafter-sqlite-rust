use std::fs::File;
use std::io::Read;

pub fn database_infos(dbfile: &mut File) {
    let mut header_and_header_page = [0; 112];
    let _ = dbfile.read_exact(&mut header_and_header_page);
    
    let page_size = u16::from_be_bytes([header_and_header_page[16], header_and_header_page[17]]);
    let cells_count = u16::from_be_bytes([header_and_header_page[103], header_and_header_page[104]]);
    println!("database page size: {}", page_size);
    println!("number of tables: {}", cells_count);
    
}