use anyhow::{bail, Result};
use database_infos::database_infos;
use database_tables_names::database_tables_names;
use std::fs::File;
mod database_infos;
mod database_tables_names;
mod sql_clause_select;
mod utils_func;


fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            eprintln!("Logs from your program will appear here!");
            database_infos(&mut file);
        },
        ".tables" => {
            let file = File::open(&args[1])?;
            database_tables_names(file);
        },
        _ => {
            let command_items: Vec<&str> = command.as_str().split(' ').collect();
            let clause = command_items[0].to_lowercase();
            match clause.as_str() {
                "select" => {
                    let file = File::open(&args[1])?;
                    let table_name = match command_items.last()  {
                        Some(tablename) => *tablename,
                        _ => ""
                    };
                    sql_clause_select::select_count::select_count(file, String::from(table_name));
                },
                _ => bail!("Missing or invalid command passed: {}", command),
            }
        }
    }

    Ok(())
}
