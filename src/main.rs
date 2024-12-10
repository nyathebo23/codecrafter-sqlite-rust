use anyhow::{bail, Result};
use database_infos::database_infos;
use database_tables_names::database_tables_names;
use sql_statement_select::select_columns::select_columns;
use std::fs::File;
//use std::collections::HashMap;
mod database_infos;
mod database_tables_names;
mod sql_statement_select;
mod utils_func;
use sql_statement_select::parser::parse_statement::select_statement;
use sql_statement_select::select_count::select_count;
//use sql_statement_select::parser_utils::SelectStmtData;


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
            let file = File::open(&args[1])?;
            let stmt = select_statement(command.as_str());
            match stmt {
                Ok(stment) => {
                    if stment.columns.len() == 1 && stment.columns[0].to_lowercase().as_str() == "count(*)" {
                        select_count(&file, &stment);
                    }
                    else {
                        select_columns(&file, &stment);
                    }
                },
                Err(err) => {
                    println!("{}", err);
                }
            }
        }
    }

    Ok(())
}
