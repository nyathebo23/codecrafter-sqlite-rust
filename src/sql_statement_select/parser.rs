use peg::parser;
use super::parser_utils::{*};

parser!{
    pub grammar parse_statement() for str {
        rule string() -> String
        =  "'" string_val:$([^'\'']*) "'" { String::from(string_val) }

        rule whitespace() = quiet!{[' ' | '\n' | '\t' | '\r']+}

        rule identifier() -> String = 
            word:$([ 'a'..='z' | 'A'..='Z']['a'..='z' | 'A'..='Z' | '_' | '0'..='9' ]*){ String::from(word) }

        rule expr() -> String = 
            word:$([ 'a'..='z' | 'A'..='Z']['a'..='z' | 'A'..='Z' | '_' | ',' | '0'..='9' | ' ' | '\n' | '\t' | '\r' ]*){ String::from(word) }

        rule count() -> String =
            str_count:$("count(*)"/"COUNT(*)") {String::from(str_count)}

        rule number() -> f64 = 
            n:$(['0'..='9']+(['.']['0'..='9']+)?) {? n.parse::<f64>().or(Err("u32")) }

        rule table_attr() -> String = attribut:identifier() (whitespace() identifier())* {  String::from(attribut) }

        pub rule table_column_names() -> Vec<String> =
            ("create"/"CREATE") whitespace() ("table" / "TABLE") whitespace() identifier() whitespace()? 
            "(" whitespace()? expression:$(expr()) whitespace()?")"
            {
                let res = expression.split(",").map(|val| {
                    let arr: Vec<&str> = val.trim().split(" ").collect(); 
                    String::from(arr[0])
                }).collect();
                res
            }

        rule condition_expression() -> CondExpression = precedence! {
            left:(@) whitespace()? ("and"/"AND") whitespace()? right:@ { 
                CondExpression::Condition(
                    ExprBinaryCondition {
                        left_operand: Box::new(left),
                        operator: CondOperator::And,
                        right_operand: Box::new(right)
                    }
                ) 
            }
            left:(@) whitespace()? ("or"/"OR") whitespace()? right:@ {
                CondExpression::Condition(
                    ExprBinaryCondition {
                        left_operand: Box::new(left),
                        operator: CondOperator::Or,
                        right_operand: Box::new(right)
                    }
                ) 
            }
            --
            left:(@) whitespace()?  "=" whitespace()? right:@ {
                CondExpression::Comparison(
                    ExprBinaryComparison { 
                        left_operand: Box::new(left), 
                        operator: CompOperator::Equal, 
                        right_operand: Box::new(right) 
                    }                     
                ) 
            }
            left:(@) whitespace()? "<=" whitespace()? right:@ {
                CondExpression::Comparison(
                    ExprBinaryComparison { 
                        left_operand: Box::new(left), 
                        operator: CompOperator::LesserEqual, 
                        right_operand: Box::new(right) 
                    }                     
                ) 
            }
            left:(@) whitespace()? ">=" whitespace()? right:@ {
                CondExpression::Comparison(
                    ExprBinaryComparison { 
                        left_operand: Box::new(left), 
                        operator: CompOperator::GreaterEqual, 
                        right_operand: Box::new(right) 
                    }                     
                ) 
            }
            left:(@) whitespace()? "<" whitespace()? right:@ {
                CondExpression::Comparison(
                    ExprBinaryComparison { 
                        left_operand: Box::new(left), 
                        operator: CompOperator::Lesser, 
                        right_operand: Box::new(right) 
                    }                     
                ) 
            }
            left:(@) whitespace()? ">" whitespace()? right:@ {
                CondExpression::Comparison(
                    ExprBinaryComparison { 
                        left_operand: Box::new(left), 
                        operator: CompOperator::Greater, 
                        right_operand: Box::new(right) 
                    }                     
                ) 
            }
            left:(@) whitespace()? "<>" whitespace()? right:@ {
                CondExpression::Comparison(
                    ExprBinaryComparison { 
                        left_operand: Box::new(left), 
                        operator: CompOperator::NotEqual, 
                        right_operand: Box::new(right) 
                    }                     
                ) 
            }
            --
            literal:identifier(){CondExpression::Literal(CompOperand::Identifier(literal))}
            literal:string(){CondExpression::Literal(CompOperand::Str(literal))}
            literal:number(){CondExpression::Literal(CompOperand::Number(literal))}
            "(" expr:condition_expression() ")" {expr}
        }

        pub rule where_condition() -> CondExpression = 
            whitespace() ("where"/"WHERE") whitespace() cond:condition_expression() {
                cond
            }

        pub rule select_statement() -> SelectStmtData = 
            ("select"/"SELECT") whitespace() cols:(count()/identifier()) ** (whitespace()? "," whitespace()?)
            whitespace()? ("from"/"FROM") whitespace()? tbl_name:identifier() cond:where_condition()? {
            let condition = match cond {
                Some(c) => c,
                None => CondExpression::Null
            };
            SelectStmtData {
                table_name: tbl_name,
                condition: condition,
                columns: cols
            }
        }
    }
}
