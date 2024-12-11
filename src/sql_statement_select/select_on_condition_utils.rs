use std::collections::HashMap;

use super::parser_utils::CompOperand;
use super::parser_utils::CompOperator;
use super::parser_utils::CondExpression;
use super::parser_utils::CondOperator;
use super::parser_utils::ExprBinaryComparison;
use super::parser_utils::ExprBinaryCondition;
use super::select_query_result::ColumnValue;
use super::select_query_result::ColumnValueType;

pub trait ExprCondition {
    fn is_condition_valid(&self) -> bool;
    fn evaluate(&self, row_hash: &HashMap<String, ColumnValue>) -> bool;
}

impl ExprCondition for ExprBinaryComparison {
    fn is_condition_valid(&self) -> bool{
        match *self.left_operand.clone() {
            #[allow(unused_variables)]
            CondExpression::Literal(valleft) => match *self.right_operand.clone() {
                CondExpression::Literal(valright) => {
                    match valleft {
                        CompOperand::Identifier(nameleft) => true,
                        _ => match valright {
                            CompOperand::Identifier(nameright) => true,
                            _ => {
                                false
                            }
                        }
                    }
                },
                _ => false
            },
            _ => {
               return false;
            }
        }
    }
    fn evaluate(&self, row_hash: &HashMap<String, ColumnValue>) -> bool {
        if !self.is_condition_valid(){
            panic!("Invalid arguments to compare");
        }
        let left_op = literal_operand_from_expression(*self.left_operand.clone()).expect("literal");
        let right_op = literal_operand_from_expression(*self.right_operand.clone()).expect("literal");

        let opleft_field_value: ColumnValue = value_from_operand(left_op, row_hash);
        let opright_field_value: ColumnValue = value_from_operand(right_op, row_hash);

        if is_comparison_valid(&opleft_field_value, &opright_field_value, &self.operator){
            compare(&opleft_field_value, &opright_field_value, &self.operator)
        }
        else {
            panic!("Invalid arguments to compare");
        }
    }
}

fn literal_operand_from_expression(expr: CondExpression) -> Option<CompOperand> {
    match expr {
        CondExpression::Literal(val) => {
            Some(val)
        },
        _ => None
    }
}

fn value_from_operand (column_operand: CompOperand, row_hash: &HashMap<String, ColumnValue>) -> ColumnValue {
    match column_operand {
        CompOperand::Identifier(name) => {
            match row_hash.get(&name) {
                Some(value) => {
                    value.clone()
                },
                None => {
                    ColumnValue::new(ColumnValueType::Null, String::from(""))
                }
            }
        },
        CompOperand::Number(num) => {
            ColumnValue::new(ColumnValueType::Real, num.to_string())
        },
        CompOperand::Str(string) => {
            ColumnValue::new(ColumnValueType::Text, string)
        }
    }
}

fn is_comparison_valid(col1: &ColumnValue, col2: &ColumnValue, op: &CompOperator) -> bool {
    if col1.data_type == ColumnValueType::Real || col1.data_type == ColumnValueType::Integer {
        if col2.data_type == ColumnValueType::Real || col2.data_type == ColumnValueType::Integer {
            return false;
        }
        return true
    }

    if (col1.data_type != col2.data_type) && (col1.data_type != ColumnValueType::Null) && (col2.data_type != ColumnValueType::Null) {
        return false;
    }
    if col1.data_type != ColumnValueType::Text {
        return match op {
            CompOperator::Equal | CompOperator::NotEqual => true,
            _ => false
        }
    }
    true
}

fn compare(col1: &ColumnValue, col2: &ColumnValue, op: &CompOperator) -> bool {
    if col1.data_type == ColumnValueType::Real || col1.data_type == ColumnValueType::Integer {
        let val1: f64 = col1.value.parse().unwrap();
        let val2: f64 = col2.value.parse().unwrap();
        return compare_value::<f64>(val1, val2, &op);
    }
    if col1.data_type == ColumnValueType::Text {
        return compare_value::<String>(col1.value.clone(), col2.value.clone(), &op);
    }
    false

}

fn compare_value<T: PartialOrd + PartialEq>(value1: T, value2: T, op: &CompOperator) -> bool {
    match op {
        CompOperator::Equal => value1 == value2,
        CompOperator::NotEqual => value1 != value2,
        CompOperator::Greater => value1 > value2,
        CompOperator::GreaterEqual => value1 >= value2,
        CompOperator::Lesser => value1 < value2,
        CompOperator::LesserEqual => value1 <= value2,
    }
}

impl ExprCondition for ExprBinaryCondition {

    fn is_condition_valid(&self) -> bool {
        checkvalidity_from_expression(&self.left_operand) &&
        checkvalidity_from_expression(&self.right_operand)
    }

    fn evaluate(&self, row_hash: &HashMap<String, ColumnValue>) -> bool {
        if !self.is_condition_valid(){
            panic!("Invalid arguments to compare");
        }
        let leftcond =  evaluate_operand(*self.left_operand.clone() , row_hash);
        let rightcond =  evaluate_operand(*self.right_operand.clone() , row_hash);
        match self.operator {
            CondOperator::And => rightcond && leftcond,
            CondOperator::Or => rightcond || leftcond
        }
    }
}

fn checkvalidity_from_expression(expr: &CondExpression) -> bool {
    match expr {
        CondExpression::Condition(val) => {
            val.is_condition_valid()
        },
        CondExpression::Comparison(val) => {
            val.is_condition_valid()
        },
        _ => false
    }
}

fn evaluate_operand(operand: CondExpression, row_hash: &HashMap<String, ColumnValue>) -> bool {
    match operand {
        CondExpression::Condition(val) => {
            val.evaluate(&row_hash)
        },
        CondExpression::Comparison(val) => {
            val.evaluate(&row_hash)
        },
        _ => false
    }
}