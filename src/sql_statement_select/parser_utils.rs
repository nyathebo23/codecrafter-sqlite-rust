
#[derive(Clone, Debug, PartialEq)]
pub enum CondOperator {
    Or,
    And
}
#[derive(Clone, Debug, PartialEq)]
pub enum CompOperator {
    Equal,
    GreaterEqual,
    Greater,
    Lesser,
    LesserEqual,
    NotEqual
}
#[derive(Clone, Debug, PartialEq)]
pub enum CompOperand {
    Identifier(String),
    Str(String),
    Number(f64)
}

#[derive(Clone, Debug, PartialEq)]
pub enum CondExpression {
    Comparison(ExprBinaryComparison),
    Condition(ExprBinaryCondition),
    Literal(CompOperand),
    Null
}


#[derive(Clone, Debug, PartialEq)]
pub struct ExprBinaryComparison {
    pub left_operand: Box<CondExpression>,
    pub operator: CompOperator,
    pub right_operand: Box<CondExpression>
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExprBinaryCondition {
    pub left_operand: Box<CondExpression>,
    pub operator: CondOperator,
    pub right_operand: Box<CondExpression>
}

impl ExprBinaryCondition {
    pub fn is_condition_valid(&self) -> bool{
        true
    }
}

// pub struct ExprGroup {
//     pub expression: CondExpression
// }
#[derive(Clone, Debug)]
pub struct SelectStmtData {
    pub columns: Vec<String>,
    pub table_name: String,
    pub condition: CondExpression
}



#[allow(dead_code)]
pub fn print_stmt_cond(cond: CondExpression) {
    match cond {
        CondExpression::Comparison(expr) => {
            print_stmt_cond(*expr.left_operand);
            print_comp_op(expr.operator);
            print_stmt_cond(*expr.right_operand);

        },
        CondExpression::Condition(expr) => {
            print_stmt_cond(*expr.left_operand);
            print_cond_op(expr.operator);
            print_stmt_cond(*expr.right_operand);
        },
        CondExpression::Literal(expr) => {
            match expr {
                CompOperand::Identifier(op) => {println!("Identifier {}", op);}
                CompOperand::Str(op) => {println!("String {}", op);}
                CompOperand::Number(op) => {println!("Number {}", op);}
            }
        },
        CondExpression::Null => {
            println!("")
        },
    }
}

#[allow(dead_code)]
pub fn print_comp_op(op: CompOperator) {
    match op {
        CompOperator::Equal => println!("="),
        CompOperator::Greater => println!(">"),      
        CompOperator::GreaterEqual => println!(">="),
        CompOperator::Lesser => println!("<"),
        CompOperator::LesserEqual => println!("<="),
        CompOperator::NotEqual => println!("!=")
    }
}

#[allow(dead_code)]
pub fn print_cond_op(op: CondOperator) {
    match op {
        CondOperator::And => println!("and"),
        CondOperator::Or => println!("or")
    }
}
