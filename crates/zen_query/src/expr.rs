//! Expression AST.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Literal {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Null,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Expr {
    Column(String),
    Literal(Literal),
    Eq(Box<Expr>, Box<Expr>),
    Ne(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Le(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    Ge(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    /// `text_match(column, "search")` — full-text predicate.
    TextMatch { column: String, query: String },
    /// `vector_distance(column, query_vector)` returning a score column.
    VectorDistance { column: String, query: Vec<f32> },
    /// `metadata.user_id = 'foo'` etc.
    JsonPathEq { path: String, value: String },
}

impl Expr {
    pub fn col<S: Into<String>>(s: S) -> Self {
        Expr::Column(s.into())
    }
    pub fn lit_str<S: Into<String>>(s: S) -> Self {
        Expr::Literal(Literal::String(s.into()))
    }
    pub fn lit_int(i: i64) -> Self {
        Expr::Literal(Literal::Int(i))
    }
    pub fn eq(a: Expr, b: Expr) -> Self {
        Expr::Eq(Box::new(a), Box::new(b))
    }
    pub fn and(a: Expr, b: Expr) -> Self {
        Expr::And(Box::new(a), Box::new(b))
    }
}
