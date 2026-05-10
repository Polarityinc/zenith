//! SQL → LogicalPlan via `sqlparser-rs`.

use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    Query, SelectItem, SetExpr, Statement, TableFactor, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use zen_common::ZenError;
use zen_query::expr::{Expr, Literal};
use zen_query::logical::{AggregateFn, LogicalPlan, Predicate, Projection};

pub fn parse_sql(sql: &str, tenant_id: u64) -> Result<LogicalPlan, ZenError> {
    let dialect = GenericDialect {};
    let stmts =
        Parser::parse_sql(&dialect, sql).map_err(|e| ZenError::query(format!("sql parse: {e}")))?;
    if stmts.len() != 1 {
        return Err(ZenError::query("expected exactly one statement"));
    }
    let stmt = stmts.into_iter().next().unwrap();
    match stmt {
        Statement::Query(q) => parse_query(&q, tenant_id),
        other => Err(ZenError::query(format!("unsupported statement: {other:?}"))),
    }
}

fn parse_query(q: &Query, tenant_id: u64) -> Result<LogicalPlan, ZenError> {
    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s.as_ref(),
        other => {
            return Err(ZenError::query(format!(
                "only SELECT statements are supported (got {other:?})"
            )));
        }
    };

    // Source must be `spans`.
    if select.from.len() != 1 {
        return Err(ZenError::query("expected single FROM table"));
    }
    let tf = &select.from[0];
    let table_name = match &tf.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        other => return Err(ZenError::query(format!("unsupported FROM: {other:?}"))),
    };
    if table_name.to_lowercase() != "spans" {
        return Err(ZenError::query(format!(
            "only `spans` table is supported (got `{table_name}`)"
        )));
    }

    let mut plan = LogicalPlan {
        tenant_id,
        partition_ids: vec![0],
        ..Default::default()
    };

    // Projection.
    plan.projection = parse_projection(&select.projection)?;
    let mut aggregates: Vec<(String, AggregateFn)> = Vec::new();
    detect_aggregates(&select.projection, &mut aggregates);
    plan.aggregates = aggregates;

    // WHERE.
    if let Some(w) = &select.selection {
        let expr = parse_expr(w)?;
        let (time_min_ms, time_max_ms) = extract_time_range(&expr);
        plan.time_min_ms = time_min_ms;
        plan.time_max_ms = time_max_ms;
        plan.predicate = strip_time_range_predicates(expr).map(|expr| Predicate { expr });
    }

    // GROUP BY.
    if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
        for e in exprs {
            if let SqlExpr::Identifier(id) = e {
                plan.group_by.push(id.value.clone());
            }
        }
    }

    // ORDER BY.
    if let Some(o) = &q.order_by {
        if let Some(first) = o.exprs.first() {
            if let SqlExpr::Identifier(id) = &first.expr {
                plan.order_by = Some((id.value.clone(), first.asc.unwrap_or(true)));
            }
        }
    }

    // LIMIT.
    if let Some(SqlExpr::Value(Value::Number(n, _))) = &q.limit {
        if let Ok(parsed) = n.parse::<u32>() {
            plan.limit = Some(parsed);
        }
    }
    Ok(plan)
}

fn extract_time_range(expr: &Expr) -> (i64, i64) {
    let mut lo = i64::MIN;
    let mut hi = i64::MAX;
    collect_time_range(expr, &mut lo, &mut hi);
    (lo, hi)
}

fn collect_time_range(expr: &Expr, lo: &mut i64, hi: &mut i64) {
    match expr {
        Expr::And(left, right) => {
            collect_time_range(left, lo, hi);
            collect_time_range(right, lo, hi);
        }
        Expr::Eq(left, right) => {
            if let Some(v) = start_time_cmp_value(left, right) {
                *lo = (*lo).max(v);
                *hi = (*hi).min(v);
            }
        }
        Expr::Ge(left, right) => {
            if let Some(v) = start_time_cmp_value(left, right) {
                *lo = (*lo).max(v);
            } else if let Some(v) = literal_cmp_start_time_value(left, right) {
                *hi = (*hi).min(v);
            }
        }
        Expr::Gt(left, right) => {
            if let Some(v) = start_time_cmp_value(left, right) {
                *lo = (*lo).max(v.saturating_add(1));
            } else if let Some(v) = literal_cmp_start_time_value(left, right) {
                *hi = (*hi).min(v.saturating_sub(1));
            }
        }
        Expr::Le(left, right) => {
            if let Some(v) = start_time_cmp_value(left, right) {
                *hi = (*hi).min(v);
            } else if let Some(v) = literal_cmp_start_time_value(left, right) {
                *lo = (*lo).max(v);
            }
        }
        Expr::Lt(left, right) => {
            if let Some(v) = start_time_cmp_value(left, right) {
                *hi = (*hi).min(v.saturating_sub(1));
            } else if let Some(v) = literal_cmp_start_time_value(left, right) {
                *lo = (*lo).max(v.saturating_add(1));
            }
        }
        _ => {}
    }
}

fn start_time_cmp_value(left: &Expr, right: &Expr) -> Option<i64> {
    match (left, right) {
        (Expr::Column(c), Expr::Literal(Literal::Int(v))) if c == "start_time_ms" => Some(*v),
        _ => None,
    }
}

fn literal_cmp_start_time_value(left: &Expr, right: &Expr) -> Option<i64> {
    match (left, right) {
        (Expr::Literal(Literal::Int(v)), Expr::Column(c)) if c == "start_time_ms" => Some(*v),
        _ => None,
    }
}

fn strip_time_range_predicates(expr: Expr) -> Option<Expr> {
    if is_start_time_bound(&expr) {
        return None;
    }
    match expr {
        Expr::And(left, right) => {
            match (
                strip_time_range_predicates(*left),
                strip_time_range_predicates(*right),
            ) {
                (Some(l), Some(r)) => Some(Expr::and(l, r)),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
        other => Some(other),
    }
}

fn is_start_time_bound(expr: &Expr) -> bool {
    match expr {
        Expr::Eq(left, right)
        | Expr::Ge(left, right)
        | Expr::Gt(left, right)
        | Expr::Le(left, right)
        | Expr::Lt(left, right) => {
            start_time_cmp_value(left, right).is_some()
                || literal_cmp_start_time_value(left, right).is_some()
        }
        _ => false,
    }
}

fn parse_projection(items: &[SelectItem]) -> Result<Projection, ZenError> {
    if items.iter().any(|i| matches!(i, SelectItem::Wildcard(_))) {
        return Ok(Projection::star());
    }
    let mut cols = Vec::new();
    for item in items {
        match item {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(id)) => cols.push(id.value.clone()),
            SelectItem::ExprWithAlias {
                expr: SqlExpr::Identifier(id),
                alias,
            } => {
                let _ = alias;
                cols.push(id.value.clone());
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let _ = expr;
                cols.push(alias.value.clone());
            }
            SelectItem::UnnamedExpr(e) => {
                // For a function call, use a synthetic name based on the function.
                cols.push(format!("{e}").replace(' ', "_"));
            }
            other => {
                let _ = other;
                cols.push("col".into());
            }
        }
    }
    Ok(Projection::list(cols))
}

fn detect_aggregates(items: &[SelectItem], out: &mut Vec<(String, AggregateFn)>) {
    for item in items {
        let (alias_or_name, expr) = match item {
            SelectItem::UnnamedExpr(e) => (format!("{e}"), e),
            SelectItem::ExprWithAlias { expr, alias } => (alias.value.clone(), expr),
            _ => continue,
        };
        if let SqlExpr::Function(f) = expr {
            let name = f.name.to_string();
            let lower = name.to_lowercase();
            let arg_col = arg_first_ident(&f.args);
            match lower.as_str() {
                "count" => out.push((alias_or_name, AggregateFn::Count)),
                "sum" => {
                    if let Some(c) = arg_col {
                        out.push((alias_or_name, AggregateFn::Sum(c)));
                    }
                }
                "avg" => {
                    if let Some(c) = arg_col {
                        out.push((alias_or_name, AggregateFn::Avg(c)));
                    }
                }
                "min" => {
                    if let Some(c) = arg_col {
                        out.push((alias_or_name, AggregateFn::Min(c)));
                    }
                }
                "max" => {
                    if let Some(c) = arg_col {
                        out.push((alias_or_name, AggregateFn::Max(c)));
                    }
                }
                "percentile" => {
                    let (col, q) = arg_col_and_float(&f.args);
                    if let (Some(col), Some(q)) = (col, q) {
                        out.push((alias_or_name, AggregateFn::Percentile { column: col, q }));
                    }
                }
                _ => {}
            }
        }
    }
}

fn arg_first_ident(args: &FunctionArguments) -> Option<String> {
    if let FunctionArguments::List(list) = args {
        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Identifier(id)))) =
            list.args.first()
        {
            return Some(id.value.clone());
        }
    }
    None
}

fn arg_col_and_float(args: &FunctionArguments) -> (Option<String>, Option<f64>) {
    let mut col = None;
    let mut q = None;
    if let FunctionArguments::List(list) = args {
        for (i, a) in list.args.iter().enumerate() {
            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = a {
                if i == 0 {
                    if let SqlExpr::Identifier(id) = e {
                        col = Some(id.value.clone());
                    }
                } else if i == 1 {
                    if let SqlExpr::Value(Value::Number(n, _)) = e {
                        q = n.parse::<f64>().ok();
                    }
                }
            }
        }
    }
    (col, q)
}

/// Maximum recursion depth for `parse_expr`. Hard cap to prevent
/// stack overflow on adversarial nested expressions like
/// `((((((... a OR b))))))`. Real queries fit in <20 levels.
const MAX_EXPR_DEPTH: u32 = 64;

fn parse_expr(e: &SqlExpr) -> Result<Expr, ZenError> {
    parse_expr_rec(e, 0)
}

fn parse_expr_rec(e: &SqlExpr, depth: u32) -> Result<Expr, ZenError> {
    if depth > MAX_EXPR_DEPTH {
        return Err(ZenError::query(format!(
            "expression too deeply nested (>{MAX_EXPR_DEPTH} levels) — possible adversarial input"
        )));
    }
    match e {
        SqlExpr::Identifier(id) => Ok(Expr::col(id.value.clone())),
        SqlExpr::CompoundIdentifier(parts) => {
            // metadata.foo.bar — used for JSON-path
            let path = parts
                .iter()
                .map(|i| i.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            Ok(Expr::col(path))
        }
        SqlExpr::Value(v) => match v {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(Expr::Literal(Literal::String(s.clone())))
            }
            Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Expr::Literal(Literal::Int(i)))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Expr::Literal(Literal::Float(f)))
                } else {
                    Err(ZenError::query(format!("bad number: {n}")))
                }
            }
            Value::Boolean(b) => Ok(Expr::Literal(Literal::Bool(*b))),
            Value::Null => Ok(Expr::Literal(Literal::Null)),
            other => Err(ZenError::query(format!("unsupported literal: {other:?}"))),
        },
        SqlExpr::BinaryOp { left, op, right } => {
            let l = parse_expr_rec(left, depth + 1)?;
            let r = parse_expr_rec(right, depth + 1)?;
            // Metadata-prefix → JsonPathEq if both sides match.
            if let (Expr::Column(c), Expr::Literal(Literal::String(v))) = (&l, &r) {
                if c.contains('.') && c.starts_with("metadata.") && matches!(op, BinaryOperator::Eq)
                {
                    let path = c.trim_start_matches("metadata.").to_string();
                    return Ok(Expr::JsonPathEq {
                        path,
                        value: v.clone(),
                    });
                }
            }
            Ok(match op {
                BinaryOperator::Eq => Expr::eq(l, r),
                BinaryOperator::NotEq => Expr::Ne(Box::new(l), Box::new(r)),
                BinaryOperator::Lt => Expr::Lt(Box::new(l), Box::new(r)),
                BinaryOperator::LtEq => Expr::Le(Box::new(l), Box::new(r)),
                BinaryOperator::Gt => Expr::Gt(Box::new(l), Box::new(r)),
                BinaryOperator::GtEq => Expr::Ge(Box::new(l), Box::new(r)),
                BinaryOperator::And => Expr::and(l, r),
                BinaryOperator::Or => Expr::Or(Box::new(l), Box::new(r)),
                other => return Err(ZenError::query(format!("unsupported op: {other:?}"))),
            })
        }
        SqlExpr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr,
        } => Ok(Expr::Not(Box::new(parse_expr_rec(expr, depth + 1)?))),
        SqlExpr::Function(f) => {
            let name = f.name.to_string();
            let name_lower = name.to_lowercase();
            if name_lower == "text_match" {
                let mut args_iter = match &f.args {
                    FunctionArguments::List(l) => l.args.iter(),
                    _ => return Err(ZenError::query("text_match requires args")),
                };
                let col = args_iter
                    .next()
                    .ok_or_else(|| ZenError::query("text_match needs column"))?;
                let qarg = args_iter
                    .next()
                    .ok_or_else(|| ZenError::query("text_match needs query"))?;
                let column = match col {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Identifier(id))) => {
                        id.value.clone()
                    }
                    _ => return Err(ZenError::query("text_match arg 1 must be column")),
                };
                let query = match qarg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(
                        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
                    ))) => s.clone(),
                    _ => return Err(ZenError::query("text_match arg 2 must be string literal")),
                };
                return Ok(Expr::TextMatch { column, query });
            }
            Err(ZenError::query(format!(
                "unsupported function in WHERE: {name}"
            )))
        }
        other => Err(ZenError::query(format!(
            "unsupported expression: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_select() {
        let p = parse_sql(
            "SELECT span_id, model FROM spans WHERE model = 'gpt-4o' AND status = 'error' LIMIT 10",
            7,
        )
        .unwrap();
        assert_eq!(p.tenant_id, 7);
        assert!(p.predicate.is_some());
        assert_eq!(p.limit, Some(10));
        match &p.projection.columns {
            Some(cols) => assert_eq!(cols, &vec!["span_id".to_string(), "model".to_string()]),
            None => panic!(),
        }
    }

    #[test]
    fn parse_text_match() {
        let p = parse_sql(
            "SELECT * FROM spans WHERE text_match(prompt, 'out of memory')",
            1,
        )
        .unwrap();
        let pred = p.predicate.unwrap();
        match pred.expr {
            Expr::TextMatch { column, query } => {
                assert_eq!(column, "prompt");
                assert_eq!(query, "out of memory");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn parse_group_by_count() {
        let p = parse_sql("SELECT model, count(*) AS n FROM spans GROUP BY model", 1).unwrap();
        assert_eq!(p.group_by, vec!["model".to_string()]);
        assert_eq!(p.aggregates.len(), 1);
        match &p.aggregates[0].1 {
            AggregateFn::Count => {}
            _ => panic!("expected count"),
        }
    }

    #[test]
    fn parse_jsonpath_eq() {
        let p = parse_sql("SELECT * FROM spans WHERE metadata.user_id = 'foo'", 1).unwrap();
        let pred = p.predicate.unwrap();
        match pred.expr {
            Expr::JsonPathEq { path, value } => {
                assert_eq!(path, "user_id");
                assert_eq!(value, "foo");
            }
            other => panic!("expected JsonPathEq, got {other:?}"),
        }
    }

    #[test]
    fn extracts_start_time_bounds() {
        let p = parse_sql(
            "SELECT span_id FROM spans WHERE start_time_ms >= 1700 AND start_time_ms < 1800 AND status = 'error'",
            1,
        )
        .unwrap();
        assert_eq!(p.time_min_ms, 1700);
        assert_eq!(p.time_max_ms, 1799);
        assert!(p.predicate.is_some());
        match p.predicate.unwrap().expr {
            Expr::Eq(left, _) => match left.as_ref() {
                Expr::Column(c) => assert_eq!(c, "status"),
                other => panic!("expected status column, got {other:?}"),
            },
            other => panic!("expected remaining status predicate, got {other:?}"),
        }
    }

    #[test]
    fn extracts_reversed_start_time_bounds() {
        let p = parse_sql(
            "SELECT span_id FROM spans WHERE 1700 <= start_time_ms AND 1800 > start_time_ms",
            1,
        )
        .unwrap();
        assert_eq!(p.time_min_ms, 1700);
        assert_eq!(p.time_max_ms, 1799);
        assert!(p.predicate.is_none());
    }
}
