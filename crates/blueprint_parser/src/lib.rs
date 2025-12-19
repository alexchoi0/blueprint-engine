use blueprint_core::{BlueprintError, Result, SourceLocation, Span};
use starlark_syntax::codemap::CodeMap;
use starlark_syntax::dialect::{Dialect, DialectTypes};
use starlark_syntax::syntax::module::AstModuleFields;
use starlark_syntax::syntax::AstModule;

pub use starlark_syntax::syntax::ast::{
    AstArgument, AstExpr, AstLiteral, AstParameter, AstPayload, AstStmt, AssignOp,
    AssignTarget, AssignTargetP, Clause, Expr, ExprP, ForClause, Parameter, ParameterP, Stmt,
    StmtP,
};
pub use starlark_syntax::syntax::def::{DefParam, DefParams};

pub struct ParsedModule {
    pub codemap: CodeMap,
    pub statement: AstStmt,
}

impl ParsedModule {
    pub fn statements(&self) -> &AstStmt {
        &self.statement
    }
}

pub fn parse(filename: &str, content: &str) -> Result<ParsedModule> {
    let dialect = Dialect {
        enable_f_strings: true,
        enable_lambda: true,
        enable_keyword_only_arguments: true,
        enable_top_level_stmt: true,
        enable_types: DialectTypes::Enable,
        ..Dialect::Standard
    };

    match AstModule::parse(filename, content.to_owned(), &dialect) {
        Ok(module) => {
            let (codemap, statement, _dialect, _) = module.into_parts();
            Ok(ParsedModule { codemap, statement })
        }
        Err(e) => {
            let location = SourceLocation {
                file: Some(filename.to_string()),
                line: 1,
                column: 1,
                span: None,
            };
            Err(BlueprintError::ParseError {
                location,
                message: e.to_string(),
            })
        }
    }
}

pub fn get_location(codemap: &CodeMap, span: starlark_syntax::codemap::Span) -> SourceLocation {
    let file_span = codemap.file_span(span);
    let loc = file_span.resolve();
    SourceLocation {
        file: Some(loc.file.clone()),
        line: loc.span.begin.line + 1,
        column: loc.span.begin.column + 1,
        span: Some(Span {
            start: span.begin().get() as usize,
            end: span.end().get() as usize,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple() {
        let result = parse("test.star", "x = 1 + 2");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_function() {
        let code = r#"
def hello(name):
    print("Hello, " + name)
"#;
        let result = parse("test.star", code);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_lambda() {
        let result = parse("test.star", "f = lambda x: x * 2");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_error() {
        let result = parse("test.star", "x = ");
        assert!(result.is_err());
    }
}
