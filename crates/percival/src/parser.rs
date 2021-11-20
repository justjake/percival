//! Parser definitions and error recovery for Percival.

use ariadne::{Color, Fmt, Label, Report, ReportKind, Source};
use chumsky::prelude::*;

use crate::ast::{Fact, Program, Rule, Value};

/// Constructs a parser combinator for the Percival language.
pub fn parser() -> impl Parser<char, Program, Error = Simple<char>> {
    let id = text::ident().labelled("ident");

    let value = id.map(Value::Id).labelled("value");

    let prop = id
        .then(just(':').padded().ignore_then(value).or_not())
        .map(|(id, value)| (id.clone(), value.unwrap_or(Value::Id(id))))
        .labelled("prop");

    let fact = text::ident()
        .then(prop.padded().separated_by(just(',')).delimited_by('(', ')'))
        .map(|(name, props)| Fact {
            name,
            props: props.into_iter().collect(),
        })
        .labelled("fact");

    let rule = fact
        .then_ignore(seq(":-".chars()).padded())
        .then(fact.padded().separated_by(just(',')))
        .then_ignore(just('.'))
        .map(|(head, clauses)| Rule { head, clauses })
        .labelled("rule");

    rule.padded()
        .repeated()
        .map(|rules| Program { rules })
        .then_ignore(end())
}

/// Format parser errors into a human-readable message.
pub fn format_errors(src: &str, errors: Vec<Simple<char>>) -> String {
    let mut reports = vec![];

    for e in errors {
        let e = e.map(|tok| tok.to_string());
        let report = Report::build(ReportKind::Error, (), e.span().start);

        let report = match e.reason() {
            chumsky::error::SimpleReason::Unclosed { span, delimiter } => report
                .with_message(format!(
                    "Unclosed delimiter {}",
                    delimiter.fg(Color::Yellow)
                ))
                .with_label(
                    Label::new(span.clone())
                        .with_message(format!(
                            "Unclosed delimiter {}",
                            delimiter.fg(Color::Yellow)
                        ))
                        .with_color(Color::Yellow),
                )
                .with_label(
                    Label::new(e.span())
                        .with_message(format!(
                            "Must be closed before this {}",
                            e.found()
                                .unwrap_or(&"end of file".to_string())
                                .fg(Color::Red)
                        ))
                        .with_color(Color::Red),
                ),
            chumsky::error::SimpleReason::Unexpected => report
                .with_message(format!(
                    "{}, expected {}",
                    if e.found().is_some() {
                        "Unexpected token in input"
                    } else {
                        "Unexpected end of input"
                    },
                    if e.expected().len() == 0 {
                        "end of input".to_string()
                    } else {
                        e.expected()
                            .map(|x| x.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    }
                ))
                .with_label(
                    Label::new(e.span())
                        .with_message(format!(
                            "Unexpected token {}",
                            e.found()
                                .unwrap_or(&"end of file".to_string())
                                .fg(Color::Red)
                        ))
                        .with_color(Color::Red),
                ),
            chumsky::error::SimpleReason::Custom(msg) => report.with_message(msg).with_label(
                Label::new(e.span())
                    .with_message(format!("{}", msg.fg(Color::Red)))
                    .with_color(Color::Red),
            ),
        };

        let mut buf = vec![];
        report.finish().write(Source::from(&src), &mut buf).unwrap();
        reports.push(std::str::from_utf8(&buf[..]).unwrap().to_string());
    }

    reports.join("\n")
}

#[cfg(test)]
mod tests {
    use maplit::btreemap;

    use super::*;

    #[test]
    fn parse_single_rule() {
        let parser = parser();
        let result = parser.parse("tc(x, y) :- tc(x, y: z), edge(x: z, y).");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Program {
                rules: vec![Rule {
                    head: Fact {
                        name: "tc".into(),
                        props: btreemap! {
                            "x".into() => Value::Id("x".into()),
                            "y".into() => Value::Id("y".into()),
                        },
                    },
                    clauses: vec![
                        Fact {
                            name: "tc".into(),
                            props: btreemap! {
                                "x".into() => Value::Id("x".into()),
                                "y".into() => Value::Id("z".into()),
                            },
                        },
                        Fact {
                            name: "edge".into(),
                            props: btreemap! {
                                "x".into() => Value::Id("z".into()),
                                "y".into() => Value::Id("y".into()),
                            },
                        },
                    ],
                }],
            },
        );
    }

    #[test]
    fn parse_err() {
        let parser = parser();
        let text = "tc(x, y) :- f(.
tc(z) :- tc(z, &).";
        let (_, errors) = parser.parse_recovery(text);
        assert!(errors.len() == 1);
        let message = format_errors(text, errors);
        assert!(message.contains("Unexpected token in input, expected )"));
    }
}