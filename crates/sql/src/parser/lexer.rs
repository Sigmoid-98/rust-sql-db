use std::iter::{Peekable};
use std::str::Chars;

use common_error::RaftDBResult;
use common_error::errinput;

/// The lexer (lexical analyzer) preprocesses raw SQL strings into a sequence of
/// lexical tokens (e.g. keyword, number, string, etc), which are passed on to
/// the SQL parser. In doing so, it strips away basic syntactic noise such as
/// whitespace, case, and quotes, and performs initial symbol validation.
pub struct Lexer<'a> {
    chars: Peekable<Chars<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// A numeric string, with digits, decimal points, and/or exponents. Leading
    /// signs (e.g. -) are separate tokens.
    Number(String),
    /// A Unicode string, with quotes stripped and escape sequences resolved.
    String(String),
    /// An identifier, with any quotes stripped.
    Ident(String),
    /// A SQL keyword.
    Keyword(Keyword),
    /// keyword .
    Period,
    /// keyword =
    Equal,
    /// keyword !=
    NotEqual,
    /// keyword >
    GreaterThan,
    /// keyword >=
    GreaterThanOrEqual,
    /// keyword <
    LessThan,
    /// keyword <=
    LessThanOrEqual,
    /// keyword <>
    LessOrGreaterThan,
    /// keyword +
    Plus,
    /// keyword -
    Minus,
    /// keyword *
    Asterisk,
    /// keyword /
    Slash,
    /// keyword ^
    Caret,
    /// keyword %
    Percent,
    /// keyword !
    Exclamation,
    /// keyword ?
    Question,
    /// keyword ,
    Comma,
    /// keyword ;
    Semicolon,
    /// keyword (
    OpenParen,
    /// keyword )
    CloseParen,
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Token::Number(n) => n,
            Token::String(s) => s,
            Token::Ident(s) => s,
            Token::Keyword(k) => return k.fmt(f),
            Token::Period => ".",
            Token::Equal => "=",
            Token::NotEqual => "!=",
            Token::GreaterThan => ">",
            Token::GreaterThanOrEqual => ">=",
            Token::LessThan => "<",
            Token::LessThanOrEqual => "<=",
            Token::LessOrGreaterThan => "<>",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Asterisk => "*",
            Token::Slash => "/",
            Token::Caret => "^",
            Token::Percent => "%",
            Token::Exclamation => "!",
            Token::Question => "?",
            Token::Comma => ",",
            Token::Semicolon => ";",
            Token::OpenParen => "(",
            Token::CloseParen => ")",
        })
    }
}

impl From<Keyword> for Token {
    fn from(keyword: Keyword) -> Self {
        Self::Keyword(keyword)
    }
}

impl Iterator for Lexer<'_> {
    type Item = RaftDBResult<Token>;

    fn next(&mut self) -> Option<RaftDBResult<Token>> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            // If there's any remaining chars, the lexer didn't recognize them.
            // Otherwise, we're done lexing.
            Ok(None) => self.chars.peek().map(|c| errinput!("unexpected character: {c}")),
            Err(err) => Some(Err(err)),
        }
    }
}

impl<'a> Lexer<'a> {
    /// Creates a new lexer for the given string.
    pub fn new(input: &'a str) -> Lexer<'a> {
        Lexer { chars: input.chars().peekable() }
    }

    /// Returns the next character if it satisfies the predicate.
    fn next_if(&mut self, predicate: impl Fn(char) -> bool) -> Option<char> {
        self.chars.peek().filter(|&&c| predicate(c))?;
        self.chars.next()
    }

    /// Applies a function to the next character, returning its result and
    /// consuming the next character if it's Some.
    fn next_if_map<T>(&mut self, mapper: impl Fn(char) -> Option<T>) -> Option<T> {
        let value = self.chars.peek().and_then(|&c| mapper(c))?;
        self.chars.next();
        Some(value)
    }

    /// Returns true if the next character is the given character, consuming it.
    fn next_is(&mut self, c: char) -> bool {
        self.next_if(|n| n == c).is_some()
    }

    /// Scans the next token, if any.
    fn scan(&mut self) -> RaftDBResult<Option<Token>> {
        // Ignore whitespace.
        self.skip_whitespace();
        // The first character tells us the token type.
        match self.chars.peek() {
            Some('\'') => self.scan_string(),
            Some('"') => self.scan_ident_quoted(),
            Some(c) if c.is_ascii_digit() => Ok(self.scan_number()),
            Some(c) if c.is_alphabetic() => Ok(self.scan_ident_or_keyword()),
            Some(_) => Ok(self.scan_symbol()),
            None => Ok(None)
        }
    }

    /// Scans the next identifier or keyword, if any. It's converted to
    /// lowercase, by SQL convention.
    fn scan_ident_or_keyword(&mut self) -> Option<Token> {
        // The first character must be alphabetic. The rest can be numeric.
        let mut name = self.next_if(|c| c.is_alphabetic())?.to_lowercase().to_string();
        while let Some(c) = self.next_if(|c| c.is_alphanumeric() || c == '_') {
            name.extend(c.to_lowercase())
        }

        // Check if the identifier matches a keyword.
        match Keyword::try_from(name.as_str()).ok() {
            Some(k) => Some(Token::Keyword(k)),
            None => Some(Token::Ident(name)),
        }
        
    }

    /// Scans the next quoted identifier, if any. Case is preserved.
    fn scan_ident_quoted(&mut self) -> RaftDBResult<Option<Token>> {
        if !self.next_is('"') {
            return Ok(None);
        }
        let mut ident = String::new();
        loop {
            match self.chars.next() {
                // "" is the escape sequence for ".
                Some('"') if self.next_is('"') => ident.push('"'),
                Some('"') => break,
                Some(c) => ident.push(c),
                None => return errinput!("unexpected end of quoted identifier"),
            }
        }
        Ok(Some(Token::Ident(ident)))
    }

    /// Scans the next number, if any.
    fn scan_number(&mut self) -> Option<Token> {
        // Scan the integer part. There must be one digit.
        let mut number = self.next_if(|c| c.is_ascii_digit())?.to_string();
        while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
            number.push(c)
        }
        if self.next_is('.') { 
            number.push('.');
            while let Some(dec) = self.next_if(|c| c.is_ascii_digit()) {
                number.push(dec)
            }
        }
        // Scan the exponent, if any.
        if let Some(exp) = self.next_if(|c| c == 'e' || c == 'E') {
            number.push(exp);
            if let Some(sign) = self.next_if(|c| c == '+' || c == '-') {
                number.push(sign)
            }
            while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
                number.push(c)
            }
        }
        Some(Token::Number(number))
    }

    /// Scans the next quoted string literal, if any.
    fn scan_string(&mut self) -> RaftDBResult<Option<Token>> {
        if !self.next_is('\'') { 
            return Ok(None);
        }
        let mut string = String::new();
        loop {
            match self.chars.next() {
                None => return errinput!("unexpected end or string literal"),
                // '' is the escape sequence for '.
                Some('\'') if self.next_is('\'') => string.push('\''),
                Some('\'') => break,
                Some(c) => string.push(c),
            }
        }
        Ok(Some(Token::String(string)))
    }
    
    /// Scans the next symbol token, if any.
    fn scan_symbol(&mut self) -> Option<Token> {
        let mut token = self.next_if_map(|c| {
            Some(match c { 
                '.' => Token::Period,
                '=' => Token::Equal,
                '>' => Token::GreaterThan,
                '<' => Token::LessThan,
                '+' => Token::Plus,
                '-' => Token::Minus,
                '*' => Token::Asterisk,
                '/' => Token::Slash,
                '^' => Token::Caret,
                '%' => Token::Percent,
                '!' => Token::Exclamation,
                '?' => Token::Question,
                ',' => Token::Comma,
                ';' => Token::Semicolon,
                '(' => Token::OpenParen,
                ')' => Token::CloseParen,
                _ => return None,
            })
        })?;
        
        // Handle two-character tokens, e.g. !=.
        token = match token {
            Token::Exclamation if self.next_is('=') => Token::NotEqual,
            Token::GreaterThan if self.next_is('=') => Token::GreaterThanOrEqual,
            Token::LessThan if self.next_is('=') => Token::LessThanOrEqual,
            Token::LessThan if self.next_is('>') => Token::LessOrGreaterThan,
            token => token,
        };
        
        Some(token)
    }
    
    fn skip_whitespace(&mut self) {
        while self.next_if(|c| c.is_whitespace()).is_some() {
        }
    }
}

/// Reserved SQL keywords.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Keyword {
    And,
    As,
    Asc,
    Begin,
    Bool,
    Boolean,
    By,
    Commit,
    Create,
    Cross,
    Default,
    Delete,
    Desc,
    Double,
    Drop,
    Exists,
    Explain,
    False,
    Float,
    From,
    Group,
    Having,
    If,
    Index,
    Infinity,
    Inner,
    Insert,
    Int,
    Integer,
    Into,
    Is,
    Join,
    Key,
    Left,
    Like,
    Limit,
    NaN,
    Not,
    Null,
    Of,
    Offset,
    On,
    Only,
    Or,
    Order,
    Outer,
    Primary,
    Read,
    References,
    Right,
    Rollback,
    Select,
    Set,
    String,
    System,
    Table,
    Text,
    Time,
    Transaction,
    True,
    Unique,
    Update,
    Values,
    Varchar,
    Where,
    Write,
}

impl TryFrom<&str> for Keyword {
    // Use a cheap static string, since this just indicates it's not a keyword.
    type Error = &'static str;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        // Only compare lowercase, which is enforced by the lexer. This avoids
        // allocating a string to change the case. Assert this.
        debug_assert!(value.chars().all(|c| !c.is_uppercase()), "keyword must be lowercase");
        Ok(match value {
            "as" => Self::As,
            "asc" => Self::Asc,
            "and" => Self::And,
            "begin" => Self::Begin,
            "bool" => Self::Bool,
            "boolean" => Self::Boolean,
            "by" => Self::By,
            "commit" => Self::Commit,
            "create" => Self::Create,
            "cross" => Self::Cross,
            "default" => Self::Default,
            "delete" => Self::Delete,
            "desc" => Self::Desc,
            "double" => Self::Double,
            "drop" => Self::Drop,
            "exists" => Self::Exists,
            "explain" => Self::Explain,
            "false" => Self::False,
            "float" => Self::Float,
            "from" => Self::From,
            "group" => Self::Group,
            "having" => Self::Having,
            "if" => Self::If,
            "index" => Self::Index,
            "infinity" => Self::Infinity,
            "inner" => Self::Inner,
            "insert" => Self::Insert,
            "int" => Self::Int,
            "integer" => Self::Integer,
            "into" => Self::Into,
            "is" => Self::Is,
            "join" => Self::Join,
            "key" => Self::Key,
            "left" => Self::Left,
            "like" => Self::Like,
            "limit" => Self::Limit,
            "nan" => Self::NaN,
            "not" => Self::Not,
            "null" => Self::Null,
            "of" => Self::Of,
            "offset" => Self::Offset,
            "on" => Self::On,
            "only" => Self::Only,
            "or" => Self::Or,
            "order" => Self::Order,
            "outer" => Self::Outer,
            "primary" => Self::Primary,
            "read" => Self::Read,
            "references" => Self::References,
            "right" => Self::Right,
            "rollback" => Self::Rollback,
            "select" => Self::Select,
            "set" => Self::Set,
            "string" => Self::String,
            "system" => Self::System,
            "table" => Self::Table,
            "text" => Self::Text,
            "time" => Self::Time,
            "transaction" => Self::Transaction,
            "true" => Self::True,
            "unique" => Self::Unique,
            "update" => Self::Update,
            "values" => Self::Values,
            "varchar" => Self::Varchar,
            "where" => Self::Where,
            "write" => Self::Write,
            _ => return Err("not a keyword"),
        })
    }
}

impl std::fmt::Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Keyword::And => "AND",
            Keyword::As => "AS",
            Keyword::Asc => "ASC",
            Keyword::Begin => "BEGIN",
            Keyword::Bool => "BOOL",
            Keyword::Boolean => "BOOLEAN",
            Keyword::By => "BY",
            Keyword::Commit => "COMMIT",
            Keyword::Create => "CREATE",
            Keyword::Cross => "CROSS",
            Keyword::Default => "DEFAULT",
            Keyword::Delete => "DELETE",
            Keyword::Desc => "DESC",
            Keyword::Double => "DOUBLE",
            Keyword::Drop => "DROP",
            Keyword::Exists => "EXISTS",
            Keyword::Explain => "EXPLAIN",
            Keyword::False => "FALSE",
            Keyword::Float => "FLOAT",
            Keyword::From => "FROM",
            Keyword::Group => "GROUP",
            Keyword::Having => "HAVING",
            Keyword::If => "IF",
            Keyword::Index => "INDEX",
            Keyword::Infinity => "INFINITY",
            Keyword::Inner => "INNER",
            Keyword::Insert => "INSERT",
            Keyword::Int => "INT",
            Keyword::Integer => "INTEGER",
            Keyword::Into => "INTO",
            Keyword::Is => "IS",
            Keyword::Join => "JOIN",
            Keyword::Key => "KEY",
            Keyword::Left => "LEFT",
            Keyword::Like => "LIKE",
            Keyword::Limit => "LIMIT",
            Keyword::NaN => "NAN",
            Keyword::Not => "NOT",
            Keyword::Null => "NULL",
            Keyword::Of => "OF",
            Keyword::Offset => "OFFSET",
            Keyword::On => "ON",
            Keyword::Only => "ONLY",
            Keyword::Or => "OR",
            Keyword::Order => "ORDER",
            Keyword::Outer => "OUTER",
            Keyword::Primary => "PRIMARY",
            Keyword::Read => "READ",
            Keyword::References => "REFERENCES",
            Keyword::Right => "RIGHT",
            Keyword::Rollback => "ROLLBACK",
            Keyword::Select => "SELECT",
            Keyword::Set => "SET",
            Keyword::String => "STRING",
            Keyword::System => "SYSTEM",
            Keyword::Table => "TABLE",
            Keyword::Text => "TEXT",
            Keyword::Time => "TIME",
            Keyword::Transaction => "TRANSACTION",
            Keyword::True => "TRUE",
            Keyword::Unique => "UNIQUE",
            Keyword::Update => "UPDATE",
            Keyword::Values => "VALUES",
            Keyword::Varchar => "VARCHAR",
            Keyword::Where => "WHERE",
            Keyword::Write => "WRITE",
        })
    }
}


/// Returns true if the entire given string is a single valid identifier.
pub fn is_ident(ident: &str) -> bool {
    let mut lexer = Lexer::new(ident);
    let Some(Ok(Token::Ident(_))) = lexer.next() else {
        return false;
    };
    lexer.next().is_none()
}