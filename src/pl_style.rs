use std::process::Command;
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeQualityResult {
    pub language: String,
    pub compiles: bool,
    pub syntax_errors: Vec<String>,
    pub style_score: f64,
    pub comment_ratio: f64,
    pub final_score: f64,
}

pub trait LanguageAnalyzer {
    fn check_syntax(&self, code: &str) -> Result<(bool, Vec<String>)>;
    fn get_style_score(&self, code: &str) -> Result<f64>;
    fn calculate_comment_ratio(&self, code: &str) -> f64;
}

// Tree-sitter based analyzer for multiple languages
pub struct TreeSitterAnalyzer {
    language: tree_sitter::Language,
    language_name: String,
}

impl TreeSitterAnalyzer {
    pub fn new(language: tree_sitter::Language, name: &str) -> Self {
        Self {
            language,
            language_name: name.to_string(),
        }
    }
}

impl LanguageAnalyzer for TreeSitterAnalyzer {
    fn check_syntax(&self, code: &str) -> Result<(bool, Vec<String>)> {
        let mut parser = tree_sitter::Parser::new();
        parser.set_language(self.language)?;
        
        let tree = parser.parse(code, None)
            .context("Failed to parse code")?;
        
        let root = tree.root_node();
        let mut errors = Vec::new();
        
        // Walk the tree to find ERROR nodes
        let mut cursor = root.walk();
        let mut to_visit = vec![root];
        
        while let Some(node) = to_visit.pop() {
            if node.kind() == "ERROR" {
                errors.push(format!(
                    "Syntax error at line {}: {}",
                    node.start_position().row + 1,
                    node.utf8_text(code.as_bytes()).unwrap_or("<invalid>")
                ));
            }
            
            for i in 0..node.child_count() {
                if let Some(child) = node.child(i) {
                    to_visit.push(child);
                }
            }
        }
        
        Ok((errors.is_empty(), errors))
    }
    
    fn get_style_score(&self, code: &str) -> Result<f64> {
        // Basic style scoring based on tree-sitter AST
        // You can enhance this with language-specific rules
        let mut parser = tree_sitter::Parser::new();
        parser.set_language(self.language)?;
        
        let tree = parser.parse(code, None)
            .context("Failed to parse for style analysis")?;
        
        // Simple scoring: penalize very long lines, deep nesting, etc.
        let lines: Vec<&str> = code.lines().collect();
        let mut score = 100.0;
        
        // Penalize long lines
        for line in &lines {
            if line.len() > 100 {
                score -= 0.5;
            }
            if line.len() > 120 {
                score -= 1.0;
            }
        }
        
        // Add more sophisticated scoring based on AST analysis
        // This is a simplified example
        
        Ok(score.max(0.0).min(100.0) / 100.0)
    }
    
    fn calculate_comment_ratio(&self, code: &str) -> f64 {
        // Language-specific comment detection
        let comment_patterns = match self.language_name.as_str() {
            "C" | "C++" | "C-Sharp" | "Java" | "JavaScript" | "TypeScript" | "Rust" | "Go" | "Swift" => {
                vec![r"//.*$", r"/\*[\s\S]*?\*/"]
            },
            "Python" | "Ruby" | "Shell" => vec![r"#.*$"],
            "SQL" => vec![r"--.*$", r"/\*[\s\S]*?\*/"],
            "PHP" => vec![r"//.*$", r"#.*$", r"/\*[\s\S]*?\*/"],
            _ => vec![],
        };
        
        let lines: Vec<&str> = code.lines().collect();
        let total_lines = lines.len() as f64;
        let mut comment_lines = 0;
        
        for line in lines {
            let trimmed = line.trim();
            for pattern in &comment_patterns {
                if let Ok(re) = regex::Regex::new(pattern) {
                    if re.is_match(trimmed) {
                        comment_lines += 1;
                        break;
                    }
                }
            }
        }
        
        comment_lines as f64 / total_lines.max(1.0)
    }
}

// Specialized analyzers for languages with better native support

pub struct RustAnalyzer;

impl LanguageAnalyzer for RustAnalyzer {
    fn check_syntax(&self, code: &str) -> Result<(bool, Vec<String>)> {
        match syn::parse_file(code) {
            Ok(_) => Ok((true, vec![])),
            Err(e) => Ok((false, vec![format!("Syntax error: {}", e)])),
        }
    }
    
    fn get_style_score(&self, code: &str) -> Result<f64> {
        // Run clippy via command
        // In practice, you'd write to a temp file
        // This is a simplified version
        Ok(0.85) // Placeholder
    }
    
    fn calculate_comment_ratio(&self, code: &str) -> f64 {
        let lines: Vec<&str> = code.lines().collect();
        let total = lines.len() as f64;
        let comments = lines.iter()
            .filter(|l| l.trim().starts_with("//") || l.trim().starts_with("/*"))
            .count() as f64;
        comments / total.max(1.0)
    }
}

pub struct JavaScriptAnalyzer;

impl LanguageAnalyzer for JavaScriptAnalyzer {
    fn check_syntax(&self, code: &str) -> Result<(bool, Vec<String>)> {
        // Using swc for JavaScript/TypeScript
        use swc_ecma_parser::{lexer::Lexer, Parser, StringInput, Syntax};
        use swc_common::{sync::Lrc, SourceMap, FileName};
        
        let cm: Lrc<SourceMap> = Default::default();
        let fm = cm.new_source_file(FileName::Custom("test.js".into()), code.into());
        
        let lexer = Lexer::new(
            Syntax::Es(Default::default()),
            Default::default(),
            StringInput::from(&*fm),
            None,
        );
        
        let mut parser = Parser::new_from(lexer);
        
        match parser.parse_module() {
            Ok(_) => Ok((true, vec![])),
            Err(e) => Ok((false, vec![format!("Parse error: {:?}", e)])),
        }
    }
    
    fn get_style_score(&self, code: &str) -> Result<f64> {
        // Could integrate oxc linter here
        Ok(0.9) // Placeholder
    }
    
    fn calculate_comment_ratio(&self, code: &str) -> f64 {
        let lines: Vec<&str> = code.lines().collect();
        let total = lines.len() as f64;
        let comments = lines.iter()
            .filter(|l| {
                let trimmed = l.trim();
                trimmed.starts_with("//") || trimmed.starts_with("/*") || trimmed.starts_with("*")
            })
            .count() as f64;
        comments / total.max(1.0)
    }
}

// Main analyzer that dispatches to language-specific implementations
pub struct CodeQualityAnalyzer {
    analyzers: std::collections::HashMap<String, Box<dyn LanguageAnalyzer>>,
}

impl CodeQualityAnalyzer {
    pub fn new() -> Self {
        let mut analyzers: std::collections::HashMap<String, Box<dyn LanguageAnalyzer>> = 
            std::collections::HashMap::new();
        
        // Register analyzers
        analyzers.insert("Rust".to_string(), Box::new(RustAnalyzer));
        analyzers.insert("JavaScript".to_string(), Box::new(JavaScriptAnalyzer));
        
        // Register tree-sitter based analyzers
        // You'll need to add the appropriate tree-sitter-* crates
        /*
        analyzers.insert(
            "C".to_string(), 
            Box::new(TreeSitterAnalyzer::new(tree_sitter_c::language(), "C"))
        );
        analyzers.insert(
            "Python".to_string(),
            Box::new(TreeSitterAnalyzer::new(tree_sitter_python::language(), "Python"))
        );
        */
        
        Self { analyzers }
    }
    
    pub fn analyze(&self, code: &str, language: &str) -> Result<CodeQualityResult> {
        let analyzer = self.analyzers.get(language)
            .context(format!("No analyzer for language: {}", language))?;
        
        let (compiles, syntax_errors) = analyzer.check_syntax(code)?;
        let style_score = if compiles {
            analyzer.get_style_score(code)?
        } else {
            0.0
        };
        
        let comment_ratio = analyzer.calculate_comment_ratio(code);
        let final_score = style_score * (1.0 - comment_ratio);
        
        Ok(CodeQualityResult {
            language: language.to_string(),
            compiles,
            syntax_errors,
            style_score,
            comment_ratio,
            final_score,
        })
    }
}

