/* Some code for scoring of python text data 
(lots of small utilities here, so it's better if we break this into a separate file)
*/

use std::io::Write;
use std::process::{Command, Stdio};
use serde::{Deserialize};
use anyhow::{Error, Result};


/*==============================================
=                  RUFF STUFF                  =
==============================================*/
/*
Ruff linting and scoring utilities. 
Should be able to 
- Run ruff on a string and get a list of messages and their codes out
- Use this to calculate a "score" from 0-10 on code cleanliness
Code mostly LLM generated, with some manual cleanups 

*/

const ERROR_WEIGHT: f64 = 5.0;
const OTHER_WEIGHT: f64 = 1.0;
const ERR_KEYS: [&str; 3] = ["F", "E9", "B0"];

#[derive(Debug, Deserialize)]
pub struct RuffScoreResult {
    pub score: f64,
    pub comment_score: f64,
    pub error_count: usize,
    pub other_count: usize,
    pub total_statements: usize,
    pub total_comments: usize,
}

impl RuffScoreResult {

	fn make_err() -> Self {
		RuffScoreResult {score: -1.0,
					 comment_score: -1.0,
					 error_count: 0,
					 other_count: 0,
					 total_statements: 0,
					 total_comments: 0}
	}

    fn print_summary(&self) {
        println!("Pylint-style Score: {:.2}/10.0", self.score);
        println!("Comment-adjusted Score: {:.2}/10.0", self.comment_score);
        println!("Breakdown:");
        println!("  Errors: {} (weight: 5.0)", self.error_count);
        println!("  Other: {} (weight: 1.0)", self.other_count);
        println!("  Total Statements: {}", self.total_statements);
        println!("  Total Comments: {}", self.total_comments);
    }
}

#[derive(Debug, Deserialize)]
struct RuffMessage {
    code: String,
    message: String,
}


pub fn run_ruff_on_string(code: &str) -> Result<RuffScoreResult, Error> {
	// First make code string into something that I can pass to ruffI

    let mut child = Command::new("ruff")
        .arg("check")
        .arg("--output-format")
        .arg("json")        
        .arg("--stdin-filename")
        .arg("temp.py") // Provide a filename for ruff to use for rule matching
        .arg("-") // Read from stdin
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    // Write the Python code to stdin and ensure it's closed
    if let Some(stdin) = child.stdin.take() {
        let mut stdin = stdin;
        stdin.write_all(code.as_bytes())?; // Handle error properly
        stdin.flush()?; // Explicitly flush
        drop(stdin); // Explicitly close stdin
    }
    let output = child.wait_with_output().unwrap(); 


	let ruff_messages: Vec<RuffMessage> = serde_json::from_str::<Vec<RuffMessage>>(&String::from_utf8_lossy(&output.stdout)).unwrap();

	let mut error_count = 0;
	let mut other_count = 0;
	for message in ruff_messages {
		if message.code.starts_with("F0") {
			error_count += 1_000_000;
		}
		else if ERR_KEYS.iter().any(|key| message.code.starts_with(key)) {
			error_count += 1;
		} else {
			other_count += 1;
		}
	}
	let (total_statements, total_comments) = count_python_statements_and_comments(code);
	let mut comment_score = 0.0;
	let mut score = 0.0;
	if total_statements > 0 {
		let comment_penalty = 1.0 - (total_comments) as f64 / (total_statements + total_comments) as f64;
		let total_penalty = ERROR_WEIGHT * error_count as f64 + OTHER_WEIGHT * other_count as f64;
		score = 0.0_f64.max(10.0 - 10.0 * total_penalty / total_statements as f64);
		comment_score = score * comment_penalty;
	}


	Ok(RuffScoreResult {score, comment_score, error_count, other_count, total_statements, total_comments})
}





pub fn count_python_statements_and_comments(python_code: &str) -> (usize, usize) {
    let mut statement_count = 0;
    let mut comment_count = 0;
    
    for line in python_code.lines() {
        let trimmed = line.trim();
        
        // Skip empty lines
        if trimmed.is_empty() {
            continue;
        }
        
        // Check if line starts with # (pure comment line)
        if trimmed.starts_with('#') {
            comment_count += 1;
            continue;
        }
        
        // For lines that might have both code and comments
        let mut has_code = false;
        let mut has_comment = false;
        let mut chars = trimmed.chars().peekable();
        let mut in_string = false;
        let mut string_char = '\0';
        let mut escape_next = false;
        
        while let Some(ch) = chars.next() {
            if escape_next {
                escape_next = false;
                continue;
            }
            
            match ch {
                '\\' if in_string => {
                    escape_next = true;
                }
                '"' | '\'' => {
                    if !in_string {
                        // Check for triple quotes
                        if chars.peek() == Some(&ch) {
                            chars.next(); // consume second quote
                            if chars.peek() == Some(&ch) {
                                chars.next(); // consume third quote
                                // Skip to end of triple quote (simplified)
                                let triple_quote = format!("{}{}{}", ch, ch, ch);
                                let remaining: String = chars.collect();
                                if remaining.contains(&triple_quote) {
                                    has_code = true;
                                    break;
                                } else {
                                    // Triple quote string continues to end of line
                                    has_code = true;
                                    break;
                                }
                            } else {
                                // Two quotes - empty string
                                has_code = true;
                                continue;
                            }
                        } else {
                            // Start of single/double quote string
                            in_string = true;
                            string_char = ch;
                            has_code = true;
                        }
                    } else if ch == string_char {
                        // End of string
                        in_string = false;
                        string_char = '\0';
                    }
                }
                '#' if !in_string => {
                    // Found comment outside of string
                    has_comment = true;
                    break;
                }
                c if !c.is_whitespace() && !in_string => {
                    has_code = true;
                }
                _ => continue,
            }
        }
        
        if has_code {
            statement_count += 1;
        }
        if has_comment {
            comment_count += 1;
        }
    }
    
    (statement_count, comment_count)
}






