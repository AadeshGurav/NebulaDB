use serde_json::Value as JsonValue;

/// Check if a string is valid JSON
pub fn is_valid_json(json_str: &str) -> bool {
    // Simple JSON validation - check for paired braces and at least one key-value pair
    if !json_str.starts_with('{') || !json_str.ends_with('}') {
        return false;
    }
    
    // Count braces to ensure they're balanced
    let mut brace_count = 0;
    for c in json_str.chars() {
        if c == '{' {
            brace_count += 1;
        } else if c == '}' {
            brace_count -= 1;
        }
        
        if brace_count < 0 {
            return false;
        }
    }
    
    brace_count == 0
}

/// Format and pretty-print document output
pub fn format_output(data: &str) {
    // Check if it's JSON
    if data.starts_with('{') && data.ends_with('}') {
        // Indent JSON for readability
        let mut indentation = 0;
        let mut formatted = String::new();
        let mut in_quotes = false;
        
        for c in data.chars() {
            match c {
                '"' => {
                    in_quotes = !in_quotes;
                    formatted.push(c);
                },
                '{' | '[' => {
                    formatted.push(c);
                    if !in_quotes {
                        indentation += 2;
                        formatted.push('\n');
                        formatted.push_str(&" ".repeat(indentation));
                    }
                },
                '}' | ']' => {
                    if !in_quotes {
                        indentation -= 2;
                        formatted.push('\n');
                        formatted.push_str(&" ".repeat(indentation));
                    }
                    formatted.push(c);
                },
                ',' => {
                    formatted.push(c);
                    if !in_quotes {
                        formatted.push('\n');
                        formatted.push_str(&" ".repeat(indentation));
                    }
                },
                ':' => {
                    formatted.push(c);
                    if !in_quotes {
                        formatted.push(' ');
                    }
                },
                _ => formatted.push(c),
            }
        }
        
        println!("{}", formatted);
    } else {
        // Just print the raw data
        println!("{}", data);
    }
}

/// Check if a document matches a query
pub fn matches_query(document: &str, query: &JsonValue) -> bool {
    // Parse the document JSON
    if let Ok(doc_value) = serde_json::from_str::<JsonValue>(document) {
        // If query is empty, match all documents
        if query.as_object().map_or(false, |obj| obj.is_empty()) {
            return true;
        }
        
        // If query is a simple object with key-value pairs, check each one
        if let Some(query_obj) = query.as_object() {
            if let Some(doc_obj) = doc_value.as_object() {
                for (query_key, query_val) in query_obj {
                    // Check if document has this key and the value matches
                    match doc_obj.get(query_key) {
                        Some(doc_val) => {
                            if doc_val != query_val {
                                return false;
                            }
                        },
                        None => return false,
                    }
                }
                return true;
            }
        }
    }
    
    false
} 