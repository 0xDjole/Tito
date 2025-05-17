pub struct KeyEncoder {
    separator: char,
    escape_char: char,
}

impl KeyEncoder {
    pub fn new() -> Self {
        Self {
            separator: ':',
            escape_char: '\\',
        }
    }

    /// Encode a single component, escaping any special characters
    pub fn encode_component(&self, component: &str) -> String {
        let mut result = String::with_capacity(component.len() * 2);

        for ch in component.chars() {
            if ch == self.separator || ch == self.escape_char {
                result.push(self.escape_char);
            }
            result.push(ch);
        }

        result
    }

    /// Decode a component by interpreting escape sequences
    pub fn decode_component(&self, component: &str) -> String {
        let mut result = String::with_capacity(component.len());
        let mut escaped = false;

        for ch in component.chars() {
            if escaped {
                result.push(ch);
                escaped = false;
            } else if ch == self.escape_char {
                escaped = true;
            } else {
                result.push(ch);
            }
        }

        result
    }

    /// Build a key from multiple components
    pub fn build_key(&self, components: &[&str]) -> String {
        components
            .iter()
            .map(|comp| self.encode_component(comp))
            .collect::<Vec<_>>()
            .join(&self.separator.to_string())
    }

    /// Parse a key into its component parts
    pub fn parse_key(&self, key: &str) -> Vec<String> {
        let mut components = Vec::new();
        let mut current_component = String::new();
        let mut escaped = false;

        for ch in key.chars() {
            if escaped {
                current_component.push(ch);
                escaped = false;
            } else if ch == self.escape_char {
                escaped = true;
            } else if ch == self.separator {
                components.push(current_component);
                current_component = String::new();
            } else {
                current_component.push(ch);
            }
        }

        // Don't forget the last component
        if !current_component.is_empty() {
            components.push(current_component);
        }

        components
    }
}
