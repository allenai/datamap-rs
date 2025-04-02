use datamap_rs::group_map_fxn::extract_python_imports;
#[cfg(test)]
mod tests {
    use std::error::Error;
use super::*;

    fn test_extraction(input: &str, expected: Vec<&str>) -> Result<(), Box<dyn Error>> {
        let content = input.to_string();
        let filename = "test.py".to_string();
        let imports = extract_python_imports(&content, &filename)?;
        
        // Convert expected &str to String for comparison
        let expected: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
        
        assert_eq!(imports, expected, 
            "\nExtracted imports: {:?}\nExpected imports: {:?}", imports, expected);
        Ok(())
    }

    #[test]
    fn test_basic_imports() -> Result<(), Box<dyn Error>> {
        let input = r#"import os
import sys
import numpy as np
import pandas"#;
        
        test_extraction(input, vec!["os", "sys", "numpy", "pandas"])
    }

    #[test]
    fn test_from_imports() -> Result<(), Box<dyn Error>> {
        let input = r#"from os import path
from sys import argv, exit
from numpy import array as arr"#;
        
        test_extraction(input, vec![
            "from os import path", 
            "from sys import argv, exit", 
            "from numpy import array as arr"
        ])
    }

    #[test]
    fn test_relative_imports() -> Result<(), Box<dyn Error>> {
        let input = r#"from . import utils
from .. import config
from .models import User
from ..services.auth import authenticate"#;
        
        test_extraction(input, vec![
            "from . import utils",
            "from .. import config",
            "from .models import User",
            "from ..services.auth import authenticate"
        ])
    }

    #[test]
    fn test_deep_relative_imports() -> Result<(), Box<dyn Error>> {
        let input = r#"from ... import base
from .... import constants"#;
        
        test_extraction(input, vec![
            "from ... import base",
            "from .... import constants"
        ])
    }

    #[test]
    fn test_mixed_imports() -> Result<(), Box<dyn Error>> {
        let input = r#"import os
from sys import argv
import numpy as np
from . import utils
from pathlib import Path, PurePath"#;
        
        test_extraction(input, vec![
            "os",
            "from sys import argv",
            "numpy",
            "from . import utils",
            "from pathlib import Path, PurePath"
        ])
    }

    #[test]
    fn test_imports_with_comments() -> Result<(), Box<dyn Error>> {
        let input = r#"# Essential imports
import os  # Operating system interface
import sys  # System-specific parameters

# Data processing
from pandas import DataFrame  # For data manipulation
import numpy as np  # Numerical computations"#;
        
        test_extraction(input, vec![
            "os",
            "sys",
            "from pandas import DataFrame",
            "numpy"
        ])
    }

    #[test]
    fn test_imports_with_code() -> Result<(), Box<dyn Error>> {
        let input = r#"import os

def main():
    print("Hello world")
    
import sys  # This should be detected

class MyClass:
    def __init__(self):
        from datetime import datetime  # This should NOT be detected (not top-level)
        self.time = datetime.now()
        
from json import loads  # This should be detected"#;
        
        test_extraction(input, vec![
            "os",
            "sys",
            "from json import loads"
        ])
    }

    #[test]
    fn test_multiline_imports() -> Result<(), Box<dyn Error>> {
        let input = r#"from package import (
    module1,
    module2,
    module3 as m3
)"#;
        
        test_extraction(input, vec![
            "from package import module1, module2, module3 as m3"
        ])
    }

    #[test]
    fn test_complex_cases() -> Result<(), Box<dyn Error>> {
        let input = r#"import os.path
import sys as system
from os.path import (
    join,
    dirname as dir_name
)
from .. import (models, utils)"#;
        
        test_extraction(input, vec![
            "os.path",
            "sys",
            "from os.path import join, dirname as dir_name",
            "from .. import models, utils"
        ])
    }

    #[test]
    fn test_empty_file() -> Result<(), Box<dyn Error>> {
        let input = r#""#;
        test_extraction(input, vec![])
    }

    #[test]
    fn test_file_with_no_imports() -> Result<(), Box<dyn Error>> {
        let input = r#"# This is a comment
def hello():
    print("Hello world")

class TestClass:
    pass
"#;
        test_extraction(input, vec![])
    }

    #[test]
    fn test_invalid_syntax() -> Result<(), Box<dyn Error>> {
        let input = r#"import os
from sys import
def broken_function(
"#;
        // Should return empty Vec for invalid Python syntax
        test_extraction(input, vec![])
    }
}