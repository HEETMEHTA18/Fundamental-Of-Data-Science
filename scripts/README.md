# Python Utility Scripts

A collection of Python scripts demonstrating various programming concepts and utilities.

## Scripts Overview

### 📊 Calculators
- **calculator.py** - Basic arithmetic calculator
  - Performs: addition, subtraction, multiplication, division, modulus, exponents
  - Input: Two floating-point numbers
  - Output: Results of all operations
  - Usage: `python calulator.py`

- **calculatorWithGui.py** - Tkinter-based GUI Calculator
  - Full graphical interface with button grid
  - Error handling for invalid expressions
  - Clear button functionality
  - Usage: `python calculatorWithGui.py`

### ⏰ Utilities
- **clock.py** - Real-time digital clock
  - Displays current time updating in real-time
  - Press Ctrl+C to stop
  - Usage: `python clock.py`

- **hello.py** - Simple hello world program
  - Basic Python print demonstration
  - Usage: `python hello.py`

- **turnoff.py** - Computer shutdown scheduler
  - Schedule system shutdown after specified delay
  - Includes test mode for safe testing
  - Requires appropriate system permissions
  - Usage: `python turnoff.py`

### 🎮 Games
- **randomnum.py** - Number guessing game
  - Computer picks random number 1-100
  - Player guesses with hints (too high/too low)
  - Counts attempts until correct
  - Usage: `python randomnum.py`

### 📝 Utilities
- **module.py** - Random joke generator
  - Fetches random jokes using pyjokes library
  - Displays two jokes per run
  - Requires: `pip install pyjokes`
  - Usage: `python module.py`

- **demo1.py** - Test case generator
  - Generates Excel spreadsheet with test cases
  - Creates two sheets: 1D and 2D test cases
  - Calculates max/min counts and results
  - Creates file: `Hackathon_TestCases.xlsx`
  - Requires: `pip install openpyxl`
  - Usage: `python demo1.py`

- **test.py** - PDF document verification
  - Verifies presence of names and keywords in PDF documents
  - Uses fuzzy matching for name detection
  - Supports similarity threshold configuration
  - Requires: `pip install PyPDF2`
  - Usage: See docstrings for function details

## Installation

### Prerequisites
- Python 3.6 or higher
- pip (Python package manager)

### Dependencies
```bash
pip install openpyxl        # For Excel file generation
pip install PyPDF2          # For PDF text extraction
pip install pyjokes         # For joke generation
pip install python-docx     # For Word document manipulation (root scripts)
```

### Optional GUI Requirements
- **Windows:** Tkinter is included with Python
- **Linux:** Install with `sudo apt-get install python3-tk`
- **macOS:** Typically included, or install via Homebrew

## Code Quality

All scripts follow **PEP 8 Python Style Guide**:
- ✅ Module and function docstrings included
- ✅ Proper indentation (4 spaces)
- ✅ Line length < 79 characters
- ✅ Snake_case variable naming
- ✅ Proper error handling and exceptions
- ✅ Clear comments for complex logic

## Examples

### Hello World
```bash
$ python hello.py
hello world
hello world 2
```

### Calculator
```bash
$ python calulator.py
Welcome to the calculator program
Enter first number: 10
Enter second number: 3
The sum of a and b is:  13.0
The difference of a and b is:  7.0
The product of a and b is:  30.0
The quotient of a and b is:  3.3333333333333335
The modulus of a and b is:  1.0
The exponent of a to the power of b is:  1000.0
```

### Digital Clock
```bash
$ python clock.py
 Digital Clock (Press Ctrl+C to stop)
14:23:45
```

### Number Guessing Game
```bash
$ python randomnum.py
🎯 Welcome to the Number Guessing Game!
I'm thinking of a number between 1 and 100.
Enter your guess: 50
Too high! 📈
Enter your guess: 25
Too low! 📉
Enter your guess: 37
🎉 Correct! You guessed it in 3 attempts.
```

## Contributing

When adding new scripts:
1. Follow PEP 8 style guidelines
2. Include comprehensive docstrings
3. Add error handling
4. Test with Python 3.6+
5. Update this README with script description and usage

## License

These scripts are provided as educational examples.

## Author

Created as part of Python learning exercises and demonstrations.
