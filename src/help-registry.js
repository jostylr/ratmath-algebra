/**
 * Help Registry for ratmath
 * 
 * Manages help topics loaded from help/*.txt files in packages.
 * Supports hierarchical naming: "topic" or "package-topic"
 */

// Static help topics - inline content for topics that don't need files
const StaticHelp = {
    packages: null, // Generated dynamically from PackageRegistry
    
    // Built-in stdlib topics
    core: `Core Functions - Standard Library

Basic control flow and variable management functions.

VARIABLE MANAGEMENT:
  GETVAR(name, default)   - Get variable value, or default if not set
  ASSIGN(name, value)     - Assign value to a variable
  GLOBAL(name, value)     - Create/set a global variable

CONTROL FLOW:
  IF(cond, then, else)    - Lazy conditional: evaluates only the chosen branch
  MULTI(expr1, expr2, ..) - Evaluate multiple expressions, return last result

EXAMPLES:
  ASSIGN("x", 5)          → Sets x = 5
  GETVAR("x", 0)          → Returns 5 (or 0 if x not set)
  IF(1, 10, 1/0)          → Returns 10 (doesn't evaluate 1/0)
`,

    logic: `Logic Functions - Standard Library

Comparison and boolean operations. Returns 1 for true, 0 for false.

COMPARISON:
  EQ(a, b)      - Equal: returns 1 if a == b
  NE(a, b)      - Not equal: returns 1 if a != b
  GT(a, b)      - Greater than: returns 1 if a > b
  LT(a, b)      - Less than: returns 1 if a < b
  GE(a, b)      - Greater or equal: returns 1 if a >= b
  LE(a, b)      - Less or equal: returns 1 if a <= b

BOOLEAN:
  AND(a, b)     - Logical AND
  OR(a, b)      - Logical OR
  NOT(a)        - Logical NOT
`,

    list: `List Functions - Standard Library

Functions for working with sequences and lists.

BASIC:
  LEN(list)               - Length of a list
  GET(list, index)        - Get element at index

AGGREGATION:
  SUM[i](expr, start, end)      - Sum expr for i from start to end
  PROD[i](expr, start, end)     - Product expr for i from start to end
  SEQ[i](expr, start, end, step) - Generate sequence

HIGHER-ORDER:
  MAP(list, "Func")       - Apply Func to each element
  FILTER(list, "Pred")    - Keep elements where Pred returns non-zero
  REDUCE(list, "Func", init) - Reduce list using binary Func
`,

    syntax: `Parsing Syntax - RatMath Expression Language

NUMBERS:
  123           Integer
  3/4           Fraction
  1.25          Decimal
  1..2/3        Mixed number (1 and 2/3)
  0.#3          Repeating decimal (1/3)
  2:5           Interval

NOTATION:
  1E3           Scientific (1000)
  0b101         Binary (5)
  0xFF          Hex (255)
  3.~7          Continued fraction (22/7)

OPERATORS:
  + - * /       Arithmetic
  ^             Exponentiation
  !             Factorial
  ( )           Grouping

Type HELP syntax-full for complete syntax reference.
`,

    "syntax-full": `Complete Parsing Syntax - RatMath

NUMBERS:
  123                     Integer
  3/4                     Fraction (rational)
  1.25                    Decimal (converted to rational)
  1..2/3                  Mixed number (1 and 2/3 = 5/3)
  0.#3                    Repeating decimal (0.333... = 1/3)

INTERVALS:
  2:5                     Interval from 2 to 5
  1.23[+-0.01]            Uncertainty notation (1.22:1.24)
  1.2[3,6]                Decimal concatenation (1.23:1.26)

SCIENTIFIC NOTATION:
  1E3                     1000
  2.5E-2                  0.025
  1_^3                    Alternative (for non-decimal bases)

CONTINUED FRACTIONS:
  3.~7                    [3; 7] = 22/7
  3.~7~15~1               [3; 7, 15, 1] = 355/113

BASE PREFIXES:
  0b101                   Binary (= 5)
  0o17                    Octal (= 15)
  0xFF                    Hexadecimal (= 255)
  0d123                   Explicit decimal

OPERATORS (precedence high to low):
  !                       Factorial (postfix)
  ^                       Exponentiation
  **                      Multiplicative power
  * /                     Multiplication, division
  + -                     Addition, subtraction

NAMES:
  Variables (values):
    x, myVar, aVar        Start with lowercase letter
    aVar = avar           Case-insensitive after first char
    _precision            Underscore prefix = environment var

  Functions/Lists (callable):
    Sq, MyFunc, Avar      Start with Uppercase letter
    Sq(x) -> x^2          Function definition
    List = [1, 2, 3]      List/accessor definition
`,

    string: `String Functions - Standard Library

Functions for string manipulation.

BASIC:
  STRLEN(str)             - Length of string
  CHARAT(str, idx)        - Character at index
  SUBSTR(str, start, len) - Extract substring
  CONCAT(s1, s2, ...)     - Concatenate strings

SEARCH:
  INDEXOF(str, search)    - Find index (-1 if not found)
  CONTAINS(str, search)   - Returns 1 if contains
  STARTSWITH(str, prefix) - Returns 1 if starts with
  ENDSWITH(str, suffix)   - Returns 1 if ends with

TRANSFORM:
  UPPER(str), LOWER(str)  - Case conversion
  TRIM(str)               - Remove whitespace
  REPLACE(str, old, new)  - Replace all occurrences
  REVERSE(str), REPEAT(str, n)

SPLIT/JOIN:
  SPLIT(str, delim)       - Split into list
  JOIN(list, delim)       - Join list into string
`,

    objects: `Object Properties - Property Decoration System

Attach metadata properties to variables and functions using dot notation.

PROPERTY ASSIGNMENT:
  P.type = "poly"         - Set property on variable/function
  x.order = 3             - Numeric properties
  P.Der = x->2*x          - Store function references

OBJECT LITERALS:
  P = {a=5, b=10}         - Create object with properties
  P = {}                  - Empty object
  P = {_eval=x->x^2}      - Object with evaluation function
  Set("P", {a=5, b=c})    - Set multiple properties at once

INTERNAL PROPERTIES (underscore prefix):
  _eval       - Function called when object is evaluated: P(x)
  _display    - Custom display when typing P (e.g., "5x^3 + 3x^2")
  _definition - Store original definition for restoration

PROPERTY ACCESS:
  P.type                  - Read property in expressions
  x.order + 2             - Use property values in calculations
  P.Der(5)                - Call function stored as property

FUNCTIONS:
  Get(target, prop, default?) - Get property value
  Set(target, p1, v1, ...)    - Set unlimited properties
  Has(target, prop)           - Check if property exists (returns 1/0)
  Del(target, prop)           - Delete property (returns 1/0)
  Type(target, check?)        - Get/check "type" property
  Props(target)               - Get list of all property names
  Info(P)                     - Show definition and all properties
  Info(P, "filter")           - Filter properties by substring
  CopyProps(src, dest)        - Copy all properties
  ClearProps(target)          - Clear all properties

EXAMPLES:
  P(x) -> x^2
  P.type = "poly"
  P.degree = 2
  Info(P)                 → P(x) -> x^2
                              type = poly
                              degree = 2
  
  Q = {a=5, b=10, _display="custom"}
  Info(Q)                 → custom
                              a = 5
                              b = 10
`,

    trig: `Trigonometric Functions (requires: LOAD reals)

All angles are in RADIANS.

FUNCTIONS:
  Sin(x)        - Sine
  Cos(x)        - Cosine
  Tan(x)        - Tangent
  Arcsin(x)     - Inverse sine (|x| ≤ 1)
  Arccos(x)     - Inverse cosine (|x| ≤ 1)
  Arctan(x)     - Inverse tangent

COMMON VALUES:
  Sin(PI()/6) = 1/2     (30°)
  Sin(PI()/4) = √2/2    (45°)
  Cos(PI()/3) = 1/2     (60°)

Convert degrees: radians = degrees * PI() / 180
`,

    exp: `Exponential & Logarithm Functions (requires: LOAD reals)

FUNCTIONS:
  Exp(x)              - e^x
  Ln(x)               - Natural log (x > 0)
  Log(x, base)        - Logarithm base b
  E()                 - Euler's number e ≈ 2.71828

EXAMPLES:
  Exp(1)              → e
  Ln(E())             → 1
  Log(100, 10)        → 2
  Exp(Ln(5))          → 5
`,
};

/**
 * Get list of available help topics
 */
export function getHelpTopics() {
    return Object.keys(StaticHelp).filter(k => k !== 'packages');
}

/**
 * Get help text for a topic
 * @param {string} topic - Topic name (case-insensitive)
 * @returns {string|null} - Help text or null if not found
 */
export function getHelpText(topic) {
    const lower = topic.toLowerCase();
    
    // Special case: "topics" returns the full topics listing
    if (lower === "topics") {
        return getHelpTopicsText();
    }
    
    // Check static help
    if (StaticHelp[lower]) {
        return StaticHelp[lower];
    }
    
    // Check with package prefix removed (e.g., "reals-trig" -> "trig")
    const dashIndex = lower.indexOf('-');
    if (dashIndex > 0) {
        const subtopic = lower.substring(dashIndex + 1);
        if (StaticHelp[subtopic]) {
            return StaticHelp[subtopic];
        }
    }
    
    return null;
}

/**
 * Check if a help topic exists
 */
export function hasHelpTopic(topic) {
    return getHelpText(topic) !== null;
}

/**
 * Get the essential help intro (default HELP with no args)
 */
export function getHelpIntroText() {
    return `RatMath Calculator - Rational Interval Arithmetic

BASICS:
  3/4 + 1/2         Fractions
  2:5               Interval from 2 to 5
  x = 3/4           Assign variable (lowercase start)
  Sq(x) -> x^2      Define function (Uppercase start)

COMMANDS:
  HELP topics       All help topics
  HELP syntax       Number formats & operators
  HELP packages     Available packages
  LOAD reals        Load transcendental functions

Type HELP <topic> for details.
`;
}

/**
 * Get the full help topics listing
 */
export function getHelpTopicsText() {
    return `Available Help Topics:

STDLIB (always available):
  HELP core         - Variable management, control flow
  HELP logic        - Comparison and boolean operations
  HELP list         - Lists, sequences, higher-order functions
  HELP string       - String manipulation functions
  HELP objects      - Property decoration system

SYNTAX:
  HELP syntax       - Quick syntax reference
  HELP syntax-full  - Complete syntax documentation

REALS (after LOAD reals):
  HELP trig         - Trigonometric functions
  HELP exp          - Exponential and logarithm

PACKAGES:
  HELP packages     - List available packages to load
  HELP <package>    - Show package details

Type HELP <topic> for details.
`;
}
