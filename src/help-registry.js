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

COMPARISON:
  < > <= >= == !=   Returns 1 (true) or 0 (false)
  && ||             Logical AND, OR

PIECEWISE (Case Statement):
  {{cond ? val, cond2 ? val2, default}}
  Example: {{x>0 ? 1, x<0 ? -1, 0}}

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
  < > <= >= == !=         Comparison (returns 1 or 0)
  &&                      Logical AND
  ||                      Logical OR

PIECEWISE / CASE STATEMENT:
  {{cond ? val, ...}}     Evaluate conditions in order
  {{x>0 ? 1, 0}}          Returns 1 if x>0, else 0
  {{x>0 ? 1, x<0 ? -1, 0}} Sign function
  
  - Conditions are evaluated left to right
  - First true condition returns its value
  - Last value without ? is the default
  - Works in function definitions: Abs = x -> {{x>=0 ? x, -x}}
  - Works as standalone expression: {{a>b ? a, b}}

NAMES:
  Variables (values):
    x, myVar, aVar        Start with lowercase letter
    aVar = avar           Case-insensitive after first char
    _precision            Underscore prefix = environment var
    mylist = [1,2,3]      Lowercase = raw sequence (no accessor)
    obj = {a=5}           Lowercase = raw object variable

  Functions/Lists (callable):
    Sq, MyFunc, Avar      Start with Uppercase letter
    Sq(x) -> x^2          Function definition
    List = [1, 2, 3]      Uppercase = List accessor: List(1) → 1

  Property Access:
    obj.prop              Access property (case-insensitive)
    obj.Prop = obj.prop   Same property, different display
    obj.name = 5          Last assignment sets display case
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

OBJECT VARIABLES (lowercase name):
  c = {a=4, b=7}          - Create object: displays {Object}
  c.a                     - Access property: returns 4
  c.d = 8                 - Add new property
  c.e = {n=7}             - Nested objects supported
  c.e.n                   - Chain access: returns 7

PROPERTY ASSIGNMENT:
  P.type = "poly"         - Set property on variable/function
  x.order = 3             - Numeric properties
  P.Der = x->2*x          - Store function references

CASE-INSENSITIVE ACCESS:
  obj.aB = 8              - Set property
  obj.ab                  - Returns 8 (same property)
  obj.AB                  - Returns 8 (same property)
  obj.ab = 7              - Overwrites; display now shows "ab"

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
  c = {a=4, b=7}
  Info(c)                 → {Object}
                              a = 4
                              b = 7

  P(x) -> x^2
  P.type = "poly"
  P.degree = 2
  Info(P)                 → P(x) -> x^2
                              type = poly
                              degree = 2
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

    // ArithFuns package help topics
    polynomial: `Polynomials (requires: LOAD arith-funs)

Create and manipulate polynomials with exact rational coefficients.

CONSTRUCTORS:
  Poly(coeffs)              - Create polynomial from coefficients (ascending)

EVALUATION:
  PolyEval(P, x)            - Evaluate P(x) directly
  PolyHorner(P, x)          - Evaluate using Horner's method

ARITHMETIC:
  PolyAdd(P, Q)             - Add polynomials
  PolySub(P, Q)             - Subtract: P - Q
  PolyMul(P, Q)             - Multiply polynomials
  PolyScale(P, c)           - Multiply by scalar: c·P
  PolyDer(P)                - First derivative
  PolyInt(P)                - Indefinite integral

DIVISION:
  SynthDiv(P, c)            - Synthetic division by (x - c)
  PolyRebase(P, a)          - Taylor expansion at x = a

EXAMPLES:
  P := Poly({1, 2, 3})      # 1 + 2x + 3x²
  PolyEval(P, 2)            → 17
  PolyDer(P)                → Poly({2, 6})  # 2 + 6x
`,

    "synth-div": `Synthetic Division (requires: LOAD arith-funs)

Efficient polynomial division by linear factors (x - c).

FUNCTIONS:
  SynthDiv(P, c)            - Divide P by (x - c), return {quotient, remainder}
  SynthDivPoly(P, c)        - Return just the quotient polynomial
  SynthDivRem(P, c)         - Return just the remainder (= P(c))

TAYLOR POLYNOMIAL (REBASING):
  PolyRebase(P, a)          - Express P(x) as polynomial in (x - a)

EXAMPLES:
  P := Poly({-6, 11, -6, 1})     # x³ - 6x² + 11x - 6
  SynthDiv(P, 1)                 → {quotient: Poly(...), remainder: 0}
  SynthDivRem(P, 2)              → 0 (x=2 is a root)
`,

    "number-theory": `Number Theory (requires: LOAD arith-funs)

Integer and number-theoretic functions.

DIVISIBILITY:
  Gcd(a, b, ...)            - Greatest common divisor
  Lcm(a, b, ...)            - Least common multiple
  ExtGcd(a, b)              - Extended Euclidean: {gcd, x, y}
  Mod(a, m)                 - a mod m (always ≥ 0)

PRIMES:
  IsPrime(n)                - Primality test (returns 0 or 1)
  NextPrime(n)              - Smallest prime > n
  Factor(n)                 - Prime factorization
  Divisors(n)               - All divisors of n

MODULAR:
  ModPow(base, exp, m)      - base^exp mod m (efficient)
  ModInv(a, m)              - Modular inverse (a⁻¹ mod m)
  EulerPhi(n)               - Euler's totient φ(n)

COMBINATORICS:
  Factorial(n)              - n!
  Binomial(n, k)            - C(n,k) = n choose k
  Fibonacci(n)              - nth Fibonacci number

EXAMPLES:
  Gcd(48, 18)               → 6
  Factor(60)                → {2, 2, 3, 5}
  Binomial(10, 3)           → 120
`,

    piecewise: `Piecewise Functions (requires: LOAD arith-funs)

Define and evaluate piecewise and step functions.

STEP FUNCTIONS:
  Step(x)                   - Heaviside: 0 if x < 0, 1 if x ≥ 0
  UnitStep(x, a)            - Step at a: 0 if x < a, 1 if x ≥ a
  Rect(x, a, b)             - Rectangle: 1 if a ≤ x ≤ b
  Ramp(x)                   - Ramp: max(0, x)

UTILITY:
  Clamp(x, lo, hi)          - Clamp x to [lo, hi]
  Sgn(x)                    - Sign function: -1, 0, or 1

INDICATORS:
  Chi(x, a, b)              - χ[a,b]: 1 if a ≤ x ≤ b
  ChiOpen(x, a, b)          - χ(a,b): 1 if a < x < b

EXAMPLES:
  Step(-2)                  → 0
  Step(3)                   → 1
  Rect(1.5, 1, 2)           → 1
  Clamp(5, 0, 3)            → 3

INLINE PIECEWISE SYNTAX:
  {{cond ? val, cond2 ? val2, default}}
  Abs = x -> {{x>=0 ? x, -x}}
  Sgn2 = x -> {{x>0 ? 1, x<0 ? -1, 0}}
`,

    oracles: `Oracles - Computable Real Numbers (requires: LOAD oracles)

Oracles represent real numbers that can be computed to arbitrary precision.
They are functions that answer "yes/no" questions about intervals.

CREATING ORACLES:
  c = Oracle(...)         - Create from a computation
  Functions may return oracles for transcendental results

ARITHMETIC:
  Oracles support arithmetic operations:
  c + c                   - Addition
  2 * c                   - Scalar multiplication
  c * 2                   - Multiplication by number
  c - 1                   - Subtraction
  -c                      - Negation

EVALUATION:
  Estimate(c, n)          - Compute c to n decimal places
  Compare(a, b)           - Compare two oracles

DISPLAY:
  c                       → [Oracle] yes: <interval>
  The "yes" interval shows the current known bounds.

PROPERTIES:
  Oracle functions have a .yes property containing
  a rational interval that the value is known to lie in.

EXAMPLES:
  LOAD oracles
  c = Sqrt(2)             # Create oracle for √2
  c + c                   # Oracle for 2√2
  Estimate(c, 10)         # Compute to 10 decimal places
`,

    variables: `Variable and Function Naming - Capitalization Rules

RatMath uses the first letter's case to determine behavior:

LOWERCASE (values/data):
  x = 5                   - Store value
  mylist = [1, 2, 3]      - Store raw sequence (not callable)
  obj = {a=5, b=7}        - Store raw object
  obj.prop                - Access: returns 5

UPPERCASE (callable/functions):
  Sq(x) -> x^2            - Define function
  List = [1, 2, 3]        - Create list accessor
  List(1)                 - Returns 1 (1-indexed access)
  Obj = {a=5}             - Create object function

CASE SENSITIVITY:
  Variable names:         Case-insensitive after first letter
    myVar = myvar = MYVAR (all same if first letter matches case)
  Property names:         Fully case-insensitive
    obj.Prop = obj.prop = obj.PROP (all same property)

DISPLAY CASE:
  The last assignment determines how names are displayed:
    obj.aB = 5            # Info shows: aB = 5
    obj.ab = 7            # Info now shows: ab = 7

EXAMPLES:
  L = [10, 20, 30]        # List accessor
  L(2)                    → 20
  
  data = [10, 20, 30]     # Raw sequence
  data                    → [10, 20, 30]
  LEN(data)               → 3
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
  HELP variables    - Naming rules and capitalization

SYNTAX:
  HELP syntax       - Quick syntax reference
  HELP syntax-full  - Complete syntax documentation

REALS (after LOAD reals):
  HELP trig         - Trigonometric functions
  HELP exp          - Exponential and logarithm

ORACLES (after LOAD oracles):
  HELP oracles      - Computable real numbers

ARITH-FUNS (after LOAD arith-funs):
  HELP polynomial     - Polynomial operations
  HELP synth-div      - Synthetic division details
  HELP number-theory  - Number-theoretic functions
  HELP piecewise      - Piecewise and step functions

PACKAGES:
  HELP packages     - List available packages to load
  HELP <package>    - Show package details

Type HELP <topic> for details.
`;
}
